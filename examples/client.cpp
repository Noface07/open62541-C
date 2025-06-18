/* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
 * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

/* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
 * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <functional>

#include "Monitoring.cpp"
#include "MQTThandler.cpp"
#include "fetchAPI.cpp"
#include "structs.h"



#include <unordered_map>
#include <boost/asio.hpp>

using namespace std;

// Global MQTT handler instance
MQTTHandler* g_mqttHandler = nullptr;

std::atomic<bool> g_running(true);

void stopHandler(int signum) {
    std::cout << "\nReceived signal " << signum << ", shutting down..." << std::endl;
    g_running = false;
}



struct UA_Client_Deleter {
    void operator()(UA_Client *client) const {
        if(client)
            UA_Client_delete(client);
    }
};


struct ClientContext {
    std::string name;
    std::string endpoint;
    std::unique_ptr<UA_Client, UA_Client_Deleter> client;
    UA_CreateSubscriptionResponse subscription;
    std::atomic<bool> running{true};
    std::thread thread;
    std::mutex taskMutex;
    std::queue<std::function<void()>> taskQueue;

    void startLoop() {
        thread = std::thread([this]() {
            std::cout << "Thread started for " << name << "\n";
            while (running) {
                {
                    std::lock_guard<std::mutex> lock(taskMutex);
                    while (!taskQueue.empty()) {
                        auto task = std::move(taskQueue.front());
                        taskQueue.pop(); 
                        task();
                    }
                }
                UA_StatusCode code = UA_Client_run_iterate(client.get(), 100);
                if (code != UA_STATUSCODE_GOOD) {
                    std::cerr << name << ": UA_Client_run_iterate failed with "
                              << UA_StatusCode_name(code) << "\n";
                    break;
                }
            }
            std::cout << "Thread exiting for " << name << "\n";
        });
    }

    void stopLoop() {
        running = false;
        if(thread.joinable()) {
            thread.join();
        }
    }
};

static UA_ByteString loadFile(const char *path) {
    UA_ByteString fileContents = UA_BYTESTRING_NULL;
    FILE *fp = fopen(path, "rb");
    if(!fp)
        return fileContents;
    fseek(fp, 0, SEEK_END);
    fileContents.length = (size_t)ftell(fp);
    fileContents.data = (UA_Byte *)UA_malloc(fileContents.length * sizeof(UA_Byte));
    fseek(fp, 0, SEEK_SET);
    if(fread(fileContents.data, sizeof(UA_Byte), fileContents.length, fp) != fileContents.length) {
        UA_ByteString_clear(&fileContents);
    }
    fclose(fp);
    return fileContents;
}







int main() {
    // Initialize global MQTT handler
    boost::asio::io_context ioc;
    g_mqttHandler = new MQTTHandler(ioc);
    
    // Connect to MQTT broker
    if (!g_mqttHandler->connect("216.48.184.131", "15579", "portal", "dt0Unw7QRh")) {
        std::cerr << "Failed to connect to MQTT broker" << std::endl;
        return EXIT_FAILURE;
    }



#ifdef UA_ENABLE_SUBSCRIPTIONS
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);
#endif

    std::vector<std::unique_ptr<ClientContext>> clientContexts;
    std::unordered_map<std::string, ClientContext *> clientPool;

    auto futureToken = std::async(std::launch::async, getBearerToken);
    string BearerToken = "";
    json token = futureToken.get();
    // Extract access_token
    if (token.contains("access_token")) {
        BearerToken = token["access_token"].get<std::string>();
    }


    vector<ServerInfoO> serverList = ParseServerHierarchy(BearerToken);


    cout << "endpoints: ";
    for(const auto &server : serverList) {
        cout << server.name << " ";
        cout<<endl;
        
        // Print tags if they exist
        for(const auto &tag : server.tags) {
            if(tag.name) {
                cout << *tag.name << " ";
            }
            cout<<endl;
            
            // Print mapped infospace tags if they exist
            if(tag.mappedInfospaceTags) {
                for(const auto &infoSpace : *tag.mappedInfospaceTags) {
                    cout << infoSpace.namespaces << " ";
                    g_mqttHandler->subscribe(infoSpace.namespaces);

                    cout << infoSpace.namespaces << endl;

                    // cout << infoSpace.namespaces << " subscribed; ";
                    cout << endl;
                    // Add a small delay between subscriptions
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            }
        }
    }
    cout << endl;

    //g_mqttHandler->mqtt_subscribe_and_update("TDSPL/Test31-01/tag20-1");


    // std::vector<ServerInfo> servers = { {"Anexee", "opc.tcp://localhost:53531"},
    //                                     {"Prosys", "opc.tcp://localhost:53530"} };

    for(const auto &server : serverList) {
        auto context = std::make_unique<ClientContext>();
        context->name = server.name;
        context->endpoint = server.endpointUrl;
        context->client = std::unique_ptr<UA_Client, UA_Client_Deleter>(UA_Client_new());

        
        //ADD LOGIC FOR CERTIFICATES
            /* TODO */
        UA_ByteString client_cert = loadFile("client/own/certs/client_cert.der");
        UA_ByteString client_key = loadFile("client/own/certs/client_key.der");
        UA_ByteString server_cert = loadFile("server/own/certs/server_cert.der");
        UA_ByteString ca_cert = loadFile("ca/certs/ca.crt");
        UA_ByteString revocation_cert = loadFile("server/trusted/crl/crl.crl");

        // Create trust list array
        UA_STACKARRAY(UA_ByteString, trustList, 1);
        trustList[0] = ca_cert;

        UA_STACKARRAY(UA_ByteString, revocationList, 1);   
        revocationList[0] = revocation_cert;

        //FOR SECURITY POLICY and MESSAGE SECURITY MODE
        
        if(server.endpointUrl == "opc.tcp://Asce:53531") {
        
        UA_ClientConfig *config = UA_Client_getConfig(context->client.get());
        UA_ClientConfig_setDefaultEncryption(config,
            client_cert,
            client_key,
            trustList,  // trustList array
            1,  // trustListSize (number of certificates in trust list)
            revocationList,  // RevocationList
            1);  // RevocationListSize
            
            config->applicationUri = UA_STRING_STATIC("urn:Anexee.server.application");
            config->clientDescription.applicationUri = UA_STRING_STATIC("urn:Anexee.server.application");
            config->clientDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Anexee");
            config->clientDescription.productUri = UA_STRING_STATIC("urn:Anexee");

        // if (server.msgSecurityMode == "None") {
        //     config->securityMode = UA_MESSAGESECURITYMODE_NONE;
        // } else if (server.msgSecurityMode == "Sign") {
        //     config->securityMode = UA_MESSAGESECURITYMODE_SIGN;
        // } else if (server.msgSecurityMode == "SignAndEncrypt") {
        //     config->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
        // } else {
        //     config->securityMode = UA_MESSAGESECURITYMODE_INVALID;
        // }
        // std::string base = "http://opcfoundation.org/UA/SecurityPolicy#";
        // std::string full = base + server.securityPolicy;
        // config->securityPolicyUri = UA_STRING_STATIC(full.c_str());

        config->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
        config->securityPolicyUri = UA_STRING_STATIC("http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256");
                UA_StatusCode retval = UA_Client_connect(context->client.get(), server.endpointUrl.c_str());
        if(retval != UA_STATUSCODE_GOOD) {
            std::cerr << "Could not connect to server: " << server.endpointUrl << std::endl;
            continue;
        }
        }
        else{
                UA_ClientConfig_setDefault(UA_Client_getConfig(context->client.get()));
                UA_StatusCode retval = UA_Client_connect(context->client.get(), server.endpointUrl.c_str());
                if(retval != UA_STATUSCODE_GOOD) {
                    std::cerr << "Could not connect to server: " << server.endpointUrl << std::endl;
                    continue;
                }
        }
#ifdef UA_ENABLE_SUBSCRIPTIONS
        UA_CreateSubscriptionRequest request = UA_CreateSubscriptionRequest_default();
        request.requestedMaxKeepAliveCount = 60;

        context->subscription = UA_Client_Subscriptions_create(
            context->client.get(), request, nullptr, nullptr, nullptr);
        if(context->subscription.responseHeader.serviceResult == UA_STATUSCODE_GOOD) {
            std::cout << "Subscription created successfully for server: "
                      << server.endpointUrl << std::endl;
        }
#endif

        context->startLoop();
        clientPool[context->endpoint] = context.get();
        clientContexts.push_back(std::move(context));
    }

    std::cout << "Client pool initialized. Press Ctrl+C to stop..." << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(1));




        // Set up callback for MQTT messages
g_mqttHandler->setCallback(
    [&clientPool](const std::string &topic, const std::string &payload) {
        std::cout << "Received message on topic " << topic << ": " << payload << "\n\n";

        json json_payload = json::parse(payload);

        if (!json_payload.contains("Data") || !json_payload["Data"].is_array())
            return;

        auto data = json_payload["Data"][0];
        if (!data.contains("TagId") || !data["TagId"].is_number_integer())
            return;

        int tagId = data["TagId"].get<int>();
        std::cout << "TagId: " << tagId << std::endl;

        if (!data.contains("UpdateType") || !data["UpdateType"].is_string())
            return;

        int updateType = data["UpdateType"].get<int>();
        std::cout << "UpdateType: " << updateType << std::endl;

        std::string endpoint = Mapping[tagId].second;
        if (!clientPool.count(endpoint))
            return;

        auto context = clientPool.at(endpoint);
        std::cout << "Ready to use client:  (connected to " << context->endpoint << ")" << std::endl;

        if (context->subscription.responseHeader.serviceResult != UA_STATUSCODE_GOOD ||
            context->subscription.subscriptionId == 0) {
            std::cerr << "Failed to create subscription" << std::endl;
            return;
        }

        if (updateType == UpdateType::TELEMETERY) {
            // std::lock_guard<std::mutex> lock(context->taskMutex);
            // context->taskQueue.push([context, tagId]() {
            //     MonitorItem(context->client.get(), context->subscription,
            //                 Mapping[tagId].first.c_str(), tagId);
            // });
            cout<<"TELEMETERY"<<endl;
        } else if (updateType == UpdateType::COMMAND) {
            std::lock_guard<std::mutex> lock(context->taskMutex);
            context->taskQueue.push([context, tagId, data, topic, json_payload]() mutable {
                UA_Variant value;
                UA_Variant_init(&value);
                double val = data["Value"].get<double>();
                UA_Variant_setScalar(&value, &val, &UA_TYPES[UA_TYPES_DOUBLE]);

                auto nsAndValue = extractNsAndValue(Mapping[tagId].first);
                UA_StatusCode retval = UA_Client_writeValueAttribute(
                    context->client.get(),
                    UA_NODEID_STRING(nsAndValue.first, const_cast<char*>(nsAndValue.second.c_str())),
                    &value);

                if (retval != UA_STATUSCODE_GOOD) {
                    std::cerr << "Failed to write value: " << UA_StatusCode_name(retval) << std::endl;
                } else {
                    std::cout << "Value written successfully" << std::endl;
                    g_mqttHandler->publish(topic, json_payload.dump());
                }

                UA_Variant_clear(&value);
            });
        }
        else if (updateType == UpdateType::BULKDATA) {
            cout<<"BULKDATA"<<endl;
        }
        
    }
);







    // if(clientPool.count("opc.tcp://localhost:53531")) {
    //     auto context = clientPool["opc.tcp://localhost:53531"];
    //     std::cout << "Ready to use client: Anexee (connected to "
    //               << context->endpoint << ")" << std::endl;

    //     if (UA_STATUSCODE_GOOD == context->subscription.responseHeader.serviceResult &&
    //         context->subscription.subscriptionId != 0) {

    //         std::lock_guard<std::mutex> lock(context->taskMutex);
    //         context->taskQueue.push([context]() {
    //             MonitorItem(context->client.get(), context->subscription, "ns=1;i=194", 123);
    //         });
    //     } else {
    //         std::cerr << "Failed to create subscription" << std::endl;
    //     }
    // }










    //     if(clientPool.count("Prosys")) {
    //     auto context = clientPool["Prosys"];
    //     std::cout << "Ready to use client: Anexee (connected to "
    //               << context->endpoint << ")" << std::endl;

    //     if (UA_STATUSCODE_GOOD == context->subscription.responseHeader.serviceResult &&
    //         context->subscription.subscriptionId != 0) {

    //         std::lock_guard<std::mutex> lock(context->taskMutex);
    //         context->taskQueue.push([context]() {
    //             MonitorItem(context->client.get(), context->subscription, "ns=3;i=1002", "Prosys");
    //         });
    //     } else {
    //         std::cerr << "Failed to create subscription" << std::endl;
    //     }
    // }








    while(g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(200));
    }

    std::cout << "Cleaning up..." << std::endl;

    for(auto &context : clientContexts) {
        UA_Client_disconnect(context->client.get());
        context->stopLoop();
    }

    clientPool.clear();
    clientContexts.clear();

    // Clean up MQTT handler at the end
    delete g_mqttHandler;
    return EXIT_SUCCESS;



    //     UA_Client *client = UA_Client_new();
    //     UA_ClientConfig *config = UA_Client_getConfig(client);

    //     const char *certPath = "D:/OPC UA Server/OPCUA- open "
    //                           "62451/open62541-C/build/bin/examples/client/client.der";
    //     const char *certKey = "D:/OPC UA Server/OPCUA- open "
    //                            "62451/open62541-C/build/bin/examples/client/client_key.der";
    //     const char *servercertPath = "D:/OPC UA Server/OPCUA- open "
    //                            "62451/open62541-C/build/bin/examples/Certs/own/certs/server_cert.der";

    //     // Step 1: Set default config
    //     UA_ClientConfig_setDefault(config);

    //     // Step 2: Load certificates
    //     UA_ByteString certificate = loadFile(certPath);
    //     UA_ByteString privateKey = loadFile(certKey);
    //     UA_ByteString serverCert = loadFile(servercertPath);

    //         if (certificate.length == 0) {
    //             UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load
    //             client certificate"); return EXIT_FAILURE;
    //         }
    //         if (privateKey.length == 0) {
    //             UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load
    //             client private key"); return EXIT_FAILURE;
    //         }
    //         if (serverCert.length == 0) {
    //             UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load
    //             server certificate into trust list"); return EXIT_FAILURE;
    //         }

    //     UA_STACKARRAY(UA_ByteString, trustList, 1);
    //     trustList[0] = serverCert;

    //     // Step 3: Apply encryption
    //     UA_ClientConfig_setDefaultEncryption(config, certificate, privateKey, NULL, 0,
    //     NULL, 0);

    //     UA_CertificateGroup_AcceptAll(&config->certificateVerification);

    //     // Step 5: Other settings
    //     config->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
    //     config->securityPolicyUri =
    //     UA_STRING_STATIC("http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256");
    //     config->maxTrustListSize = 1;

    //     UA_String_clear(&config->applicationUri);
    //     config->applicationUri = UA_STRING_ALLOC("urn:Anexee.server.application");
    //     config->clientDescription.applicationUri =
    //     UA_STRING_ALLOC("urn:Anexee.server.application");
    //     config->clientDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en-US",
    //     "Anexee"); config->clientDescription.productUri =
    //     UA_STRING_ALLOC("urn:Anexee.server");

    //     /* Connect to a server */
    //     UA_StatusCode retval = UA_Client_connectUsername(client,
    //     "opc.tcp://Asce:53531", "user1", "password1");

    //     // retval = UA_Client_connectSecureChannel(client,
    //     "opc.tcp://localhost:53531");

    //         // Clean up certificate and private key
    //         UA_ByteString_clear(&certificate);
    //     UA_ByteString_clear(&privateKey);
    //     UA_ByteString_clear(&serverCert);

    //     if(retval != UA_STATUSCODE_GOOD) {
    //         cout << "Could not connect" << endl;
    //         UA_Client_delete(client);
    //         return EXIT_SUCCESS;
    //     }

    // #ifdef UA_ENABLE_SUBSCRIPTIONS
    //     /* Create a subscription */
    //     UA_CreateSubscriptionRequest request = UA_CreateSubscriptionRequest_default();
    //     request.requestedMaxKeepAliveCount = 60;
    //     UA_CreateSubscriptionResponse response = UA_Client_Subscriptions_create(client,
    //     request,
    //                                                                             NULL,
    //                                                                             NULL,
    //                                                                             NULL);

    //     UA_UInt32 subId = response.subscriptionId;
    //     if(response.responseHeader.serviceResult == UA_STATUSCODE_GOOD) {
    //         cout << "Create subscription succeeded, id " << subId << endl;
    //         cout << "Revised publishing interval: " <<
    //         response.revisedPublishingInterval << " ms" << endl; cout << "Revised
    //         lifetime count: " << response.revisedLifetimeCount << endl; cout <<
    //         "Revised max keep alive count: " << response.revisedMaxKeepAliveCount <<
    //         endl;
    //     }

    //     MonitorItem(client, response, "ns=1;i=194");
    //     UA_Client_run_iterate(client, 1000);
    // #endif

    //     /* Read attribute */
    //     UA_Int32 value = 0;
    //     cout << "\nReading the value of node (1, \"the.answer\"):" << endl;
    //     UA_Variant *val = UA_Variant_new();
    //     retval = UA_Client_readValueAttribute(
    //         client, UA_NODEID_STRING(1, const_cast<char *>("the.answer")), val);
    //     if(retval == UA_STATUSCODE_GOOD && UA_Variant_isScalar(val) &&
    //        val->type == &UA_TYPES[UA_TYPES_INT32]) {
    //             value = *(UA_Int32*)val->data;
    //             cout << "the value is: " << value << endl;
    //     }
    //     UA_Variant_delete(val);

    // #ifdef UA_ENABLE_SUBSCRIPTIONS

    //     cout << "Listening for events. Press Ctrl-C to exit." << endl;
    //     while(running) {
    //         UA_Client_run_iterate(client, 100);
    //     }

    // #endif

    //     cout << "Press Enter to continue...";
    //     getchar();

    //     UA_Client_disconnect(client);
    //     UA_Client_delete(client);
    //     return EXIT_SUCCESS;
}
