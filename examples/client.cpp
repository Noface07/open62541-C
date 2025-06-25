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
#include "MQTThandler.h"
#include "fetchAPI.cpp"
#include "structs.h"



#include <unordered_map>
#include <boost/asio.hpp>

using namespace std;

// Global MQTT handler instance
MQTTHandler* g_mqttHandler = nullptr;

unordered_map<string, int> groupIdMap;

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
    std::map<std::string, UA_CreateSubscriptionResponse> subscriptions;
    std::atomic<bool> running{true};
    bool isConnected = false;
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
    else{
        std::cout << "Connected to MQTT broker" << std::endl;
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
        for(const auto &group : server.groups) {
        for(const auto &tag : group.tags) {
            if(tag.name) {
                cout << *tag.name << " ";
            }
            cout<<endl;
            

            if (tag.rdWtOpt == "RD_WRT_RO") {
                cout << "ReadOnly" << endl;
            }
            else if (tag.rdWtOpt == "RD_WRT_RW") {
            if(tag.mappedInfospaceTags) {
                for(const auto &infoSpace : *tag.mappedInfospaceTags) {
                    cout << infoSpace.namespaces << " ";
                    
                    if(g_mqttHandler->isConnected()){
                        g_mqttHandler->subscribe(infoSpace.namespaces);
                        std::cout << "Subscribing to topic: " << infoSpace.namespaces << std::endl;
                    }
                    else{
                        std::cout << "MQTT handler is not connected" << std::endl;
                    }

                    cout << infoSpace.namespaces << endl;

                    // cout << infoSpace.namespaces << " subscribed; ";
                    cout << endl;
                    // Add a small delay between subscriptions
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
            }
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

        if(server.msgSecurityMode != "NONE") {
        
        UA_ClientConfig *config = UA_Client_getConfig(context->client.get());
        UA_ClientConfig_setDefaultEncryption(config,
            client_cert,
            client_key,
            trustList,  // trustList array
            1,  // trustListSize (number of certificates in trust list)
            revocationList,  // RevocationList
            1);  // RevocationListSize

            UA_String_clear(&config->applicationUri);
            UA_String_clear(&config->clientDescription.applicationUri);
            UA_LocalizedText_clear(&config->clientDescription.applicationName);
            UA_String_clear(&config->clientDescription.productUri);
            
            config->applicationUri = UA_STRING_ALLOC("urn:Anexee.server.application");
            config->clientDescription.applicationUri = UA_STRING_ALLOC("urn:Anexee.server.application");
            config->clientDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Anexee");
            config->clientDescription.productUri = UA_STRING_ALLOC("urn:Anexee");

            if (server.msgSecurityMode == "NONE") {
                config->securityMode = UA_MESSAGESECURITYMODE_NONE;
            } else if (server.msgSecurityMode == "OPC_UA_SM_SG") {
                config->securityMode = UA_MESSAGESECURITYMODE_SIGN;
            } else if (server.msgSecurityMode == "OPC_UA_SM_SG_ENC") {
                config->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
            } else {
                config->securityMode = UA_MESSAGESECURITYMODE_INVALID;
            }
            // std::string base = "http://opcfoundation.org/UA/SecurityPolicy#";
            // std::string full = base + server.securityPolicy;
            // config->securityPolicyUri = UA_STRING_STATIC(full.c_str());

            // config->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
            config->securityPolicyUri = UA_STRING_ALLOC("http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep");
            
            UA_StatusCode retval;
            
            if(server.authType == "anonymous") {
                retval = UA_Client_connect(context->client.get(), server.endpointUrl.c_str());
            }
            else if (server.authType == "user") {
                retval = UA_Client_connectUsername(context->client.get(), server.endpointUrl.c_str(), "user1", "password1");
            } 
            else {
                std::cerr << "Invalid authentication type: " << server.authType << std::endl;
                continue;
            }

            if(retval != UA_STATUSCODE_GOOD) {
                std::cerr << "Could not connect to server: " << server.endpointUrl << std::endl;
                UA_Client_disconnect(context->client.get());
                continue;
                }
            else{
                context->isConnected = true;
            }
        }
        else{
                UA_ClientConfig_setDefault(UA_Client_getConfig(context->client.get()));
                UA_StatusCode retval = UA_Client_connect(context->client.get(), server.endpointUrl.c_str());
                if(retval != UA_STATUSCODE_GOOD) {
                    std::cerr << "Could not connect to server: " << server.endpointUrl << std::endl;
                    continue;
                }
            else{
                context->isConnected = true;
            }
        }

        cout << "HEREEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE";
        cout << endl;
        cout << endl;
        cout << endl;
        cout << endl;
        cout << endl;
#ifdef UA_ENABLE_SUBSCRIPTIONS

        for(const auto &group : server.groups) {
        UA_CreateSubscriptionRequest request = UA_CreateSubscriptionRequest_default();
        //add Group properties here!!!
        request.requestedMaxKeepAliveCount = group.maxKeepAliveCount;
        request.requestedPublishingInterval = group.publishingInterval;
        // request.publishingEnabled = true;
        request.requestedLifetimeCount = group.lifetimeCount;
        request.priority = group.priority;
        request.maxNotificationsPerPublish = group.maxNotificationsPerPublish;

        UA_CreateSubscriptionResponse sub = UA_Client_Subscriptions_create(
            context->client.get(), request, nullptr, nullptr, nullptr);

        context->subscriptions[group.name] = sub;
        cout<<"Subscription Created:" << group.name << endl;

        if(context->subscriptions[group.name].responseHeader.serviceResult == UA_STATUSCODE_GOOD) {
            std::cout << "Subscription created successfully for server: "
                      << server.endpointUrl << std::endl;
        }

        }
#endif

            if(context->isConnected) {
                context->startLoop();  
                clientPool[context->endpoint] = context.get();
                clientContexts.push_back(std::move(context));
            }

        for(const auto &group : server.groups) {
            string groupName = group.name;
            for(const auto &tag : group.tags) {
            for(const auto &infoSpace : *tag.mappedInfospaceTags) {

                if (!clientPool.count(Mapping[infoSpace.tagId].second)) continue;

                auto context = clientPool.at(Mapping[infoSpace.tagId].second);
                if (!context->isConnected) {
                    std::cerr << "Skipping tagId " << infoSpace.tagId << " due to disconnected server\n";
                    continue;
                }

                std::lock_guard<std::mutex> lock(context->taskMutex);
                context->taskQueue.push([context, infoSpace, groupName]() {
                MyMonitorContext *myContext = new MyMonitorContext{infoSpace, g_mqttHandler};
                MonitorItem(context->client.get(), context->subscriptions[groupName],
                            Mapping[infoSpace.tagId].first.c_str(), infoSpace.tagId,myContext);
                });




            }
        }
        }


    }


    std::cout << "Client pool initialized. Press Ctrl+C to stop..." << std::endl;

    
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
        if (!context->isConnected) {
            return;
        }
        std::cout << "Ready to use client:  (connected to " << context->endpoint << ")" << std::endl;

        // if (context->subscriptions[groupName].responseHeader.serviceResult != UA_STATUSCODE_GOOD ||
        //     context->subscriptions[groupName].subscriptionId == 0) {
        //     std::cerr << "Failed to create subscription" << std::endl;
        //     return;
        // }

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


    while(g_running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
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
}
