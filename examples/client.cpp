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
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>
#include <windows.h>

#include "Logger.h"
#include "MQTThandler.h"
#include "Monitoring.cpp"
#include "fetchAPI.cpp"
#include "structs.h"
#include <boost/asio.hpp>
#include <unordered_map>

using namespace std;

// Global MQTT handler instance
MQTTHandler *g_mqttHandler = nullptr;

// Global Debug Flag
bool g_debug = false;

unordered_map<string, int> groupIdMap;

std::atomic<bool> g_running(true);

// Windows Service globals
static SERVICE_STATUS g_ServiceStatus;
static SERVICE_STATUS_HANDLE g_StatusHandle = nullptr;
static HANDLE g_ServiceStopEvent = nullptr;
static const char *SERVICE_NAME = "AnexeeOPCUAClient";
static HANDLE g_EventLog = nullptr;

// Forward declarations for service
static void ReportSvcStatus(DWORD currentState, DWORD win32ExitCode, DWORD waitHint);
static void WINAPI ServiceMain(DWORD argc, LPTSTR *argv);
static void WINAPI ServiceCtrlHandler(DWORD controlCode);
static DWORD WINAPI ServiceWorkerThread(LPVOID lpParam);
static void LogEventWord(WORD type, const char *msg);
static void SetWorkingDirectoryToExe();

void
stopHandler(int signum) {
    log("Received signal " + std::to_string(signum) + ", shutting down...");
    g_running = false;
}

struct UA_Client_Deleter {
    void
    operator()(UA_Client *client) const {
        if(client)
            UA_Client_delete(client);
    }
};

struct ClientContext {
    std::string name;
    std::string endpoint;
    std::unique_ptr<UA_Client, UA_Client_Deleter> client;
    std::map<std::string, UA_CreateSubscriptionResponse> subscriptions;
    std::atomic<bool> running;
    bool isConnected;
    std::thread thread;
    std::mutex taskMutex;
    std::queue<std::function<void()>> taskQueue;

    // new:
    std::function<UA_StatusCode()> connectOnce;
    std::function<void()> onConnected;

    ClientContext() : running(true), isConnected(false) {}

    void
    startLoop() {
        thread = std::thread([this]() {
            log("Thread started for" + name, LogLevel::DEBUG);
            while(running) {
                if(!isConnected) {
                    if(connectOnce) {
                        UA_StatusCode rc = connectOnce();
                        if(rc == UA_STATUSCODE_GOOD) {
                            isConnected = true;
                            if(onConnected) onConnected();
                            log("Connected to " + endpoint);
                        } else {
                            std::this_thread::sleep_for(std::chrono::seconds(5));
                            continue;
                        }
                    } else {
                        std::this_thread::sleep_for(std::chrono::seconds(1));
                        continue;
                    }
                }

                {
                    std::lock_guard<std::mutex> lock(taskMutex);
                    while(!taskQueue.empty()) {
                        auto task = std::move(taskQueue.front());
                        taskQueue.pop();
                        task();
                    }
                }

                UA_StatusCode code = UA_Client_run_iterate(client.get(), 100);
                if(code != UA_STATUSCODE_GOOD) {
                    log(name + ": UA_Client_run_iterate failed with " +
                            UA_StatusCode_name(code), LogLevel::ERRORS);
                    UA_Client_disconnect(client.get());
                    isConnected = false;
                    // optional: clear subscriptions to avoid duplicates
                    subscriptions.clear();
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                }
            }
            log("Thread exiting for " + name);
        });
    }

    void stopLoop() {
        running = false;
        if(thread.joinable()) {
            thread.join();
        }
    }
};

static UA_ByteString
loadFile(const char *path) {
    UA_ByteString fileContents = UA_BYTESTRING_NULL;
    FILE *fp = fopen(path, "rb");
    if(!fp)
        return fileContents;
    fseek(fp, 0, SEEK_END);
    fileContents.length = (size_t)ftell(fp);
    fileContents.data = (UA_Byte *)UA_malloc(fileContents.length * sizeof(UA_Byte));
    fseek(fp, 0, SEEK_SET);
    if(fread(fileContents.data, sizeof(UA_Byte), fileContents.length, fp) !=
       fileContents.length) {
        UA_ByteString_clear(&fileContents);
    }
    fclose(fp);
    return fileContents;
}

// Your custom logger callback
static void
myLog(void *context, UA_LogLevel level, UA_LogCategory category, const char *msg,
      va_list args) {

    char buffer[1024];
    vsnprintf(buffer, sizeof(buffer), msg, args);

    switch(level) {
        case UA_LOGLEVEL_FATAL:
            log(buffer, LogLevel::ERRORS);
            break;
        case UA_LOGLEVEL_ERROR:
            log(buffer, LogLevel::ERRORS);
            break;
        case UA_LOGLEVEL_WARNING:
            log(buffer, LogLevel::INFO);
            break;
        case UA_LOGLEVEL_INFO:
            log(buffer, LogLevel::INFO);
            break;
        case UA_LOGLEVEL_DEBUG:
            log(buffer, LogLevel::DEBUG);
            break;
    }
}

// Custom logger plugin
static UA_Logger myLogger = {myLog, nullptr, nullptr};

// Extracted main logic so it can be reused by console and service
static int
runClient(bool isService, int argc, char *argv[]) {

    HANDLE hMutex = CreateMutexA(NULL, TRUE, "Global\\AnexeeMutex");

    if(hMutex == NULL || GetLastError() == ERROR_ALREADY_EXISTS) {
        if(!isService) {
            MessageBoxA(NULL, "Another instance is already running.", "Error",
                        MB_OK | MB_ICONERROR);
        }
        return 1;  // Exit immediately
    }


    // Ask user if they want to start (only in console/GUI mode)
    int result = IDYES;
    if(!isService) {
        result = MessageBoxA(NULL, "Do you want to start the client?", "Confirmation",
                             MB_YESNO | MB_ICONQUESTION | MB_DEFBUTTON2);
    }


    if(result == IDYES && !isService) {
        MessageBoxA(NULL, "Client Started", "Info", MB_OK);
    }


    if(result == IDNO) {
        return 0;  // Exit if user chooses No
    }

    if(argc > 1 && std::string(argv[1]) == "--debug") {
        g_debug = true;
    }
    if(g_debug && !isService) {
        // Allocate a console at runtime
        if(AllocConsole()) {
            FILE *fp;
            freopen_s(&fp, "CONOUT$", "w", stdout);
            freopen_s(&fp, "CONOUT$", "w", stderr);
            freopen_s(&fp, "CONIN$", "r", stdin);
            std::cout << "[DEBUG] Console attached" << std::endl;
        }
    }
    // Initialize global MQTT handler
    boost::asio::io_context ioc;
    g_mqttHandler = new MQTTHandler(ioc);

    // Connect to MQTT broker
    if(!g_mqttHandler->connect("216.48.184.131", "15579", "portal", "dt0Unw7QRh")) {
        log("Failed to connect to MQTT broker", LogLevel::ERRORS);
        return EXIT_FAILURE;
    } else {
        log("Connected to MQTT broker");
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
    if(token.contains("access_token")) {
        BearerToken = token["access_token"].get<std::string>();
    }

    vector<ServerInfoO> serverList = ParseServerHierarchy(BearerToken);

    log("endpoints: ");
    for(const auto &server : serverList) {
        log(server.name + " ", LogLevel::DEBUG);
        cout << (endl);

        // Print tags if they exist
        for(const auto &group : server.groups) {
            for(const auto &tag : group.tags) {
                if(tag.name) {
                    log(*tag.name + " ", LogLevel::DEBUG);
                }
                cout << endl;

                if(tag.rdWtOpt == "RD_WRT_RO") {
                    log("ReadOnly");
                } else if(tag.rdWtOpt == "RD_WRT_RW") {
                    if(tag.mappedInfospaceTags) {
                        for(const auto &infoSpace : *tag.mappedInfospaceTags) {
                            log(infoSpace.namespaces);

                            if(g_mqttHandler->isConnected()) {
                                g_mqttHandler->subscribe(infoSpace.namespaces);
                                log("Subscribing to topic: " + infoSpace.namespaces,
                                    LogLevel::DEBUG);
                            } else {
                                log("MQTT handler is not connected", LogLevel::DEBUG);
                            }

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

    // g_mqttHandler->mqtt_subscribe_and_update("TDSPL/Test31-01/tag20-1");

    // std::vector<ServerInfo> servers = { {"Anexee", "opc.tcp://localhost:53531"},
    //                                     {"Prosys", "opc.tcp://localhost:53530"} };

    for(const auto &server : serverList) {
        auto context = std::make_unique<ClientContext>();
        context->name = server.name;
        context->endpoint = server.endpointUrl;
        context->client.reset(UA_Client_new());

        // ADD LOGIC FOR CERTIFICATES
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

        // FOR SECURITY POLICY and MESSAGE SECURITY MODE

        if(server.msgSecurityMode != "NONE") {

            UA_ClientConfig *config = UA_Client_getConfig(context->client.get());
            config->logging = &myLogger;
            UA_ClientConfig_setDefaultEncryption(
                config, client_cert, client_key,
                trustList,       // trustList array
                1,               // trustListSize (number of certificates in trust list)
                revocationList,  // RevocationList
                1);              // RevocationListSize

            UA_String_clear(&config->applicationUri);
            UA_String_clear(&config->clientDescription.applicationUri);
            UA_LocalizedText_clear(&config->clientDescription.applicationName);
            UA_String_clear(&config->clientDescription.productUri);

            config->applicationUri = UA_STRING_ALLOC("urn:Anexee.server.application");
            config->clientDescription.applicationUri =
                UA_STRING_ALLOC("urn:Anexee.server.application");
            config->clientDescription.applicationName =
                UA_LOCALIZEDTEXT_ALLOC("en-US", "Anexee");
            config->clientDescription.productUri = UA_STRING_ALLOC("urn:Anexee");

            if(server.msgSecurityMode == "NONE") {
                config->securityMode = UA_MESSAGESECURITYMODE_NONE;
            } else if(server.msgSecurityMode == "OPC_UA_SM_SG") {
                config->securityMode = UA_MESSAGESECURITYMODE_SIGN;
            } else if(server.msgSecurityMode == "OPC_UA_SM_SG_ENC") {
                config->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
            } else {
                config->securityMode = UA_MESSAGESECURITYMODE_INVALID;
            }
            // std::string base = "http://opcfoundation.org/UA/SecurityPolicy#";
            // std::string full = base + server.securityPolicy;
            // config->securityPolicyUri = UA_STRING_STATIC(full.c_str());

            // config->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
            config->securityPolicyUri = UA_STRING_ALLOC(
                "http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep");
        } else {
            UA_ClientConfig *config = UA_Client_getConfig(context->client.get());
            config->logging = &myLogger;
            UA_ClientConfig_setDefault(config);
        }

        // after you configured UA_ClientConfig based on server settings
        context->connectOnce = [ctx=context.get(), server]() -> UA_StatusCode {
            if(server.authType == "anonymous") {
                return UA_Client_connect(ctx->client.get(), server.endpointUrl.c_str());
            } else if(server.authType == "user") {
                return UA_Client_connectUsername(ctx->client.get(),
                                                 server.endpointUrl.c_str(), "user1",
                                                 "password1");
            }
            return UA_STATUSCODE_BADIDENTITYTOKENINVALID;
        };

        context->onConnected = [ctx=context.get(), server]() {
            // base subscription (events)
            UA_CreateSubscriptionRequest req = UA_CreateSubscriptionRequest_default();
            UA_CreateSubscriptionResponse sub =
                UA_Client_Subscriptions_create(ctx->client.get(), req, nullptr, nullptr, nullptr);
            ctx->subscriptions[server.name] = sub;
            MonitorEvent(ctx->client.get(), ctx->subscriptions[server.name]);

#ifdef UA_ENABLE_SUBSCRIPTIONS
            // group subscriptions
            for(const auto &group : server.groups) {
                UA_CreateSubscriptionRequest greq = UA_CreateSubscriptionRequest_default();
                greq.requestedMaxKeepAliveCount = group.maxKeepAliveCount;
                greq.requestedPublishingInterval = group.publishingInterval;
                greq.requestedLifetimeCount = group.lifetimeCount;
                greq.priority = group.priority;
                greq.maxNotificationsPerPublish = group.maxNotificationsPerPublish;

                UA_CreateSubscriptionResponse gsub =
                    UA_Client_Subscriptions_create(ctx->client.get(), greq, nullptr, nullptr, nullptr);
                ctx->subscriptions[group.name] = gsub;
                log("Subscription Created:" + group.name);
            }
#endif

            // queue monitored items creation
            for(const auto &group : server.groups) {
                std::string groupName = group.name;
                for(const auto &tag : group.tags) {
                    if(!tag.mappedInfospaceTags) continue;
                    for(const auto &infoSpace : *tag.mappedInfospaceTags) {
                        std::lock_guard<std::mutex> lock(ctx->taskMutex);
                        ctx->taskQueue.push([ctx, infoSpace, groupName]() {
                            MyMonitorContext *myContext = new MyMonitorContext{infoSpace, g_mqttHandler};
                            MonitorItem(ctx->client.get(),
                                        ctx->subscriptions[groupName],
                                        Mapping[infoSpace.tagId].first.c_str(),
                                        infoSpace.tagId, myContext);
                        });
                    }
                }
            }
        };

        // cout<<"Event Monitoring Created"<<endl;

        context->startLoop();
        clientPool[context->endpoint] = context.get();
        clientContexts.push_back(std::move(context));
    }

    log("Client pool initialized. Press Ctrl+C to stop...");

    g_mqttHandler->setCallback([&clientPool](const std::string &topic,
                                             const std::string &payload) {
        log("Received message on topic " + topic + ": " + payload + "\n\n",
            LogLevel::DEBUG);

        json json_payload = json::parse(payload);

        if(!json_payload.contains("Data") || !json_payload["Data"].is_array())
            return;

        auto data = json_payload["Data"][0];
        if(!data.contains("TagId") || !data["TagId"].is_number_integer())
            return;

        int tagId = data["TagId"].get<int>();
        log("TagId: " + std::to_string(tagId) + "\n", LogLevel::DEBUG);

        if(!data.contains("UpdateType") || !data["UpdateType"].is_string())
            return;

        int updateType = data["UpdateType"].get<int>();
        log("UpdateType: " + std::to_string(updateType), LogLevel::DEBUG);

        std::string endpoint = Mapping[tagId].second;
        if(!clientPool.count(endpoint))
            return;

        auto context = clientPool.at(endpoint);
        if(!context->isConnected) {
            return;
        }
        log("Ready to use client:  (connected to " + context->endpoint + ")");

        // if (context->subscriptions[groupName].responseHeader.serviceResult !=
        // UA_STATUSCODE_GOOD ||
        //     context->subscriptions[groupName].subscriptionId == 0) {
        //     std::cerr << "Failed to create subscription" << std::endl;
        //     return;
        // }

        if(updateType == UpdateType::TELEMETERY) {
            // std::lock_guard<std::mutex> lock(context->taskMutex);
            // context->taskQueue.push([context, tagId]() {
            //     MonitorItem(context->client.get(), context->subscription,
            //                 Mapping[tagId].first.c_str(), tagId);
            // });

            log("TELEMETERY");

        } else if(updateType == UpdateType::COMMAND) {
            std::lock_guard<std::mutex> lock(context->taskMutex);
            context->taskQueue.push([context, tagId, data, topic,
                                     json_payload]() mutable {
                UA_Variant value;
                UA_Variant_init(&value);
                double val = data["Value"].get<double>();
                UA_Variant_setScalar(&value, &val, &UA_TYPES[UA_TYPES_DOUBLE]);

                auto nsAndValue = extractNsAndValue(Mapping[tagId].first);
                UA_StatusCode retval = UA_Client_writeValueAttribute(
                    context->client.get(),
                    UA_NODEID_STRING(nsAndValue.first,
                                     const_cast<char *>(nsAndValue.second.c_str())),
                    &value);

                if(retval != UA_STATUSCODE_GOOD) {
                    log("Failed to write value: " + string(UA_StatusCode_name(retval)),
                        LogLevel::ERRORS);
                } else {
                    log("Value written successfully");
                    g_mqttHandler->publish(topic, json_payload.dump());
                }

                UA_Variant_clear(&value);
            });
        } else if(updateType == UpdateType::BULKDATA) {
            log("BULKDATA");
        }
    });

    while(g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    log("Cleaning up...");

    for(auto &context : clientContexts) {
        UA_Client_disconnect(context->client.get());
        context->stopLoop();
    }

    clientPool.clear();
    clientContexts.clear();

    // Clean up MQTT handler at the end
    delete g_mqttHandler;

    if(hMutex) {
        ReleaseMutex(hMutex);
        CloseHandle(hMutex);
    }

    return EXIT_SUCCESS;
}

int
main(int argc, char *argv[]) {
    return runClient(false, argc, argv);
}

int WINAPI
WinMain(HINSTANCE hInstance, HINSTANCE hPrevInstance, LPSTR lpCmdLine, int nCmdShow) {
    SERVICE_TABLE_ENTRYA serviceTable[] = {
        {const_cast<LPSTR>(SERVICE_NAME), (LPSERVICE_MAIN_FUNCTIONA)ServiceMain},
        {nullptr, nullptr}};

    if(StartServiceCtrlDispatcherA(serviceTable)) {
        return 0;  // Running as service
    }
    // Not launched by SCM. Fall back to console/GUI mode.
    return main(__argc, __argv);
}

// Service helper implementations
static void
ReportSvcStatus(DWORD currentState, DWORD win32ExitCode, DWORD waitHint) {
    static DWORD checkPoint = 1;

    g_ServiceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    g_ServiceStatus.dwCurrentState = currentState;

    DWORD controlsAccepted = 0;
    if(currentState == SERVICE_START_PENDING) {
        controlsAccepted = 0;
    } else {
        controlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN;
    }
    g_ServiceStatus.dwControlsAccepted = controlsAccepted;
    g_ServiceStatus.dwWin32ExitCode = win32ExitCode;
    g_ServiceStatus.dwServiceSpecificExitCode = 0;
    g_ServiceStatus.dwWaitHint = waitHint;

    if(currentState == SERVICE_RUNNING || currentState == SERVICE_STOPPED) {
        g_ServiceStatus.dwCheckPoint = 0;
    } else {
        g_ServiceStatus.dwCheckPoint = checkPoint++;
    }

    if(g_StatusHandle)
        SetServiceStatus(g_StatusHandle, &g_ServiceStatus);
}

static void
LogEventWord(WORD type, const char *msg) {
    if(!g_EventLog)
        return;
    LPCSTR strings[1] = {msg};
    ReportEventA(g_EventLog, type, 0, 0, nullptr, 1, 0, strings, nullptr);
}

static void
SetWorkingDirectoryToExe() {
    char path[MAX_PATH] = {0};
    DWORD len = GetModuleFileNameA(nullptr, path, MAX_PATH);
    if(len == 0 || len == MAX_PATH)
        return;
    // Strip filename to directory
    for(int i = (int)len - 1; i >= 0; --i) {
        if(path[i] == '\\' || path[i] == '/') {
            path[i] = '\0';
            break;
        }
    }
    SetCurrentDirectoryA(path);
}

static void WINAPI
ServiceCtrlHandler(DWORD controlCode) {
    switch(controlCode) {
        case SERVICE_CONTROL_STOP:
        case SERVICE_CONTROL_SHUTDOWN:
            ReportSvcStatus(SERVICE_STOP_PENDING, NO_ERROR, 3000);
            g_running = false;
            if(g_ServiceStopEvent)
                SetEvent(g_ServiceStopEvent);
            return;
        default:
            break;
    }
}

static DWORD WINAPI
ServiceWorkerThread(LPVOID lpParam) {
    LogEventWord(EVENTLOG_INFORMATION_TYPE, "Service worker starting");
    SetWorkingDirectoryToExe();
    LogEventWord(EVENTLOG_INFORMATION_TYPE, "Working directory set to exe folder");
    // Reuse client logic in service mode
    int code = runClient(true, __argc, __argv);
    if(code != 0) {
        LogEventWord(EVENTLOG_ERROR_TYPE, "runClient exited with failure");
    } else {
        LogEventWord(EVENTLOG_INFORMATION_TYPE, "runClient exited cleanly");
    }
    return 0;
}

static void WINAPI
ServiceMain(DWORD argc, LPTSTR *argv) {
    g_StatusHandle = RegisterServiceCtrlHandlerA(SERVICE_NAME, ServiceCtrlHandler);
    if(!g_StatusHandle) {
        // Cannot report to SCM; nothing else we can do
        return;
    }

    g_EventLog = RegisterEventSourceA(nullptr, SERVICE_NAME);
    LogEventWord(EVENTLOG_INFORMATION_TYPE, "ServiceMain entered");

    ReportSvcStatus(SERVICE_START_PENDING, NO_ERROR, 3000);

    g_ServiceStopEvent = CreateEvent(nullptr, TRUE, FALSE, nullptr);
    if(!g_ServiceStopEvent) {
        LogEventWord(EVENTLOG_ERROR_TYPE, "CreateEvent failed");
        ReportSvcStatus(SERVICE_STOPPED, GetLastError(), 0);
        return;
    }

    HANDLE hThread = CreateThread(nullptr, 0, ServiceWorkerThread, nullptr, 0, nullptr);
    if(!hThread) {
        LogEventWord(EVENTLOG_ERROR_TYPE, "CreateThread failed");
        ReportSvcStatus(SERVICE_STOPPED, GetLastError(), 0);
        return;
    }

    ReportSvcStatus(SERVICE_RUNNING, NO_ERROR, 0);

    // Wait for either stop signal or worker thread exit
    HANDLE handles[2] = {g_ServiceStopEvent, hThread};
    bool stopping = false;
    while(true) {
        DWORD wait = WaitForMultipleObjects(2, handles, FALSE, INFINITE);
        if(wait == WAIT_OBJECT_0) { // stop event
            LogEventWord(EVENTLOG_INFORMATION_TYPE, "Stop event signaled");
            g_running = false;
            stopping = true;
            // Wait briefly for thread to exit
            WaitForSingleObject(hThread, 15000);
            break;
        } else if(wait == WAIT_OBJECT_0 + 1) { // thread exited
            LogEventWord(EVENTLOG_INFORMATION_TYPE, "Worker thread exited");
            break;
        } else {
            LogEventWord(EVENTLOG_WARNING_TYPE, "Unexpected wait result in ServiceMain");
        }
    }

    if(hThread)
        CloseHandle(hThread);
    if(g_ServiceStopEvent)
        CloseHandle(g_ServiceStopEvent);
    if(g_EventLog) {
        LogEventWord(EVENTLOG_INFORMATION_TYPE, "Service stopping");
        DeregisterEventSource(g_EventLog);
        g_EventLog = nullptr;
    }

    ReportSvcStatus(SERVICE_STOPPED, stopping ? NO_ERROR : NO_ERROR, 0);
}
