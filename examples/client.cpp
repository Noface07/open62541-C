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
 #include <future>
 #include <iomanip>
 #include <iostream>
 #include <memory>
 #include <mutex>
 #include <optional>
 #include <queue>
 #include <thread>
 #include <vector>
 
 #ifdef _WIN32
     #include <windows.h> // For Sleep(), service APIs
     #include <dirent.h> // for Directory functions
 #else
     #include <unistd.h>  // For usleep(), readlink, chdir
     #include <sys/file.h> // flock
     #include <fcntl.h>    // open
     #include <limits.h>   // PATH_MAX
 #endif
 
 #include "SqliteQueueService.h"
 #include "Logger.h"
 #include "MQTThandler.h"
 #include "Monitoring.h"
 #include "fetchAPI.h"
 #include "structs.h"
 #include <boost/asio.hpp>
 #include <unordered_map>
 #include <nlohmann/json.hpp>
#include "UserProfile.h"
#include "RedisClient.h"
#include "alarm_enums.h"

using namespace std;

// Global MQTT handler instance
MQTTHandler *g_mqttHandler = nullptr;
std::shared_ptr<AsyncPublisher> g_asyncPublisher = nullptr;

//Global SqliteQueueService instance
SqliteQueueService *g_sqliteService = nullptr;

static std::once_flag security_policies_loaded;
static std::mutex cert_loading_mutex;

unordered_map<string, int> groupIdMap;

std::atomic<bool> g_running(true);
std::string APIusername = "bhupesh.paliwal@techondater.com";
std::string APIpassword = "Admin@123";


#ifdef _WIN32
// Windows Service globals
static SERVICE_STATUS g_ServiceStatus;
static SERVICE_STATUS_HANDLE g_StatusHandle = nullptr;
static HANDLE g_ServiceStopEvent = nullptr;
static const char *SERVICE_NAME = "AnexeeOPCUAClient";
static HANDLE g_EventLog = nullptr;

// Forward declarations for service (Windows only)
static void ReportSvcStatus(DWORD currentState, DWORD win32ExitCode, DWORD waitHint);
static void WINAPI ServiceMain(DWORD argc, LPTSTR *argv);
static void WINAPI ServiceCtrlHandler(DWORD controlCode);
static DWORD WINAPI ServiceWorkerThread(LPVOID lpParam);
static void LogEventWord(WORD type, const char *msg);

#endif

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
                            std::this_thread::sleep_for(std::chrono::seconds(10));
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
                
                                // ADD NULL CHECK HERE
                                if (!client) {
                                    log(name + ": Client is null, reconnecting...", LogLevel::ERRORS);
                                    isConnected = false;
                                    std::this_thread::sleep_for(std::chrono::seconds(1));
                                    continue;
                                }

                UA_StatusCode code = UA_Client_run_iterate(client.get(), 100);
                if(code != UA_STATUSCODE_GOOD) {
                    log(name + ": UA_Client_run_iterate failed with " +
                            UA_StatusCode_name(code), LogLevel::ERRORS);
                    UA_Client_disconnect(client.get());

                    client.reset(); 


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

#ifdef _WIN32
static size_t
loadCertsFromDirectory(const char *dirPath, UA_ByteString **certs) {
    char searchPath[512];
    snprintf(searchPath, sizeof(searchPath), "%s\\*.der", dirPath);

    WIN32_FIND_DATAA findData;
    HANDLE hFind = FindFirstFileA(searchPath, &findData);

    if (hFind == INVALID_HANDLE_VALUE) {
        return 0;
    }

    std::vector<std::string> derFiles;
    do {
        derFiles.push_back(findData.cFileName);
    } while (FindNextFileA(hFind, &findData) != 0);
    FindClose(hFind);

    size_t count = derFiles.size();
    if (count == 0) {
        return 0;
    }

    *certs = (UA_ByteString*)UA_malloc(sizeof(UA_ByteString) * count);
    for (size_t i = 0; i < count; ++i) {
        char fullpath[512];
        snprintf(fullpath, sizeof(fullpath), "%s\\%s", dirPath, derFiles[i].c_str());
        (*certs)[i] = loadFile(fullpath);
    }

    return count;
}
#else
/* Load all .der files from a directory */
static size_t
loadCertsFromDirectory(const char *dirPath, UA_ByteString **certs) {
    DIR *dir = opendir(dirPath);
    if(!dir) return 0;

    struct dirent *entry;
    std::vector<std::string> derFiles;
    while((entry = readdir(dir)) != NULL) {
        if(strstr(entry->d_name, ".der"))
            derFiles.push_back(entry->d_name);
    }
    
    size_t count = derFiles.size();
    if(count > 0) {
        *certs = (UA_ByteString*)UA_malloc(sizeof(UA_ByteString) * count);
        for(size_t i = 0; i < count; i++) {
            char fullpath[512];
            snprintf(fullpath, sizeof(fullpath), "%s/%s", dirPath, derFiles[i].c_str());
            (*certs)[i] = loadFile(fullpath);
        }
    }
    
    closedir(dir);
    return count;
}
#endif

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
            // log(buffer, LogLevel::DEBUG); // Silenced library debug logs
            break;
    }
}

// Custom logger plugin
static UA_Logger myLogger = {myLog, nullptr, nullptr};


std::future<std::string> getBearerTokenNow(std::string applicationEndURLHost, std::string applicationEndURLPort, std::string authUsername, std::string authPassword, bool blocking = true) {
    const int retryDelay = 10; // seconds
    int attempt = 1;

    auto fut = std::async(std::launch::async, [&]() {  // <-- capture by reference
        std::string token;
        bool tokenSuccess = false;

        while (!tokenSuccess) {
            try {
                auto futureToken = std::async(std::launch::async, [&]() {
                    return ::getBearerToken(applicationEndURLHost, applicationEndURLPort, authUsername, authPassword);
                });

                json tokenResponse = futureToken.get();

                if (tokenResponse.contains("access_token")) {
                    token = tokenResponse["access_token"].get<std::string>();
                    tokenSuccess = true;
                    std::cout << "Bearer token fetched successfully!" << std::endl;
                    return token;
                } else {
                    std::cout << "Invalid token response - retrying in " 
                            << retryDelay << " seconds..." << std::endl;
                }
            } catch (const std::exception& e) {
                std::cout << "Token fetch failed: " << e.what() << std::endl;
            }

            if (!tokenSuccess) {
                std::this_thread::sleep_for(std::chrono::seconds(retryDelay));
                attempt++;   // ✅ now valid, because attempt is captured by reference
            }
        }

        return token;
    });

    if (blocking) {
        // Consume future immediately and return a ready future
        std::promise<std::string> p;
        p.set_value(fut.get());
        return p.get_future();
    }

    return fut; // async: caller decides when to wait
}


// Extracted main logic so it can be reused by console and service
static int
runClient(bool isService, int argc, char *argv[]) {

    SetWorkingDirectoryToExe(); // ensure CWD is the exe folder

    std::ifstream file("appsettings.json");
    if (!file.is_open()) {
        std::string cwdStr;
#ifdef _WIN32
        char cwd[MAX_PATH]; GetCurrentDirectoryA(MAX_PATH, cwd);
        cwdStr = cwd;
#else
        char cwd[PATH_MAX];
        if(getcwd(cwd, sizeof(cwd))) cwdStr = cwd; else cwdStr = "";
#endif
        log(std::string("Could not open appsettings.json. CWD=") + cwdStr, LogLevel::ERRORS);
        return 1;
    }

    // Parse JSON
    json config;
    file >> config;

    // Extract values


        // Extract AppSettings
        std::string applicationEndURL = config["AppSettings"]["ApplicationEndURL"].get<std::string>();
        std::string applicationEndURLHost = config["AppSettings"]["ApplicationEndURLHost"].get<std::string>();
        int applicationEndURLPort = config["AppSettings"]["ApplicationEndURLPort"].get<int>();

        // Extract MqttConfig
        std::string brokerAddress = config["MqttConfig"]["MqttSettings"][0]["BrokerAddress"].get<std::string>();
        int brokerPort = config["MqttConfig"]["MqttSettings"][0]["BrokerPort"].get<int>();
        std::string mqttUsername = config["MqttConfig"]["MqttSettings"][0]["Username"].get<std::string>();
        std::string mqttPassword = config["MqttConfig"]["MqttSettings"][0]["Password"].get<std::string>();
    
        // Extract Authorization
        std::string authUsername = config["Authorization"]["Username"].get<std::string>();
        std::string authPassword = config["Authorization"]["Password"].get<std::string>();

        //Extract NodeID
        std::string NodeID = config["ConfigurationSettings"]["NodeID"].get<std::string>();
    
        // Extract Payload
        std::string dbPath = config["Payload"]["OfflineQueueOptions"]["DbPath"].get<std::string>();
        int retentionDays = config["Payload"]["OfflineQueueOptions"]["RetentionDays"].get<int>();
        int retryBatchSize = config["Payload"]["OfflineQueueOptions"]["RetryBatchSize"].get<int>();

        // Extract RedisConfig
        std::string redisHost = config["RedisConfig"]["Host"].get<std::string>();
        int redisPort = config["RedisConfig"]["Port"].get<int>();
        std::string redisPassword = config["RedisConfig"]["Password"].get<std::string>();
        int redisDb = config["RedisConfig"]["DbIndex"].get<int>();
        
        // Initialize Redis Client
        // Use "OPCUA_SERVER:" prefix to match the server's storage for User Profiles
        std::string uniquePrefix = "OPCUA_SERVER:"; 
        log("Initializing Redis Client with prefix: " + uniquePrefix, LogLevel::INFO);
        g_redisClient.init(redisHost, redisPort, redisPassword, redisDb, uniquePrefix);
        if (!g_redisClient.connect()) {
            log("Failed to connect to Redis at startup. Cache will be disabled.", LogLevel::WARNING);
        } else {
            log("Successfully connected to Redis.", LogLevel::INFO);
        }



        #ifdef _WIN32
    HANDLE hMutex = CreateMutexA(NULL, TRUE, "Global\\AnexeeMutex");

    if(hMutex == NULL || GetLastError() == ERROR_ALREADY_EXISTS) {
        if(!isService) {
            MessageBoxA(NULL, "Another instance is already running.", "Error",
                        MB_OK | MB_ICONERROR);
        }
        return 1;  // Exit immediately
    }
    #else
    int lockFd = open("/tmp/anexee-opcua-client.lock", O_CREAT | O_RDWR, 0666);
    if(lockFd < 0 || flock(lockFd, LOCK_EX | LOCK_NB) != 0) {
        log("Another instance is already running (lock file)", LogLevel::ERRORS);
        if(lockFd >= 0) close(lockFd);
        return 1;
    }
#endif


    // Ask user if they want to start (only in console/GUI mode)
    #ifdef _WIN32
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
#endif

    // Global logging control - only enable file logging with --debug
    g_logging_enabled = false; // Default: no file logging
    
    if(argc > 1 && std::string(argv[1]) == "--debug") {
        g_debug = true;
        g_logging_enabled = true; // Enable file logging in debug mode
    } else {
        g_debug = false;
    }
    
    // Initialize logging with client-specific folder (only if logging is enabled)
    init_logging("logs/client", "client", true);
    if (g_logging_enabled) {
        log("Client logging initialized with day-wise log files", LogLevel::INFO);
    } else {
        std::cout << "Client started - no file logging (use --debug to enable)" << std::endl;
    }
    
    if(g_debug && !isService) {
        #ifdef _WIN32
        // Allocate a console at runtime
        if(AllocConsole()) {
            FILE *fp;
            freopen_s(&fp, "CONOUT$", "w", stdout);
            freopen_s(&fp, "CONOUT$", "w", stderr);
            freopen_s(&fp, "CONIN$", "r", stdin);
            std::cout << "[DEBUG] Console attached" << std::endl;
        }
        #endif
    }


    // Retry logic for API calls with constant 10-second intervals
    string BearerToken = getBearerTokenNow(applicationEndURLHost, std::to_string(applicationEndURLPort), authUsername, authPassword, true).get();
    log("Bearer token obtained successfully", LogLevel::INFO);
    
    vector<ServerInfoO> serverList;
        
    // Retry ParseServerHierarchy with constant 10-second intervals (infinite retries)
    const int retryDelay = 10; // seconds
    bool hierarchySuccess = false;
    int attempt = 1;
    
    while (!hierarchySuccess) {
        try {
            log("Attempt " + std::to_string(attempt) + " - Fetching server hierarchy...", LogLevel::INFO);

            // 1. Try Redis First
            std::string redisKey = "OPCUA_HIERARCHY_" + NodeID;
            bool cacheHit = false;

            if (g_redisClient.isConnected()) {
                log("Redis is CONNECTED. Checking key: " + redisKey, LogLevel::INFO);
                auto fromCache = g_redisClient.get(redisKey);
                
                if (fromCache.has_value()) {
                    log("Cache Hit! Loading hierarchy for " + NodeID + " from Redis.",
                        LogLevel::INFO);
                    try {
                        json cachedJson = json::parse(fromCache.value());
                        serverList = ParseServerHierarchyFromJson(cachedJson);
                        if (!serverList.empty()) {
                            hierarchySuccess = true;
                            cacheHit = true;
                            log("Hierarchy loaded from Redis successfully.", LogLevel::INFO);
                        } else {
                            log("Redis data invalid or empty. Falling back to API.", LogLevel::WARNING);
                        }
                    } catch (const std::exception& e) {
                       log("Error parsing Redis data: " + std::string(e.what()) + ". Falling back to API.", LogLevel::ERRORS); 
                    }
                } else {
                    log("Cache Miss " + redisKey + ". Fetching from API...", LogLevel::INFO);
                }
            }

            // 2. Fallback to API if Cache Miss or Parse Failure 
            if (!cacheHit) {
                std::string target = "/api/GetOpcUaHierarchy";
                // JSON body
                json jBody;
                jBody["orgId"] = 0;
                jBody["roleId"] = "";
                jBody["userId"] = 0;
                jBody["moduleId"] = 0;
                jBody["userType"] = "";
                jBody["requestDateTime"] = "2024-12-26T08:16:05.629Z"; // Keep hardcoded for now or use dynamic
                jBody["ipAddress"] = "";
                jBody["originName"] = "";
                jBody["filterModel"]["customValue"] = NodeID;

                std::string json_body = jBody.dump();
                
                // Call getResponse directly to get the raw JSON for caching
                auto futureResponse = std::async(std::launch::async, getResponse, 
                                                applicationEndURLHost, std::to_string(applicationEndURLPort), 
                                                BearerToken, json_body, target);
                
                json apiResponse = futureResponse.get();

                // Cache the fresh response
                if (!apiResponse.is_null()) {
                     g_redisClient.setCompressed(redisKey, apiResponse.dump(), 0);
                     log("Cached fresh hierarchy to Redis key: " + redisKey, LogLevel::INFO);
                }

                // Parse
                serverList = ParseServerHierarchyFromJson(apiResponse);
                
                if (!serverList.empty()) {
                    hierarchySuccess = true;
                    log("Server hierarchy fetched from API successfully! Found " + std::to_string(serverList.size()) + " servers.", LogLevel::INFO);
                } else {
                    log("Empty server hierarchy response - retrying in " + std::to_string(retryDelay) + " seconds...", LogLevel::ERRORS);
                }
            }

        } catch (const std::exception& e) {
            log("Hierarchy fetch failed: " + std::string(e.what()), LogLevel::ERRORS);
        }
        
        if (!hierarchySuccess) {
            log("Retrying in " + std::to_string(retryDelay) + " seconds...", LogLevel::INFO);
            std::this_thread::sleep_for(std::chrono::seconds(retryDelay));
            attempt++;
        }
    }

    // Build datapointId -> orgId map from your server/device config (declare outside try block)
    std::map<int, long> dpToOrg;
    for (const auto& server : serverList) {
        for (const auto& group : server.groups) {
            for (const auto& tag : group.tags) {
                if (tag.mappedInfospaceTags) {
                    for (const auto& m : *tag.mappedInfospaceTags) {
                        dpToOrg[tag.dataPointId] = static_cast<long>(m.orgId);
                    }
                }
            }
        }
    }
    
    // The Mapping variable is already populated by ParseServerHierarchy in fetchAPI.cpp
    // Let's log the populated mapping for debugging
    //log("Mapping populated with " + std::to_string(Mapping.size()) + " entries:");
    //for (const auto& entry : Mapping) {
    //    log("TagId " + std::to_string(entry.first) + " -> " + entry.second.first + " @ " + entry.second.second);
    //}

    // Initialize SqliteQueueService
    try {
        OfflineQueueOptions options;
        options.batchSize = 100; // Example value
        options.uploadIntervalSeconds = 15; // Example value
        // The DB file will be created in the current working directory
        g_sqliteService = new SqliteQueueService(dbPath, options);
        g_sqliteService->StartQueueWorker(); // Start the DB writer thread immediately
        log("SqliteQueueService initialized.", LogLevel::INFO);

        // After: g_sqliteService = new SqliteQueueService("OfflineData.db", options);
        g_sqliteService->SetApiUrl("http://164.52.221.177:5128/api/UploadBulkTagData");   // required
        string BearerToken2 = getBearerTokenNow(applicationEndURLHost, std::to_string(applicationEndURLPort), authUsername, authPassword, true).get();
        g_sqliteService->SetApiAuth(BearerToken2);  

        // Set callback for token refresh on 401
        g_sqliteService->SetTokenRefreshCallback([=]() -> std::string {
            try {
                log("[Callback] Refreshing bearer token...", LogLevel::INFO);
                return getBearerTokenNow(applicationEndURLHost, std::to_string(applicationEndURLPort), authUsername, authPassword, true).get();
            } catch (const std::exception& e) {
                 log("[Callback] Token refresh failed: " + std::string(e.what()), LogLevel::ERRORS);
                 return "";
            }
        });

        nlohmann::json apiMetadata;
        apiMetadata["OrgId"] = 1;
        apiMetadata["RoleId"] = "1";
        apiMetadata["UserId"] = 1;
        apiMetadata["ModuleId"] = 789;  
        apiMetadata["UserType"] = "system";
        apiMetadata["IpAddress"] = serverList[0].endpointUrl;
        apiMetadata["OriginName"] = "OPCUAClient";
        apiMetadata["EntityId"] = 42;
        g_sqliteService->SetApiMetadata(apiMetadata);

        g_sqliteService->SetConfigurations(dpToOrg);

    } catch (const std::exception& e) {
        log("FATAL: Failed to initialize SqliteQueueService: " + std::string(e.what()), LogLevel::ERRORS);
        return EXIT_FAILURE;
    }

    // Initialize global MQTT handler OUTSIDE the try block to ensure proper scope
    log("Creating MQTT handler...", LogLevel::INFO);
    boost::asio::io_context ioc;
    boost::asio::ssl::context ssl_ctx{boost::asio::ssl::context::tlsv12};
    
    // Optional: Configure SSL context for certificate verification
    // ssl_ctx.set_verify_mode(boost::asio::ssl::verify_peer);
    // ssl_ctx.load_verify_file("ca-cert.pem");
    
    try {
        g_mqttHandler = new MQTTHandler(ioc, ssl_ctx);
        log("MQTT handler initialized successfully", LogLevel::INFO);
        
        // Initialize Async Publisher
        g_asyncPublisher = std::make_shared<AsyncPublisher>(g_mqttHandler);
        log("AsyncPublisher initialized.", LogLevel::INFO);
        
        // Connect MQTTHandler with SqliteQueueService for proper state management
        if (g_sqliteService) {
            g_mqttHandler->setSqliteService(g_sqliteService);
            log("Connected MQTTHandler with SqliteQueueService", LogLevel::INFO);
        }
        
        // Give the MQTT thread a moment to start
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Define callbacks for MQTT connection status
        auto onMqttConnect = []() {
            log("CLIENT CALLBACK: MQTT (re)connected. Processing any offline data...", LogLevel::INFO);
            if (g_sqliteService) {
                // Requirement 1 & 4: Publish latest values to MQTT
                log("CLIENT CALLBACK: About to call PublishLatestValuesToMqtt...", LogLevel::INFO);
                g_sqliteService->PublishLatestValuesToMqtt(
                    [](const std::string& topic, const std::string& payload) {
                        log("CLIENT CALLBACK LAMBDA: Publishing to topic: " + topic + " with payload size: " + std::to_string(payload.size()), LogLevel::INFO);
                        if (g_mqttHandler) {
                            log("CLIENT CALLBACK LAMBDA: Calling g_mqttHandler->publish", LogLevel::INFO);
                            bool result = g_mqttHandler->publish(topic, payload);
                            log("CLIENT CALLBACK LAMBDA: Publish result: " + std::string(result ? "SUCCESS" : "FAILED"), LogLevel::INFO);
                        } else {
                            log("CLIENT CALLBACK LAMBDA: g_mqttHandler is null!", LogLevel::ERRORS);
                        }
                    }
                );
                log("CLIENT CALLBACK: PublishLatestValuesToMqtt completed", LogLevel::INFO);
                // Requirement 1 & 4: Start uploading the full backlog to the API
                log("CLIENT CALLBACK: Starting API upload timer...", LogLevel::INFO);
                g_sqliteService->StartApiUploadTimer();
                log("CLIENT CALLBACK: API upload timer started!", LogLevel::INFO);
            } else {
                log("CLIENT CALLBACK: g_sqliteService is null!", LogLevel::ERRORS);
            }
        };
    
        auto onMqttDisconnect = []() {
            log("CLIENT CALLBACK: MQTT disconnected. Switching to offline mode. Data will be queued.", LogLevel::ERRORS);
            if (g_sqliteService) {
                // Requirement 3: Stop trying to upload to API when MQTT is down
                log("CLIENT CALLBACK: Stopping API upload timer...", LogLevel::INFO);
                g_sqliteService->StopApiUploadTimer();
                log("CLIENT CALLBACK: API upload timer stopped!", LogLevel::INFO);
            }
        };
        
        // Handle failed messages that couldn't be published
        auto onFailedMessages = [&dpToOrg](const std::vector<PendingMessage>& failedMessages) {
            if (!g_sqliteService) {
                log("Cannot save failed messages: SqliteService not available", LogLevel::ERRORS);
                return;
            }
            
            log("Processing " + std::to_string(failedMessages.size()) + " failed MQTT messages for database storage", LogLevel::INFO);
            int successCount = 0;
            int errorCount = 0;
            
            for (const auto& msg : failedMessages) {
                try {
                    // Parse the JSON payload to extract the MqttPayload data
                    nlohmann::json payload = nlohmann::json::parse(msg.payload);
                    if (payload.contains("Data") && payload["Data"].is_array() && !payload["Data"].empty()) {
                        auto data = payload["Data"][0];
                        
                        MqttPayload p;
                        p.datapointId = data.value("DatapointId", 0);
                        p.name = ""; // We don't have the name in the JSON
                        p.tagId = data.value("TagId", 0);
                        p.tagType = data.value("TagType", "INFO_DCR");
                        p.source = data.value("Source", static_cast<int>(AlarmSource::AEEngine));
                        p.infoId = data.value("InfoId", 1001);
                        p.value = data["Value"];
                        p.timeStamp = data.value("TimeStamp", "");
                        p.quality = data.value("Quality", static_cast<int>(AlarmQuality::Good));
                        p.UpdateType = data.value("UpdateType", static_cast<int>(UpdateType::Telemetry));
                        
                        // Improved orgId lookup with proper fallback
                        long orgId = 1; // default fallback
                        if (p.datapointId > 0) { 
                            auto it = dpToOrg.find(p.datapointId);
                            if (it != dpToOrg.end()) {
                                orgId = it->second;
                                log("Found orgId " + std::to_string(orgId) + " for datapointId " + std::to_string(p.datapointId), LogLevel::DEBUG);
                            } else {
                                log("No orgId mapping found for datapointId " + std::to_string(p.datapointId) + ", using fallback", LogLevel::INFO);
                            }
                        }
                        
                        g_sqliteService->EnqueueMessage(msg.topic, p, orgId);
                        successCount++;
                        log("Queued failed message to database: " + msg.topic + " (orgId: " + std::to_string(orgId) + ")", LogLevel::DEBUG);
                    } else {
                        log("Failed message has invalid JSON structure: " + msg.payload, LogLevel::ERRORS);
                        errorCount++;
                    }
                } catch (const std::exception& e) {
                    log("Failed to parse failed message payload: " + std::string(e.what()) + " | Payload: " + msg.payload, LogLevel::ERRORS);
                    errorCount++;
                }
            }
            
            log("Failed message processing complete: " + std::to_string(successCount) + " saved, " + std::to_string(errorCount) + " errors", LogLevel::INFO);
        };

        g_mqttHandler->setOnConnectCallback(onMqttConnect);
        g_mqttHandler->setOnDisconnectCallback(onMqttDisconnect);
        g_mqttHandler->setOnFailedMessageCallback(onFailedMessages);

        // ... existing code ...

        // Fix other errors reported by user (lines ~1198, ~1204, ~1264)
        // Need to locate where UpdateType::TELEMETRY etc. are used and fix typos/scope.
        // Assuming context is inside a loop processing messages or similar.
        // The error log indicates specific lines. I will replace the whole block around these lines if possible or target them.
        
        // Wait, I am replacing lines 714-750 first block.
        // And then I will do another replacement for lines ~1198.


    } catch (const std::exception& e) {
        log("FATAL: Failed to initialize MQTT handler: " + std::string(e.what()), LogLevel::ERRORS);
        return EXIT_FAILURE;
    }

    // Connect to MQTT broker (non-blocking)
    log("Attempting to connect to MQTT broker...", LogLevel::INFO);
    if(!g_mqttHandler->connect(brokerAddress, std::to_string(brokerPort), mqttUsername, mqttPassword)) {
        log("Failed to initiate MQTT broker connection", LogLevel::ERRORS);
        return EXIT_FAILURE;
    } else {
        log("MQTT connection initiated (async)", LogLevel::INFO);
    }
    
    // Give MQTT a moment to start connecting
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

#ifdef UA_ENABLE_SUBSCRIPTIONS
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);
#endif

    std::vector<std::unique_ptr<ClientContext>> clientContexts;
    std::unordered_map<std::string, ClientContext *> clientPool;



    std::vector<std::string> topicsToSubscribe;
    //log("Collecting topics to subscribe...", LogLevel::INFO);
    
    for(const auto &server : serverList) {
        //log(server.name + " ", LogLevel::DEBUG);
        //cout << (endl);

        // Print tags if they exist
        for(const auto &group : server.groups) {
            for(const auto &tag : group.tags) {
                //if(tag.name) {
                //    //log(*tag.name + " ", LogLevel::DEBUG);
                //}
                //cout << endl;

                if(tag.rdWtOpt == "RD_WRT_RO") {
                    //log("ReadOnly");
                } else if(tag.rdWtOpt == "RD_WRT_RW") {
                    if(tag.mappedInfospaceTags) {
                        for(const auto &infoSpace : *tag.mappedInfospaceTags) {
                            //log(infoSpace.namespaces);
                            topicsToSubscribe.push_back(infoSpace.namespaces);
                            // cout << infoSpace.namespaces << " subscribed; ";
                            //cout << endl;
                        }
                    }
                }
            }
        }
    }
    //cout << endl;

    if (!topicsToSubscribe.empty()) {
        log("Waiting for MQTT connection before subscribing...", LogLevel::INFO);
        // Wait up to 10 seconds for connection
        int retries = 0;
        while (!g_mqttHandler->isConnected() && retries < 100) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            retries++;
        }

        if (g_mqttHandler->isConnected()) {
            size_t total = topicsToSubscribe.size();
            size_t batchSize = 500;
            log("Starting batch subscription for " + std::to_string(total) + " topics...", LogLevel::INFO);
            
            for (size_t i = 0; i < total; i += batchSize) {
                size_t end = std::min(i + batchSize, total);
                std::vector<std::string> batch(topicsToSubscribe.begin() + i, topicsToSubscribe.begin() + end);
                
                log("Subscribing to batch " + std::to_string(i/batchSize + 1) + 
                    " (" + std::to_string(batch.size()) + " topics)...", LogLevel::INFO);
                
                g_mqttHandler->subscribeBatch(batch);
                
                // Small delay between batches to avoid overwhelming the broker
                std::this_thread::sleep_for(std::chrono::milliseconds(200)); 
            }
            log("All batch subscriptions initiated.", LogLevel::INFO);
        } else {
            log("Failed to connect to MQTT broker after waiting. Subscriptions skipped.", LogLevel::ERRORS);
        }
    } else {
        log("No topics found to subscribe.", LogLevel::INFO);
    }

    // *** FIX START ***
    // Add this block BEFORE the main "for" loop
    std::call_once(security_policies_loaded, []() {
        log("Performing one-time global security policy initialization...", LogLevel::INFO);
        // Create a temporary client just to trigger the OpenSSL policy loading
        UA_Client *tempClient = UA_Client_new();
        UA_ClientConfig_setDefault(UA_Client_getConfig(tempClient));
        // Deleting the client is enough to ensure policies are loaded and ready
        UA_Client_delete(tempClient);
        log("Global security policies initialized.", LogLevel::INFO);
    });
    // *** FIX END ***

    

    for(const auto &server : serverList) {
        auto server_copy = server;
        auto context = std::make_unique<ClientContext>();
        context->name = server.name;
        context->endpoint = server.endpointUrl;
        
        
    
        // The rest of your loop (connectOnce, onConnected lambdas) remains the same as the previous fix
        context->connectOnce = [ctx=context.get(), server = server_copy]() -> UA_StatusCode {

            // Create a new client and get its fresh config
        ctx->client.reset(UA_Client_new());
        UA_ClientConfig *config = UA_Client_getConfig(ctx->client.get());
        UA_ClientConfig_setDefault(config); // Start with a default config for EVERY client
        config->logging = &myLogger; // Apply your custom logger

        UA_String_clear(&config->clientDescription.applicationUri);
        config->clientDescription.applicationUri =
            UA_STRING_ALLOC("urn:Anexee.client.application");
    
        // --- Start of Corrected Security Logic ---
    
        if(server.msgSecurityMode != "NONE" && !server.msgSecurityMode.empty()) {

                        // ADD MUTEX LOCK HERE
                        std::lock_guard<std::mutex> lock(cert_loading_mutex);

            // This server requires security. Load certificates and apply them.
            UA_ByteString client_cert = loadFile("client/own/certs/client_cert.der");
            UA_ByteString client_key = loadFile("client/own/certs/client_key.der");
            
            // Load trusted certificates from directory
            UA_ByteString *trustList = NULL;
            size_t trustListSize = loadCertsFromDirectory("client/trusted/certs", &trustList);
            log("Loaded " + std::to_string(trustListSize) + " trusted certificate(s) for " + server.name, LogLevel::INFO);
            
            // Load issuer certificates from directory  
            UA_ByteString *issuerList = NULL;
            size_t issuerListSize = loadCertsFromDirectory("client/issuers/certs", &issuerList);
            log("Loaded " + std::to_string(issuerListSize) + " issuer certificate(s) for " + server.name, LogLevel::INFO);
            
            // Load revocation list from directory
            UA_ByteString *revocationList = NULL;
            size_t revocationListSize = loadCertsFromDirectory("client/trusted/crl", &revocationList);
            log("Loaded " + std::to_string(revocationListSize) + " revocation certificate(s) for " + server.name, LogLevel::INFO);
    
            if (client_cert.length > 0 && client_key.length > 0) {
                UA_ClientConfig_setDefaultEncryption(
                    config, client_cert, client_key,
                    trustList, trustListSize,
                    revocationList, revocationListSize);
                    
                log("Encryption configured for secure connection to " + server.name, LogLevel::INFO);
            } else {
                 log("Warning: Could not load client certificate or key for secure server " + server.name, LogLevel::ERRORS);
            }
    
            // Clean up byte strings after use
            UA_ByteString_clear(&client_cert);
            UA_ByteString_clear(&client_key);
            
            if(trustList) {
                for(size_t i = 0; i < trustListSize; i++)
                    UA_ByteString_clear(&trustList[i]);
                UA_free(trustList);
            }
            
            if(issuerList) {
                for(size_t i = 0; i < issuerListSize; i++)
                    UA_ByteString_clear(&issuerList[i]);
                UA_free(issuerList);
            }
            
            if(revocationList) {
                for(size_t i = 0; i < revocationListSize; i++)
                    UA_ByteString_clear(&revocationList[i]);
                UA_free(revocationList);
            }
    
            // Set security mode and policy based on this server's config
            if(server.msgSecurityMode == "OPC_UA_SM_SG") {
                config->securityMode = UA_MESSAGESECURITYMODE_SIGN;
            } else if(server.msgSecurityMode == "OPC_UA_SM_SG_ENC") {
                config->securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
            }
            
            // This part needs to be dynamic based on your securityPolicy from JSON
            const std::string &policy = server.securityPolicy;

            //UA_String_clear(&config->securityPolicyUri);

            if(policy == "UA_SP_BASIC256") {
                config->securityPolicyUri = UA_STRING_ALLOC(
                    (char*)"http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256");
            } else if(policy == "UA_SP_AES128") {
                config->securityPolicyUri =
                    UA_STRING_ALLOC(
                    (char *)"http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep");
            } else if(policy == "UA_SP_AES256") {
                config->securityPolicyUri =
                    UA_STRING_ALLOC(
                    (char *)"http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss");
            } else {
                config->securityPolicyUri = UA_STRING_ALLOC(
                    (char *)"http://opcfoundation.org/UA/SecurityPolicy#None");
            }

        } 
        // If msgSecurityMode is "NONE", we do nothing extra. The UA_ClientConfig_setDefault already handled it.



    
        // --- End of Corrected Security Logic ---
            if(server.authType == "AUTH_STG_ANYMS" || server.authType == "AUTH_STG_ANYMS") {
                return UA_Client_connect(ctx->client.get(), server.endpointUrl.c_str());
            } else if(server.authType == "AUTH_STG_AUTH") {
                std::string user = server.username;
                std::string pass = server.password;

                // Fallback to global credentials if server-specific ones are missing
                if(user.empty()) user = APIusername;
                if(pass.empty()) pass = APIpassword;

                return UA_Client_connectUsername(ctx->client.get(),
                                                 server.endpointUrl.c_str(),
                                                 user.c_str(),
                                                 pass.c_str());
            }
            else if(server.authType == "AUTH_STG_CERT") {
                return UA_Client_connect(ctx->client.get(), server.endpointUrl.c_str());
            }
            return UA_STATUSCODE_BADIDENTITYTOKENINVALID;
        };

        //-------------------------------------------------------------------------

            UserProfile profile;
        try {
            std::string json_body = "{}";
            std::string cacheKey = "USER_PROFILE_" + APIusername;
            nlohmann::ordered_json profileJson;
            bool cacheHit = false;

            if(true) {
                auto start_time = std::chrono::steady_clock::now();
                auto cachedVal = g_redisClient.get(cacheKey);
                auto end_time = std::chrono::steady_clock::now();
                auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                      end_time - start_time)
                                      .count();

                if(cachedVal) {
                    try {
                        profileJson = nlohmann::ordered_json::parse(*cachedVal);
                        profile = ParseUserProfileFromJson(profileJson);
                        
                        // Validate critical fields
                        if(!profile.currentOrgCode.empty() && !profile.currentOrgId.empty()) {
                            cacheHit = true;
                            log("⚡ Redis Cache HIT for UserProfile (User: " + APIusername +
                                    ") - Fetched in " + std::to_string(elapsed_ms) + " ms",
                                LogLevel::INFO);
                        } else {
                            log("⚠️ Cached UserProfile incomplete (missing OrgCode/ID). Forcing refresh.", LogLevel::WARNING);
                            cacheHit = false;
                        }
                    } catch(const std::exception &e) {
                        log("⚠️ Redis Cache Parse Error for UserProfile: " +
                                std::string(e.what()),
                            LogLevel::WARNING);
                    }
                } else {
                    if(g_redisClient.isConnected())
                        log("📉 Redis Cache MISS for UserProfile (User: " + APIusername +
                                ") - Checked in " + std::to_string(elapsed_ms) + " ms",
                            LogLevel::INFO);
                }
            }

            if(!cacheHit) {
                auto token = getBearerTokenNow(applicationEndURLHost,
                                               std::to_string(applicationEndURLPort),
                                               APIusername, APIpassword, true).get();

                auto futureResponse = std::async(
                    std::launch::async, getResponse, applicationEndURLHost,
                               std::to_string(applicationEndURLPort), token, json_body,
                               "/api/GetUserProfile");

                // We can wait responsive or just block here as this is connection phase
                profileJson = futureResponse.get();

                // Store in Redis (Persistent - no TTL)
                g_redisClient.setCompressed(cacheKey, profileJson.dump(), 0);

                profile = ParseUserProfileFromJson(profileJson);
            }

        } catch(const std::exception &e) {
            log("❌ User profile request failed: " + std::string(e.what()),
                LogLevel::ERRORS);
            return UA_STATUSCODE_BADUSERACCESSDENIED;
        }

        if(profile.currentOrgId.empty()) {
            log("❌ No currentOrgId in user profile", LogLevel::ERRORS);
            return UA_STATUSCODE_BADUSERACCESSDENIED;
        }

        log("✓ User profile: " + profile.displayName + " (OrgID: " +
                profile.currentOrgId + ", Org: " + profile.currentOrgCode
            + ")",
            LogLevel::INFO);

        printf("User Profile Loaded: %s | OrgID: %s | OrgCode: %s\n",
               profile.displayName.c_str(), profile.currentOrgId.c_str(),
               profile.currentOrgCode.c_str());

        std::string orgShortCode = profile.currentOrgCode;

        std:
        string NamespaceURI = "Anexee:" + orgShortCode;
        //------------------------------------------------------------------------------------------

        context->onConnected = [ctx = context.get(), server = server_copy,
                                NamespaceURI]() {
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
                //greq.requestedMaxKeepAliveCount = group.maxKeepAliveCount;
                greq.requestedMaxKeepAliveCount = 40;
                greq.requestedPublishingInterval = group.publishingInterval;
                //greq.requestedPublishingInterval = 50;
                //greq.requestedLifetimeCount = group.lifetimeCount;
                greq.requestedLifetimeCount = 120;
                greq.priority = group.priority;
                greq.maxNotificationsPerPublish = group.maxNotificationsPerPublish;
                //greq.maxNotificationsPerPublish = 0;

                UA_CreateSubscriptionResponse gsub =
                    UA_Client_Subscriptions_create(ctx->client.get(), greq, nullptr, nullptr, nullptr);
                ctx->subscriptions[group.name] = gsub;
                log("Subscription Created: " + group.name + " | ID: " + std::to_string(gsub.subscriptionId) + 
                    " | Interval: " + std::to_string(gsub.revisedPublishingInterval) + "ms", LogLevel::INFO);
            }
#endif

            // queue monitored items creation
            for(const auto &group : server.groups) {
                std::string groupName = group.name;
                for(const auto &tag : group.tags) {
                    if(!tag.mappedInfospaceTags) continue;
                    for(const auto &infoSpace : *tag.mappedInfospaceTags) {
                        std::lock_guard<std::mutex> lock(ctx->taskMutex);
                        ctx->taskQueue.push([ctx, infoSpace, groupName, NamespaceURI]() {
                            MyMonitorContext *myContext = new MyMonitorContext{
                                infoSpace, g_mqttHandler, g_sqliteService, nullptr, g_asyncPublisher};

                            // Create a scheduler lambda using the current context
                            TaskScheduler scheduler = [ctx](std::function<void()> task) {
                                std::lock_guard<std::mutex> lock(ctx->taskMutex);
                                ctx->taskQueue.push(task);
                            };

                            MonitorItem(ctx->client.get(), ctx->subscriptions[groupName],
                                        Mapping[infoSpace.tagId].first.c_str(),
                                        infoSpace.tagId, myContext, NamespaceURI.c_str(), scheduler);

                        });
                    }
                }
            }
        };

        // cout<<"Event Monitoring Created"<<endl;

        context->startLoop();
        clientPool[context->endpoint] = context.get();
        clientContexts.push_back(std::move(context));

                // ADD THIS: Give each thread time to initialize before starting the next
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    log("Client pool initialized. Press Ctrl+C to stop...");

    g_mqttHandler->setCallback([&clientPool](const std::string &topic, const std::string &payload) {
        //log("Received MQTT message on topic: " + topic, LogLevel::DEBUG);

        // Parse and validate JSON payload
        json json_payload;
        try {
            json_payload = json::parse(payload);
        } catch (const std::exception& e) {
            log("Invalid JSON payload: " + std::string(e.what()), LogLevel::ERRORS);
            return;
        }

        if (!json_payload.contains("Data") || !json_payload["Data"].is_array() || json_payload["Data"].empty()) {
            log("Invalid payload structure - missing or empty Data array", LogLevel::ERRORS);
            return;
        }

        auto data = json_payload["Data"][0];
        
        // Extract and validate required fields
        if (!data.contains("TagId") || !data["TagId"].is_number_integer() ||
            !data.contains("UpdateType") || !data["UpdateType"].is_number_integer()) {
            log("Missing required fields: TagId or UpdateType", LogLevel::ERRORS);
            return;
        }

        int tagId = data["TagId"].get<int>();
        int updateType = data["UpdateType"].get<int>();
        
        //log("Processing TagId: " + std::to_string(tagId) + ", UpdateType: " + std::to_string(updateType), LogLevel::DEBUG);

        // Find client context for this tag
        if (Mapping.find(tagId) == Mapping.end()) {
            log("TagId " + std::to_string(tagId) + " not found in mapping", LogLevel::ERRORS);
            return;
        }

        std::string endpoint = Mapping[tagId].second;
        auto it = clientPool.find(endpoint);
        if (it == clientPool.end() || !it->second->isConnected) {
            log("Client not connected for endpoint: " + endpoint, LogLevel::ERRORS);
            return;
        }

        auto context = it->second;

        if(updateType == static_cast<int>(UpdateType::Telemetry)) {
            // std::lock_guard<std::mutex> lock(context->taskMutex);
            // context->taskQueue.push([context, tagId]() {
            //     MonitorItem(context->client.get(), context->subscription,
            //                 Mapping[tagId].first.c_str(), tagId);
            // });
        } else if(updateType == static_cast<int>(UpdateType::Control)) {
                // Extract NodeId from mapping
                std::string nodeIdStr = Mapping[tagId].first;
                size_t lastSlash = nodeIdStr.find_last_of('/');
                if (lastSlash != std::string::npos) {
                    nodeIdStr = nodeIdStr.substr(lastSlash + 1); // Extract "ns=1;i=194"
                }

                // Queue write operation
                std::lock_guard<std::mutex> lock(context->taskMutex);
                context->taskQueue.push([context, tagId, data, nodeIdStr]() {
                    log("Executing COMMAND write for TagId: " + std::to_string(tagId), LogLevel::DEBUG);
                    
                    // Parse value from payload
                    double val = 0.0;
                    try {
                        if (data.contains("Value")) {
                            if (data["Value"].is_number()) {
                                val = data["Value"].get<double>();
                            } else if (data["Value"].is_string()) {
                                val = std::stod(data["Value"].get<std::string>());
                            } else {
                                log("Invalid Value type in payload", LogLevel::ERRORS);
                                return;
                            }
                        } else {
                            log("Missing Value field in payload", LogLevel::ERRORS);
                            return;
                        }
                    } catch (const std::exception& e) {
                        log("Failed to parse Value: " + std::string(e.what()), LogLevel::ERRORS);
                        return;
                    }

                    // Create UA_Variant with the value
                    UA_Variant value;
                    UA_Variant_init(&value);
                    UA_Variant_setScalar(&value, &val, &UA_TYPES[UA_TYPES_DOUBLE]);

                    // Parse NodeId string safely
                    int ns = 0, identifier = 0;
                    if (sscanf(nodeIdStr.c_str(), "ns=%d;i=%d", &ns, &identifier) != 2) {
                        log("Failed to parse NodeId: " + nodeIdStr, LogLevel::ERRORS);
                        UA_Variant_clear(&value);
                        return;
                    }

                    // Create NodeId and write to OPC UA server
                    UA_NodeId nid = UA_NODEID_NUMERIC(ns, identifier);
                    log("Writing value " + std::to_string(val) + " to ns=" + std::to_string(ns) + ";i=" + std::to_string(identifier), LogLevel::DEBUG);
                    
                    UA_StatusCode retval = UA_Client_writeValueAttribute(context->client.get(), nid, &value);

                    if (retval == UA_STATUSCODE_GOOD) {
                        log("Value written successfully to OPC UA server", LogLevel::INFO);
                    } else {
                        log("Failed to write value: " + std::string(UA_StatusCode_name(retval)), LogLevel::ERRORS);
                    }
                });
            }
         else if(updateType == static_cast<int>(UpdateType::BulkData)) {
            log("BULKDATA update received", LogLevel::DEBUG);
        } else {
            log("Unknown UpdateType: " + std::to_string(updateType), LogLevel::ERRORS);
        }
    });

    while(g_running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    log("Cleaning up...");

    for(auto &context : clientContexts) {
        //UA_Client_disconnect(context->client.get());
        context->stopLoop();
    }

    clientPool.clear();
    clientContexts.clear();

    // Clean up MQTT handler at the end
    if(g_sqliteService) {
        g_sqliteService->DisposeDB();
        delete g_sqliteService;
    }
    if (g_mqttHandler) {
       delete g_mqttHandler;
    }

    #ifdef _WIN32
    if(hMutex) {
        ReleaseMutex(hMutex);
        CloseHandle(hMutex);
    }
#else
    if(lockFd >= 0) {
        flock(lockFd, LOCK_UN);
        close(lockFd);
    }
#endif

    return EXIT_SUCCESS;
}

int
main(int argc, char *argv[]) {
    return runClient(false, argc, argv);
}

#ifdef _WIN32
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
#endif

// Service helper implementations
// Service helper implementations (Windows only)
#ifdef _WIN32
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
#else
static void
SetWorkingDirectoryToExe() {
    char exePath[PATH_MAX] = {0};
    ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
    if(len <= 0) return;
    exePath[len] = '\0';
    // Strip filename to directory
    for(ssize_t i = len - 1; i >= 0; --i) {
        if(exePath[i] == '/') {
            exePath[i] = '\0';
            break;
        }
    }
    chdir(exePath);
}
#endif

// The following service functions are Windows-only and must be guarded
#ifdef _WIN32
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
#endif // _WIN32