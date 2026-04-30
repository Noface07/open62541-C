/* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
 * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

 #include <open62541/client_config_default.h>
 #include <open62541/client_highlevel.h>
 #include <open62541/client_subscriptions.h>
 #include <open62541/plugin/log_stdout.h>
 
#include <algorithm>
#include <atomic>
#include <cctype>
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
     #include <dirent.h>  // For opendir, readdir, closedir, DIR, dirent
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
#include "EdgeConfigLoader.h"
#include "TelemetrySpill.h"

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

struct ClientCliOptions {
    bool debug = false;
    std::string instanceId;
};

static std::string
sanitizeInstanceId(const std::string &raw) {
    std::string out;
    out.reserve(raw.size());
    for(unsigned char ch : raw) {
        if(std::isalnum(ch) || ch == '_' || ch == '-') {
            out.push_back(static_cast<char>(ch));
        } else {
            out.push_back('_');
        }
    }

    // Keep names bounded to avoid overly long mutex/file names.
    constexpr size_t MAX_LEN = 48;
    if(out.size() > MAX_LEN)
        out.resize(MAX_LEN);

    return out;
}

static ClientCliOptions
parseClientCliOptions(int argc, char *argv[]) {
    ClientCliOptions opts;
    std::string rawInstanceId;

    for(int i = 1; i < argc; ++i) {
        std::string arg = argv[i] ? argv[i] : "";
        if(arg == "--debug") {
            opts.debug = true;
            continue;
        }

        if(arg == "--instance-id") {
            if(i + 1 < argc && argv[i + 1]) {
                rawInstanceId = argv[++i];
            } else {
                std::cerr << "WARNING: --instance-id provided without a value."
                          << std::endl;
            }
            continue;
        }

        const std::string prefix = "--instance-id=";
        if(arg.rfind(prefix, 0) == 0) {
            rawInstanceId = arg.substr(prefix.size());
        }
    }

    opts.instanceId = sanitizeInstanceId(rawInstanceId);
    return opts;
}

static std::string
appendInstanceSuffixToPath(const std::string &path, const std::string &instanceId) {
    if(instanceId.empty())
        return path;

    size_t lastSlash = path.find_last_of("/\\");
    size_t lastDot = path.find_last_of('.');
    if(lastDot == std::string::npos ||
       (lastSlash != std::string::npos && lastDot < lastSlash)) {
        return path + "_" + instanceId;
    }

    return path.substr(0, lastDot) + "_" + instanceId + path.substr(lastDot);
}

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
    std::atomic<bool> isConnected;

    // Thread 1: OPC UA network loop (run_iterate + OPC writes)
    std::thread opcThread;
    // Thread pool: general workers (non-OPC tasks: MQTT, DB, JSON parsing, etc.)
    static constexpr int WORKER_COUNT = 32;
    std::vector<std::thread> workers;

    // Queue for non-OPC tasks — consumed by workerThread
    std::mutex taskMutex;
    std::condition_variable taskCv;
    std::queue<std::function<void()>> taskQueue;

    // Queue for OPC UA operations — consumed ONLY by opcThread (no UA_Client sharing)
    std::mutex opcMutex;
    std::queue<std::function<void()>> opcQueue;

    std::function<UA_StatusCode()> connectOnce;
    std::function<void()> onConnected;
    static constexpr size_t MAX_OPC_TASKS_PER_TICK = 256;

    ClientContext() : running(true), isConnected(false) {}

    void
    startLoop() {
        // ── Thread 1: OPC UA network loop ────────────────────────────────────
        // Exclusively owns UA_Client*. Calls run_iterate every ~10 ms and
        // drains opcQueue (OPC UA writes posted from the worker thread).
        opcThread = std::thread([this]() {
            log("OPC thread started for " + name, LogLevel::DEBUG);

            while(running) {
                // ── reconnect logic ──────────────────────────────────────────
                if(!isConnected.load(std::memory_order_acquire)) {
                    if(connectOnce) {
                        UA_StatusCode rc = connectOnce();
                        if(rc == UA_STATUSCODE_GOOD) {
                            isConnected.store(true, std::memory_order_release);
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

                // ── null-client guard ────────────────────────────────────────
                if(!client) {
                    log(name + ": Client is null, reconnecting...", LogLevel::ERRORS);
                    isConnected.store(false, std::memory_order_release);
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    continue;
                }

                // ── opcQueue backlog diagnostic ──────────────────────────────
                {
                    std::lock_guard<std::mutex> lock(opcMutex);
                    if(opcQueue.size() > 1000) {
                        log(name + ": opcQueue backlog: " +
                                std::to_string(opcQueue.size()),
                            LogLevel::INFO);
                    }
                }

                // ── drain ALL pending OPC tasks per iteration ────────────────
                // MonitorItem setup tasks are fast (microsecond SDK calls) and
                // must all complete quickly. Control writes are rare MQTT events.
                // All UA_Client_* access is safe here: exclusively on opcThread.
                for(size_t i = 0; i < MAX_OPC_TASKS_PER_TICK; ++i) {
                    std::function<void()> opcTask;
                    {
                        std::lock_guard<std::mutex> lock(opcMutex);
                        if(opcQueue.empty())
                            break;
                        opcTask = std::move(opcQueue.front());
                        opcQueue.pop();
                    }
                    try {
                        opcTask();
                    } catch(...) {
                        log(name + ": opcTask threw exception", LogLevel::ERRORS);
                    }
                }

                // ── OPC UA network tick ──────────────────────────────────────
                UA_StatusCode code = UA_Client_run_iterate(client.get(), 10);
                if(code != UA_STATUSCODE_GOOD) {
                    log(name + ": UA_Client_run_iterate failed: " +
                            std::string(UA_StatusCode_name(code)),
                        LogLevel::ERRORS);
                    UA_Client_disconnect(client.get());
                    client.reset();
                    isConnected.store(false, std::memory_order_release);
                    subscriptions.clear();
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    continue;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            log("OPC thread exiting for " + name);
        });

        // ── Thread pool: WORKER_COUNT general workers ─────────────────────────
        // Drain taskQueue in parallel. Tasks must NOT call UA_Client_* functions.
        // If a task needs an OPC UA operation, push a lambda via postOpcTask().
        workers.reserve(WORKER_COUNT);
        for(int i = 0; i < WORKER_COUNT; ++i) {
            workers.emplace_back([this, i]() {
                log("Worker[" + std::to_string(i) + "] started for " + name,
                    LogLevel::DEBUG);

                while(running) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(taskMutex);
                        taskCv.wait(lock,
                                    [this]{ return !taskQueue.empty() || !running; });

                        if(!running && taskQueue.empty()) break;

                        if(!taskQueue.empty()) {
                            task = std::move(taskQueue.front());
                            taskQueue.pop();
                        }
                    }

                    if(task) {
                        try {
                            auto start = std::chrono::steady_clock::now();
                            task();
                            auto ms =
                                std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - start)
                                    .count();
                            if(ms > 200) {
                                log(name + ": Worker[" + std::to_string(i) +
                                        "] SLOW TASK " + std::to_string(ms) + " ms",
                                    LogLevel::INFO);
                            }
                        } catch(...) {
                            log(name + ": Worker[" + std::to_string(i) +
                                    "] task threw exception",
                                LogLevel::ERRORS);
                        }
                    }
                }
                log("Worker[" + std::to_string(i) + "] exiting for " + name,
                    LogLevel::DEBUG);
            });
        }
    }

    // Helper: push an OPC UA write/read operation — called from workerThread tasks.
    // The lambda will execute on the OPC thread where UA_Client* is safe to use.
    void postOpcTask(std::function<void()> fn) {
        std::lock_guard<std::mutex> lock(opcMutex);
        opcQueue.push(std::move(fn));
    }

    // Helper: push a general (non-OPC) task and wake the worker thread.
    void postTask(std::function<void()> fn) {
        {
            std::lock_guard<std::mutex> lock(taskMutex);
            taskQueue.push(std::move(fn));
        }
        taskCv.notify_one();
    }

    void stopLoop() {
        running = false;
        taskCv.notify_all(); // wake all worker threads
        if(opcThread.joinable())
            opcThread.join();
        for(auto &w : workers)
            if(w.joinable()) w.join();
        workers.clear();
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
        const int maxRetries = 3; // Max retries before giving up (for offline fallback)

        while (!tokenSuccess && attempt <= maxRetries) {
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
                std::cout << "Token fetch failed (attempt " << attempt << "/" << maxRetries << "): " << e.what() << std::endl;
            }

            if (!tokenSuccess) {
                std::this_thread::sleep_for(std::chrono::seconds(retryDelay));
                attempt++;   // ✅ now valid, because attempt is captured by reference
            }
        }

        if (!tokenSuccess) {
            std::cout << "WARNING: All " << maxRetries << " token attempts failed. Proceeding without token (DB fallback mode)." << std::endl;
        }

        return token;  // Returns empty string if all retries failed
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
    ClientCliOptions cliOptions = parseClientCliOptions(argc, argv);

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

        // Extract MqttConfig (broker address/port/TLS still from appsettings.json)
        bool protocol = config["MqttConfig"]["MqttSettings"][0]["UseTLS"].get<bool>();
        std::string brokerAddress = config["MqttConfig"]["MqttSettings"][0]["BrokerAddress"].get<std::string>();
        int brokerPort = config["MqttConfig"]["MqttSettings"][0]["BrokerPort"].get<int>();
        // MQTT username/password/clientId come from EdgeConfig (set after bearer token below)
        std::string mqttUsername;
        std::string mqttPassword;

        // Load credentials, NodeID, and ClientId from EdgeConfig_*.txt
        std::cerr << "[EdgeConfig] Attempting to load EdgeConfig_*.txt ..." << std::endl;
        EdgeConfigData edgeCfg;
        try {
            edgeCfg = LoadEdgeConfig();
        } catch (const std::exception& ex) {
            std::cerr << "\n[EdgeConfig] FATAL ERROR: " << ex.what() << std::endl;
            std::cerr << "[EdgeConfig] Client cannot start without a valid EdgeConfig file." << std::endl;
            std::cout << "STARTUP FAILED: EdgeConfig error — " << ex.what() << std::endl;
            return EXIT_FAILURE;
        }
        std::string authUsername = edgeCfg.username;
        std::string authPassword = edgeCfg.password;

        //Extract NodeID from EdgeConfig
        std::string NodeID = edgeCfg.shortCode;
    
        // Extract Payload
        std::string dbPath = config["Payload"]["OfflineQueueOptions"]["DbPath"].get<std::string>();
        int retentionDays = config["Payload"]["OfflineQueueOptions"]["RetentionDays"].get<int>();
        int retryBatchSize = config["Payload"]["OfflineQueueOptions"]["RetryBatchSize"].get<int>();
        if(!cliOptions.instanceId.empty()) {
            dbPath = appendInstanceSuffixToPath(dbPath, cliOptions.instanceId);
        }

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
    std::string mutexName = "Global\\AnexeeMutex";
    if(!cliOptions.instanceId.empty()) {
        mutexName += "_" + cliOptions.instanceId;
    }
    HANDLE hMutex = CreateMutexA(NULL, TRUE, mutexName.c_str());

    if(hMutex == NULL || GetLastError() == ERROR_ALREADY_EXISTS) {
        if(!isService) {
            MessageBoxA(NULL, "Another instance is already running.", "Error",
                        MB_OK | MB_ICONERROR);
        }
        return 1;  // Exit immediately
    }
    #else
    std::string lockFilePath = "/tmp/anexee-opcua-client";
    if(!cliOptions.instanceId.empty()) {
        lockFilePath += "-" + cliOptions.instanceId;
    }
    lockFilePath += ".lock";
    int lockFd = open(lockFilePath.c_str(), O_CREAT | O_RDWR, 0666);
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

    g_debug = cliOptions.debug;
    g_logging_enabled = cliOptions.debug; // Enable file logging in debug mode

    // Initialize logging with client-specific folder (only if logging is enabled)
    std::string logFolder = "logs/client";
    std::string logAppName = "client";
    if(!cliOptions.instanceId.empty()) {
        logFolder = "logs/client_" + cliOptions.instanceId;
        logAppName = "client_" + cliOptions.instanceId;
    }
    init_logging(logFolder, logAppName, true);
    if (g_logging_enabled) {
        log("Client logging initialized with day-wise log files", LogLevel::INFO);
    } else {
        std::cout << "Client started - no file logging (use --debug to enable)" << std::endl;
    }
    if(!cliOptions.instanceId.empty()) {
        log("Instance mode enabled: " + cliOptions.instanceId, LogLevel::INFO);
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
    if (BearerToken.empty()) {
        log("WARNING: No bearer token available. Running in OFFLINE mode (DB fallback only).", LogLevel::WARNING);
    } else {
        log("Bearer token obtained successfully", LogLevel::INFO);
    }

    // Derive MQTT credentials from EdgeConfig now that bearer token is known
    SetEdgeConfigMqttPassword(edgeCfg, BearerToken);
    if(!cliOptions.instanceId.empty()) {
        if(edgeCfg.clientId.empty()) {
            edgeCfg.clientId = "AnexeeClient";
        }
        edgeCfg.clientId += "_" + cliOptions.instanceId;
    }
    mqttUsername = edgeCfg.mqttUsername;
    mqttPassword = edgeCfg.mqttPassword;
    log("MQTT credentials set from EdgeConfig (ClientId=" + edgeCfg.clientId + ")", LogLevel::INFO);
    
    vector<ServerInfoO> serverList;

    // Initialize SqliteQueueService EARLY so we can use config cache during hierarchy fetch
    try {
        OfflineQueueOptions options;
        options.batchSize = 100;
        options.uploadIntervalSeconds = 15;
        g_sqliteService = new SqliteQueueService(dbPath, options);
        log("SqliteQueueService initialized (early - for config cache).", LogLevel::INFO);
    } catch (const std::exception& e) {
        log("WARNING: Failed to initialize SqliteQueueService early: " + std::string(e.what()) + ". DB fallback will be unavailable.", LogLevel::WARNING);
    }
        
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

            // 2. Fallback to Database if Redis miss/fail
            if (!cacheHit && g_sqliteService) {
                log("Checking local database for key: " + redisKey, LogLevel::INFO);
                try {
                    std::string dbData = g_sqliteService->GetConfig(redisKey);
                    if (!dbData.empty()) {
                        log("DB Hit! Loading hierarchy for " + NodeID + " from local database.", LogLevel::INFO);
                        json dbJson = json::parse(dbData);
                        serverList = ParseServerHierarchyFromJson(dbJson);
                        if (!serverList.empty()) {
                            hierarchySuccess = true;
                            cacheHit = true;
                            log("Hierarchy loaded from local database successfully.", LogLevel::INFO);
                        } else {
                            log("Database data invalid or empty. Falling back to API.", LogLevel::WARNING);
                        }
                    } else {
                        log("DB Miss for " + redisKey + ". Falling back to API...", LogLevel::INFO);
                    }
                } catch (const std::exception& e) {
                    log("Error reading from database: " + std::string(e.what()) + ". Falling back to API.", LogLevel::ERRORS);
                }
            }

            // 3. Fallback to API if both Redis and DB miss
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

                // Cache the fresh response to Redis
                if (!apiResponse.is_null()) {
                     g_redisClient.setCompressed(redisKey, apiResponse.dump(), 0);
                     log("Cached fresh hierarchy to Redis key: " + redisKey, LogLevel::INFO);

                     // Also cache to local database
                     if (g_sqliteService) {
                         try {
                             g_sqliteService->SetConfig(redisKey, apiResponse.dump());
                             log("Cached fresh hierarchy to local database key: " + redisKey, LogLevel::INFO);
                         } catch (const std::exception& e) {
                             log("Failed to cache to database: " + std::string(e.what()), LogLevel::WARNING);
                         }
                     }
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

    // Configure SqliteQueueService (service was already created before hierarchy loop)
    try {
        g_sqliteService->StartQueueWorker(); // Start the DB writer thread
        log("SqliteQueueService queue worker started.", LogLevel::INFO);

        // After: g_sqliteService = new SqliteQueueService("OfflineData.db", options);
        g_sqliteService->SetApiUrl("http://164.52.221.177:5128/api/UploadBulkTagData");   // required
        g_sqliteService->SetApiAuth(BearerToken); // Reuse existing token (empty when offline, refreshed via callback when online)  

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
        g_asyncPublisher = std::make_shared<AsyncPublisher>(
            g_mqttHandler,
            [&dpToOrg](const std::string &topic, const std::string &wrapper) -> bool {
                if(!g_sqliteService)
                    return false;
                return TryEnqueueTelemetryWrapper(topic, wrapper, g_sqliteService,
                                                  dpToOrg);
            });
        log("AsyncPublisher initialized (MQTT-down spill to SQLite enabled).",
            LogLevel::INFO);
        
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
                        if (g_mqttHandler) {
                            (void)g_mqttHandler->publish(topic, payload);
                        } else {
                            log("CLIENT CALLBACK: g_mqttHandler is null during replay publish.", LogLevel::ERRORS);
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
            
            for(const auto &msg : failedMessages) {
                try {
                    if(msg.payload.empty()) {
                        log("Empty payload received on topic: " + msg.topic,
                            LogLevel::INFO);
                        continue;
                    }
                    if(TryEnqueueTelemetryWrapper(msg.topic, msg.payload, g_sqliteService,
                                                  dpToOrg)) {
                        successCount++;
                        log("Queued failed message to database: " + msg.topic,
                            LogLevel::DEBUG);
                    } else {
                        log("Failed message has invalid JSON structure: " + msg.payload,
                            LogLevel::ERRORS);
                        errorCount++;
                    }
                } catch(const std::exception &e) {
                    log("Failed to parse failed message payload: " +
                            std::string(e.what()) + " | Payload: " + msg.payload,
                        LogLevel::ERRORS);
                    errorCount++;
                }
            }
            
            log("Failed message processing complete: " + std::to_string(successCount) + " saved, " + std::to_string(errorCount) + " errors", LogLevel::INFO);
        };

        g_mqttHandler->setOnConnectCallback(onMqttConnect);
        g_mqttHandler->setOnDisconnectCallback(onMqttDisconnect);
        g_mqttHandler->setOnFailedMessageCallback(onFailedMessages);

        // Register token refresh callback: called before every MQTT reconnect attempt.
        // Re-fetches the bearer token so an expired JWT never causes permanent not_authorized.
        g_mqttHandler->setPasswordRefreshCallback(
            [applicationEndURLHost, applicationEndURLPort, &edgeCfg]() -> std::string {
                try {
                    log("MQTT token refresh: fetching new bearer token ...", LogLevel::INFO);
                    std::string freshToken = getBearerTokenNow(
                        applicationEndURLHost,
                        std::to_string(applicationEndURLPort),
                        edgeCfg.username,
                        edgeCfg.password,
                        true).get();
                    if(freshToken.empty()) {
                        log("MQTT token refresh: empty token returned.", LogLevel::WARNING);
                        return "";
                    }
                    // Wrap in JSON exactly as the broker expects
                    nlohmann::json pw;
                    pw["token"] = freshToken;
                    log("MQTT token refresh: new token obtained.", LogLevel::INFO);
                    return pw.dump();
                } catch(const std::exception& e) {
                    log("MQTT token refresh failed: " + std::string(e.what()), LogLevel::ERRORS);
                    return "";
                }
            });

        // ... existing code ...




    } catch (const std::exception& e) {
        log("FATAL: Failed to initialize MQTT handler: " + std::string(e.what()), LogLevel::ERRORS);
        return EXIT_FAILURE;
    }

    // Connect to MQTT broker (non-blocking)
    //log("Attempting to connect to MQTT broker...", LogLevel::INFO);
    //if(protocol) {
    //    log("Using MQTT over TLS", LogLevel::INFO);
    //} else {
    //    log("Using MQTT over TCP", LogLevel::INFO);
    //}
    if(!g_mqttHandler->connect(brokerAddress, std::to_string(brokerPort), mqttUsername, mqttPassword, protocol, edgeCfg.clientId)) {
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

        // With many subscriptions, default 10 outstanding PublishRequests can starve
        // keep-alives and trigger widespread "Inactivity for Subscription" events.
        const size_t expectedSubCount = server.groups.size() + 1; // + event subscription
        config->outStandingPublishRequests =
            static_cast<UA_UInt16>(std::clamp<size_t>(expectedSubCount, 20, 256));
        if(config->timeout < 30000)
            config->timeout = 30000; // widen inactivity threshold under heavy load
    
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
                // Try DB fallback before API
                if(g_sqliteService) {
                    try {
                        std::string dbData = g_sqliteService->GetConfig(cacheKey);
                        if(!dbData.empty()) {
                            log("💾 DB Hit for UserProfile: " + cacheKey, LogLevel::INFO);
                            profileJson = nlohmann::ordered_json::parse(dbData);
                            profile = ParseUserProfileFromJson(profileJson);
                            if(!profile.currentOrgCode.empty() && !profile.currentOrgId.empty()) {
                                cacheHit = true;
                            }
                        }
                    } catch(const std::exception& e) {
                        log("⚠️ DB fallback error for UserProfile: " + std::string(e.what()), LogLevel::WARNING);
                    }
                }
            }

            if(!cacheHit) {
                if(BearerToken.empty()) {
                    log("⚠️ No bearer token and no cached UserProfile. Skipping API call.", LogLevel::WARNING);
                } else {
                    auto futureResponse = std::async(
                        std::launch::async, getResponse, applicationEndURLHost,
                                   std::to_string(applicationEndURLPort), BearerToken, json_body,
                                   "/api/GetUserProfile");

                    profileJson = futureResponse.get();

                    // Store in Redis (Persistent - no TTL)
                    g_redisClient.setCompressed(cacheKey, profileJson.dump(), 0);
                    // Also store in DB
                    if(g_sqliteService) {
                        try {
                            g_sqliteService->SetConfig(cacheKey, profileJson.dump());
                        } catch(const std::exception& e) {
                            log("⚠️ Failed to cache UserProfile to DB: " + std::string(e.what()), LogLevel::WARNING);
                        }
                    }

                    profile = ParseUserProfileFromJson(profileJson);
                }
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

        std::string NamespaceURI = "Anexee:" + orgShortCode;
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
                UA_UInt32 requestedKeepAlive =
                    (group.maxKeepAliveCount > 0) ? (UA_UInt32)group.maxKeepAliveCount : 40;
                greq.requestedPublishingInterval = group.publishingInterval;
                UA_UInt32 requestedLifetime =
                    (group.lifetimeCount > 0) ? (UA_UInt32)group.lifetimeCount
                                              : (requestedKeepAlive * 3);
                if(requestedLifetime < (requestedKeepAlive * 3))
                    requestedLifetime = requestedKeepAlive * 3;
                greq.requestedMaxKeepAliveCount = requestedKeepAlive;
                greq.requestedLifetimeCount = requestedLifetime;
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

            // ── Batch MonitoredItems registration: ONE call per group ──────────
            // Uses UA_Client_MonitoredItems_createDataChanges (plural) which sends
            // ALL items for a group in a single OPC UA service request.
            for(const auto &group : server.groups) {
                std::string groupName = group.name;

                // Pre-compile expressions and parse NodeIds BEFORE hopping to the OPC thread!
                // This shifts heavy CPU parsing work to the current calling thread (pool worker).
                std::vector<MappedInfospaceTag> batchInfoSpaces;
                std::vector<UA_NodeId>          batchNodeIds;
                std::vector<MyMonitorContext *> batchContexts;

                // ── Resolve namespace index once per batch ───────────────
                UA_UInt16 resolvedNs = 0;
                bool      nsResolved = false;
                if(!NamespaceURI.empty()) {
                    UA_UInt16 tmp;
                    if(UA_Client_getNamespaceIndex(
                           ctx->client.get(),
                           UA_STRING((char *)NamespaceURI.c_str()),
                           &tmp) == UA_STATUSCODE_GOOD) {
                        resolvedNs = tmp;
                        nsResolved = true;
                    }
                }

                for(const auto &tag : group.tags) {
                    if(!tag.mappedInfospaceTags) continue;
                    for(const auto &infoSpace : *tag.mappedInfospaceTags) {
                        auto it = Mapping.find(infoSpace.tagId);
                        if(it == Mapping.end()) continue;

                        const std::string &nodeIdStr = it->second.first;
                        
                        // 1. Parse NodeId
                        UA_NodeId nodeId = UA_NODEID_NULL;
                        const char *nsStart = strstr(nodeIdStr.c_str(), "ns=");
                        if(nsStart) {
                            const char *nsEnd = strchr(nsStart + 3, ';');
                            if(nsEnd) {
                                UA_UInt16 nsIdx = nsResolved
                                    ? resolvedNs
                                    : (UA_UInt16)strtoul(nsStart + 3, nullptr, 10);
                                const char *idStart = nsEnd + 1;
                                if(strncmp(idStart, "i=", 2) == 0)
                                    nodeId = UA_NODEID_NUMERIC(
                                        nsIdx,
                                        (UA_UInt32)strtoul(idStart + 2, nullptr, 10));
                                else if(strncmp(idStart, "s=", 2) == 0)
                                    nodeId = UA_NODEID_STRING_ALLOC(nsIdx, idStart + 2);
                            }
                        }

                        // 2. Build MyMonitorContext and pre-compile ExprTk AST
                        auto *myCtx = new MyMonitorContext{
                            infoSpace, g_mqttHandler, g_sqliteService,
                            nullptr, g_asyncPublisher,
                            [ctx](std::function<void()> task) {
                                ctx->postTask(std::move(task));
                            }
                        };

                        if(infoSpace.enableExpression && !infoSpace.expression.empty()) {
                            myCtx->exprContext = std::make_shared<ExprTkContext>();
                            myCtx->exprContext->symbol_table.add_variable(
                                "value", myCtx->exprContext->value);
                            myCtx->exprContext->symbol_table.add_constants();
                            myCtx->exprContext->expression.register_symbol_table(
                                myCtx->exprContext->symbol_table);
                            exprtk::parser<double> parser;
                            std::string exprStr = infoSpace.expression;
                            std::transform(exprStr.begin(), exprStr.end(),
                                           exprStr.begin(), ::tolower);
                            if(!parser.compile(exprStr, myCtx->exprContext->expression))
                                myCtx->exprContext.reset();
                        }

                        batchInfoSpaces.push_back(infoSpace);
                        batchNodeIds.push_back(nodeId);
                        batchContexts.push_back(myCtx);
                    }
                }

                if(batchInfoSpaces.empty()) continue;

                // Move heavy pre-computed vectors into the OPC thread closure
                ctx->postOpcTask([ctx, batchInfoSpaces, batchNodeIds, batchContexts,
                                  groupName, NamespaceURI]() mutable {
                    const UA_CreateSubscriptionResponse &sub =
                        ctx->subscriptions[groupName];
                    size_t count = batchInfoSpaces.size();

                    // ── Build UA_CreateMonitoredItemsRequest ─────────────────
                    UA_CreateMonitoredItemsRequest batchReq;
                    UA_CreateMonitoredItemsRequest_init(&batchReq);
                    batchReq.subscriptionId     = sub.subscriptionId;
                    batchReq.timestampsToReturn = UA_TIMESTAMPSTORETURN_BOTH;
                    batchReq.itemsToCreate =
                        (UA_MonitoredItemCreateRequest *)UA_Array_new(
                            count,
                            &UA_TYPES[UA_TYPES_MONITOREDITEMCREATEREQUEST]);
                    batchReq.itemsToCreateSize = count;

                    // ── Parallel arrays required by the API ──────────────────
                    std::vector<UA_Client_DataChangeNotificationCallback> cbs(
                        count, handler_NodeValueChanged);
                    std::vector<UA_Client_DeleteMonitoredItemCallback>  delCbs(count, nullptr);

                    for(size_t i = 0; i < count; ++i) {
                        const MappedInfospaceTag &infoSpace  = batchInfoSpaces[i];

                        // ── Fill request item ────────────────────────────────
                        UA_MonitoredItemCreateRequest &req = batchReq.itemsToCreate[i];
                        UA_MonitoredItemCreateRequest_init(&req);
                        
                        // IMPORTANT: We must deep copy the NodeId because UA_CreateMonitoredItemsRequest_clear
                        // will attempt to free its members later. 
                        UA_NodeId_copy(&batchNodeIds[i], &req.itemToMonitor.nodeId);
                        UA_NodeId_clear(&batchNodeIds[i]);
                        
                        req.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
                        req.monitoringMode            = UA_MONITORINGMODE_REPORTING;
                        req.requestedParameters.samplingInterval = 0;
                        req.requestedParameters.queueSize        = 10000;
                        req.requestedParameters.discardOldest    = false;

                        // Deadband filter (heap-allocated, owned by req)
                        UA_DataChangeFilter *filter =
                            (UA_DataChangeFilter *)UA_malloc(
                                sizeof(UA_DataChangeFilter));
                        UA_DataChangeFilter_init(filter);
                        if(infoSpace.deadband > 0) {
                            filter->deadbandType  = UA_DEADBANDTYPE_ABSOLUTE;
                            filter->deadbandValue = infoSpace.deadband;
                        }
                        filter->trigger = UA_DATACHANGETRIGGER_STATUSVALUE;
                        UA_ExtensionObject &fobj = req.requestedParameters.filter;
                        memset(&fobj, 0, sizeof(fobj));
                        fobj.encoding             = UA_EXTENSIONOBJECT_DECODED;
                        fobj.content.decoded.type = &UA_TYPES[UA_TYPES_DATACHANGEFILTER];
                        fobj.content.decoded.data = filter;
                    }

                    // ── Single network service call for the whole group ───────
                    UA_CreateMonitoredItemsResponse resp =
                        UA_Client_MonitoredItems_createDataChanges(
                            ctx->client.get(),
                            batchReq,
                            (void **)batchContexts.data(),
                            cbs.data(),
                            delCbs.data());

                    // ── Deallocate the request (frees filter heap blocks too) ─
                    UA_CreateMonitoredItemsRequest_clear(&batchReq);

                    // ── Process results ──────────────────────────────────────
                    size_t ok = 0;
                    for(size_t i = 0; i < resp.resultsSize; ++i) {
                        if(resp.results[i].statusCode != UA_STATUSCODE_GOOD) {
                            log("MonitoredItem failed for TagId " +
                                    std::to_string(batchInfoSpaces[i].tagId) +
                                    ": " +
                                    UA_StatusCode_name(resp.results[i].statusCode),
                                LogLevel::ERRORS);
                            delete batchContexts[i];
                            batchContexts[i] = nullptr;
                        } else {
                            ++ok;
                        }
                    }
                    UA_CreateMonitoredItemsResponse_clear(&resp);

                    log("Group " + groupName + ": registered " +
                            std::to_string(ok) + "/" + std::to_string(count) +
                            " monitored items",
                        LogLevel::INFO);
                });
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
        if (payload.empty()) {
            log("Empty payload received on topic: " + topic, LogLevel::INFO);
            return;
        }

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
        if (it == clientPool.end() ||
            !it->second->isConnected.load(std::memory_order_acquire)) {
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

                // UA_Client_writeValueAttribute runs on the OPC thread via postOpcTask
                context->postOpcTask([context, tagId, data, nodeIdStr]() {
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
