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

// ─── Hot reload routing ──────────────────────────────────────────────────────
// Maps HotReloadCommand::purpose (= ServerInfoO::typeId) to the list of
// endpoint URLs that share that typeId, so a single MQTT command can fan out
// to every connected ClientContext serving the same purpose.
static std::mutex                                            g_purposeMutex;
static std::unordered_map<std::string, std::vector<std::string>> g_purposeToEndpoints;

// ─── Hot reload coordinator ──────────────────────────────────────────────────
// Single-worker queue that serializes API calls + dispatch off the MQTT thread
// and outside any OPC UA thread. Workers may post UA mutations via
// ClientContext::postOpcTask.
static std::mutex                          g_hrMutex;
static std::queue<std::function<void()>>   g_hrQueue;
static std::condition_variable             g_hrCv;
static std::thread                         g_hrWorker;
static std::atomic<bool>                   g_hrRunning{false};

// Protects clientPool (and the parallel clientContexts vector) against
// concurrent reads from the MQTT callback (control writes look up the target
// context by endpoint) and writes from the hot reload worker (Add/Delete on
// purpose=OPC_HI_SERVER mutate the pool at runtime).
static std::mutex                          g_clientPoolMutex;

// Serializes reads/writes of runClient's `BearerToken` between the hot reload
// worker, MQTT password refresh, and any path that re-authenticates after HTTP
// 401.
static std::mutex                          g_bearerTokenMutex;

// Forward declaration: a configurable factory that produces a fully-
// configured-and-started ClientContext for a given ServerInfoO. Initialized
// in runClient() once all per-process state (BearerToken, EdgeConfig,
// applicationEndURL, etc.) is loaded so that applyAddServer can reuse the
// exact same setup pipeline used at startup.
struct ClientContext; // defined below
static std::function<std::unique_ptr<ClientContext>(const ServerInfoO &)>
    g_buildContext;

static void
postHotReloadTask(std::function<void()> fn) {
    {
        std::lock_guard<std::mutex> lk(g_hrMutex);
        g_hrQueue.push(std::move(fn));
    }
    g_hrCv.notify_one();
}


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
    std::string typeId;       // ServerInfoO::typeId (e.g. "OPC_HI_SERVER")
    std::string namespaceURI; // OPC UA Namespace URI for hot reload node lookup
    // Hierarchy id of this server (= ServerInfoO::dataPointId at startup, or
    // the id field on a hot reload OPC_HI_SERVER add). Used by the hot reload
    // dispatcher to route Update/Delete commands to one specific server when
    // the client owns multiple connections.
    int serverHierarchyId = 0;
    std::unique_ptr<UA_Client, UA_Client_Deleter> client;
    std::map<std::string, UA_CreateSubscriptionResponse> subscriptions;
    std::atomic<bool> running;
    std::atomic<bool> isConnected;

    // Resolved namespace index for `namespaceURI` (set by onConnected).
    // Hot reload's applyAdd uses this to construct full OPC UA node paths
    // like "ns=<resolvedNs>;s=<nodeId>" from the API's short nodeId field.
    std::atomic<std::uint16_t> resolvedNs{0};

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

    // Live monitored item registry indexed by hierarchyId. Populated when
    // monitored items are created at startup or by applyAdd; consumed by
    // applyDelete / applyUpdate / applyReload to undo previously created items.
    std::mutex registryMutex;
    std::unordered_map<int, std::vector<LiveMonitorEntry>> liveRegistry;

    // Maps a group's hierarchy id to its `subscriptions` key (group.name) so
    // hot reload's applyGroupUpdate can look up the OPC UA subscriptionId
    // from the MQTT command's `id`.
    std::unordered_map<int, std::string> groupIdToName;

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

                // A queued task may have flipped isConnected to false (e.g.
                // applyReload's teardown). Re-check before run_iterate so we
                // jump straight to the reconnect path instead of failing on
                // a torn-down channel and burning the 5-second sleep below.
                if(!isConnected.load(std::memory_order_acquire))
                    continue;

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


// ─────────────────────────────────────────────────────────────────────────────
// Hot reload dispatchers
//
// Each function applies one MQTT-triggered action to a single ClientContext.
// All actual UA_Client_* mutations are dispatched through ctx->postOpcTask so
// they run on the OPC thread that owns the underlying UA_Client.
// ─────────────────────────────────────────────────────────────────────────────

// Build "ns=<idx>;s=<id>" for a string node id, "ns=<idx>;i=<id>" for a
// purely numeric id. The hot reload API only ships the short identifier.
static std::string
buildNodePath(std::uint16_t nsIdx, const std::string &nodeId) {
    if(nodeId.empty()) return {};

    bool numeric = !nodeId.empty();
    for(char c : nodeId) {
        if(!std::isdigit(static_cast<unsigned char>(c))) {
            numeric = false;
            break;
        }
    }
    return "ns=" + std::to_string(nsIdx) + (numeric ? ";i=" : ";s=") + nodeId;
}

static void
applyAdd(ClientContext *ctx, const HotReloadCommand &cmd,
         const std::vector<HotReloadItem> &items) {
    if(!ctx) return;
    if(items.empty()) {
        log("HotReload Add: no items to add for hierarchy id " +
                std::to_string(cmd.firstId()),
            LogLevel::WARNING);
        return;
    }

    const std::uint16_t nsIdx =
        ctx->resolvedNs.load(std::memory_order_acquire);

    // Pick a target subscription. The hot reload payload doesn't carry a
    // group, so use the first available group subscription.
    UA_UInt32 targetSubId = 0;
    std::string targetGroupName;
    {
        // subscriptions is read on multiple threads. The OPC thread mutates
        // it inside onConnected; we only read here, but the values are POD
        // so a snapshot is safe enough for routing decisions.
        for(const auto &kv : ctx->subscriptions) {
            if(kv.first == ctx->name)
                continue; // skip the events subscription
            targetSubId = kv.second.subscriptionId;
            targetGroupName = kv.first;
            break;
        }
    }
    if(targetSubId == 0) {
        log("HotReload Add: no group subscription available on " +
                ctx->endpoint,
            LogLevel::ERRORS);
        return;
    }

    // Pre-build everything that does NOT need the UA_Client.
    // Each (item × mappedTagOption) pair becomes one monitor; if an item has
    // no mappedTagOptions, we register a single placeholder monitor keyed on
    // dataPointId/id.
    std::vector<MappedInfospaceTag> infoSpaces;
    std::vector<std::string>        nodePaths;
    std::vector<std::string>        newTopics; // MQTT topics to subscribe
    infoSpaces.reserve(items.size());
    nodePaths.reserve(items.size());

    auto resolvePath = [&](const HotReloadItem &it) -> std::string {
        if(!it.namespacePath.empty())
            return it.namespacePath;
        if(nsIdx == 0) {
            log("HotReload Add: item " + std::to_string(it.id) +
                    " has no namespace and ctx->resolvedNs not set on " +
                    ctx->endpoint,
                LogLevel::ERRORS);
            return std::string{};
        }
        return buildNodePath(nsIdx, it.nodeId);
    };

    // A valid OPC UA node path must start with "ns=" and contain a ";<i|s>=…"
    // identifier. Anything else (e.g. the hierarchy label "Node 7" returned
    // for OPC_HI_GROUP rows) cannot be monitored.
    auto isValidUaPath = [](const std::string &p) {
        if(p.compare(0, 3, "ns=") != 0) return false;
        const auto semi = p.find(';');
        if(semi == std::string::npos) return false;
        const auto eq = p.find('=', semi);
        return eq != std::string::npos;
    };

    for(const auto &it : items) {
        // Hot reload payloads can describe non-leaf hierarchy nodes
        // (OPC_HI_GROUP / OPC_HI_SERVER) — those aren't monitorable.
        if(!it.typeId.empty() && it.typeId != "OPC_HI_DATAPOINT") {
            log("HotReload Add: skipping non-datapoint item " +
                    std::to_string(it.id) + " (typeId=" + it.typeId + ")",
                LogLevel::DEBUG);
            continue;
        }

        std::string path = resolvePath(it);
        if(path.empty() || !isValidUaPath(path)) {
            log("HotReload Add: item " + std::to_string(it.id) +
                    " has no valid OPC UA path (got '" + path +
                    "') — skipping",
                LogLevel::WARNING);
            continue;
        }

        // Per-item base info (shared across mappedTagOptions).
        // `mappedId` is the mapped infospace's own id (e.g. 55444) and
        // populates MappedInfospaceTag::id; it is distinct from `tagId`
        // (the value used as the Mapping/TopicMapping key, e.g. 55701)
        // and from `it.id` (the parent tag's hierarchy id, e.g. 1651).
        auto buildInfo = [&](int mappedId, int tagId,
                             const std::string &mqttTopic, int orgId) {
            MappedInfospaceTag info{};
            info.id = mappedId;
            info.tagId = tagId;
            info.name = it.name;
            info.namespaces = mqttTopic;
            info.samplingInterval = it.sampling;
            info.deadband = 0;
            info.queuesize = it.queueSize > 0 ? it.queueSize : 10000;
            info.orgId = (orgId != 0) ? orgId
                                      : (it.orgId != 0 ? it.orgId : cmd.orgId);
            info.tagType = it.typeId;
            info.hierarchyId = it.id;
            return info;
        };

        if(!it.mappedTagOptions.empty()) {
            for(const auto &opt : it.mappedTagOptions) {
                const int tagId = opt.tagId;
                if(tagId == 0) continue;
                Mapping[tagId] = {path, ctx->endpoint};
                if(!opt.namespaces.empty()) {
                    TopicMapping[tagId] = opt.namespaces;
                    newTopics.push_back(opt.namespaces);
                }
                infoSpaces.push_back(
                    buildInfo(opt.id, tagId, opt.namespaces, opt.orgId));
                nodePaths.push_back(path);
            }
        } else {
            // Fallback: no mappedTagOptions in payload → use dataPointId / id.
            const int tagId = (it.dataPointId != 0) ? it.dataPointId : it.id;
            Mapping[tagId] = {path, ctx->endpoint};
            infoSpaces.push_back(buildInfo(0, tagId, std::string{}, 0));
            nodePaths.push_back(path);
        }
    }

    if(infoSpaces.empty()) {
        log("HotReload Add: nothing to register after filtering",
            LogLevel::WARNING);
        return;
    }

    // Make sure new control topics are subscribed at the broker so writes
    // are delivered to this client. subscribeBatch is idempotent.
    if(!newTopics.empty() && g_mqttHandler && g_mqttHandler->isConnected()) {
        try {
            g_mqttHandler->subscribeBatch(newTopics);
            log("HotReload Add: subscribed " +
                    std::to_string(newTopics.size()) +
                    " MQTT topic(s) for control writes",
                LogLevel::DEBUG);
        } catch(const std::exception &e) {
            log(std::string("HotReload Add: subscribeBatch failed: ") +
                    e.what(),
                LogLevel::WARNING);
        }
    }

    ctx->postOpcTask([ctx, targetSubId, targetGroupName,
                      infoSpaces = std::move(infoSpaces),
                      nodePaths  = std::move(nodePaths),
                      hierarchyId = cmd.firstId()]() mutable {
        if(!ctx->client || !ctx->isConnected.load(std::memory_order_acquire)) {
            log("HotReload Add: client not connected for " + ctx->endpoint,
                LogLevel::ERRORS);
            return;
        }

        const size_t count = infoSpaces.size();

        UA_CreateMonitoredItemsRequest batchReq;
        UA_CreateMonitoredItemsRequest_init(&batchReq);
        batchReq.subscriptionId     = targetSubId;
        batchReq.timestampsToReturn = UA_TIMESTAMPSTORETURN_BOTH;
        batchReq.itemsToCreate =
            (UA_MonitoredItemCreateRequest *)UA_Array_new(
                count, &UA_TYPES[UA_TYPES_MONITOREDITEMCREATEREQUEST]);
        batchReq.itemsToCreateSize = count;

        std::vector<MyMonitorContext *> ctxs(count, nullptr);
        std::vector<UA_Client_DataChangeNotificationCallback> cbs(
            count, handler_NodeValueChanged);
        std::vector<UA_Client_DeleteMonitoredItemCallback> delCbs(count, nullptr);

        for(size_t i = 0; i < count; ++i) {
            UA_NodeId nodeId = UA_NODEID_NULL;
            const std::string &p = nodePaths[i];
            const char *nsStart = strstr(p.c_str(), "ns=");
            if(nsStart) {
                const char *nsEnd = strchr(nsStart + 3, ';');
                if(nsEnd) {
                    UA_UInt16 nsI = (UA_UInt16)strtoul(nsStart + 3, nullptr, 10);
                    const char *idStart = nsEnd + 1;
                    if(strncmp(idStart, "i=", 2) == 0)
                        nodeId = UA_NODEID_NUMERIC(
                            nsI, (UA_UInt32)strtoul(idStart + 2, nullptr, 10));
                    else if(strncmp(idStart, "s=", 2) == 0)
                        nodeId = UA_NODEID_STRING_ALLOC(nsI, idStart + 2);
                }
            }

            UA_MonitoredItemCreateRequest &req = batchReq.itemsToCreate[i];
            UA_MonitoredItemCreateRequest_init(&req);
            UA_NodeId_copy(&nodeId, &req.itemToMonitor.nodeId);
            UA_NodeId_clear(&nodeId);
            req.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
            req.monitoringMode = UA_MONITORINGMODE_REPORTING;
            req.requestedParameters.samplingInterval = 0;
            req.requestedParameters.queueSize =
                (UA_UInt32)(infoSpaces[i].queuesize > 0 ? infoSpaces[i].queuesize
                                                       : 10000);
            req.requestedParameters.discardOldest = false;

            UA_DataChangeFilter *filter =
                (UA_DataChangeFilter *)UA_malloc(sizeof(UA_DataChangeFilter));
            UA_DataChangeFilter_init(filter);
            if(infoSpaces[i].deadband > 0) {
                filter->deadbandType = UA_DEADBANDTYPE_ABSOLUTE;
                filter->deadbandValue = infoSpaces[i].deadband;
            }
            filter->trigger = UA_DATACHANGETRIGGER_STATUSVALUE;
            UA_ExtensionObject &fobj = req.requestedParameters.filter;
            memset(&fobj, 0, sizeof(fobj));
            fobj.encoding = UA_EXTENSIONOBJECT_DECODED;
            fobj.content.decoded.type = &UA_TYPES[UA_TYPES_DATACHANGEFILTER];
            fobj.content.decoded.data = filter;

            ctxs[i] = new MyMonitorContext{
                infoSpaces[i], g_mqttHandler, g_sqliteService, nullptr,
                g_asyncPublisher,
                [ctx](std::function<void()> task) {
                    ctx->postTask(std::move(task));
                }};
        }

        UA_CreateMonitoredItemsResponse resp =
            UA_Client_MonitoredItems_createDataChanges(
                ctx->client.get(), batchReq, (void **)ctxs.data(), cbs.data(),
                delCbs.data());

        UA_CreateMonitoredItemsRequest_clear(&batchReq);

        size_t ok = 0;
        {
            std::lock_guard<std::mutex> regLk(ctx->registryMutex);
            for(size_t i = 0; i < resp.resultsSize; ++i) {
                if(resp.results[i].statusCode != UA_STATUSCODE_GOOD) {
                    log("HotReload Add: monitor failed for tagId " +
                            std::to_string(infoSpaces[i].tagId) + ": " +
                            UA_StatusCode_name(resp.results[i].statusCode),
                        LogLevel::ERRORS);
                    delete ctxs[i];
                    ctxs[i] = nullptr;
                } else {
                    ++ok;
                    LiveMonitorEntry entry;
                    entry.hierarchyId = hierarchyId;
                    entry.tagId = infoSpaces[i].tagId;
                    entry.monitoredItemId = resp.results[i].monitoredItemId;
                    entry.subscriptionId = targetSubId;
                    entry.groupName = targetGroupName;
                    entry.endpointUrl = ctx->endpoint;
                    entry.infoSpace = infoSpaces[i];
                    ctx->liveRegistry[hierarchyId].push_back(std::move(entry));
                }
            }
        }
        UA_CreateMonitoredItemsResponse_clear(&resp);

        log("HotReload Add: hierarchy " + std::to_string(hierarchyId) +
                " on " + ctx->endpoint + ": " + std::to_string(ok) + "/" +
                std::to_string(count) + " monitors created",
            LogLevel::INFO);
    });
}

// purpose=OPC_HI_GROUP, action=Add: create a new OPC UA subscription with the
// group's parameters (publishingInterval, keepAlive, lifetime, priority, etc.)
// and register it so future datapoint adds / group updates can route to it.
//
// The API returns only the group metadata row (children array is empty), so
// we only create the subscription here — no monitored items are registered.
static void
applyAddGroup(ClientContext *ctx, const HotReloadCommand &cmd,
              const std::vector<HotReloadItem> &items) {
    if(!ctx) return;

    // Find the OPC_HI_GROUP row in the response.
    const HotReloadItem *groupItem = nullptr;
    for(const auto &it : items) {
        if(it.typeId == "OPC_HI_GROUP") {
            groupItem = &it;
            break;
        }
    }
    if(!groupItem) {
        log("HotReload AddGroup: no OPC_HI_GROUP row in API response for "
            "id=" + std::to_string(cmd.firstId()),
            LogLevel::WARNING);
        return;
    }

    const int hierarchyId = groupItem->id;
    const std::string groupName =
        !groupItem->name.empty() ? groupItem->name
                                 : ("group_" + std::to_string(hierarchyId));

    // Check if this group already exists.
    if(ctx->subscriptions.find(groupName) != ctx->subscriptions.end()) {
        log("HotReload AddGroup: subscription '" + groupName +
                "' already exists on " + ctx->endpoint + " — skipping",
            LogLevel::WARNING);
        return;
    }

    // Extract subscription parameters (mirroring the startup pattern at
    // onConnected). Use defaults that match UA_CreateSubscriptionRequest_default
    // when the API doesn't ship a value.
    const int pubInterval =
        (groupItem->publishingInterval > 0) ? groupItem->publishingInterval : 1000;
    const UA_UInt32 requestedKeepAlive =
        (groupItem->maxKeepAliveCount > 0) ? (UA_UInt32)groupItem->maxKeepAliveCount : 40;
    UA_UInt32 requestedLifetime =
        (groupItem->lifetimeCount > 0) ? (UA_UInt32)groupItem->lifetimeCount
                                       : (requestedKeepAlive * 3);
    if(requestedLifetime < (requestedKeepAlive * 3))
        requestedLifetime = requestedKeepAlive * 3; // OPC UA constraint
    const int priority =
        (groupItem->priority >= 0) ? groupItem->priority : 0;
    const int maxNotif =
        (groupItem->maxNotificationsPerPublish >= 0)
            ? groupItem->maxNotificationsPerPublish
            : 0;

    // Create the subscription on the OPC thread (UA_Client* is only safe there).
    ctx->postOpcTask([ctx, groupName, hierarchyId, pubInterval,
                      requestedKeepAlive, requestedLifetime, priority,
                      maxNotif]() {
        if(!ctx->client || !ctx->isConnected.load(std::memory_order_acquire)) {
            log("HotReload AddGroup: client not connected for " + ctx->endpoint,
                LogLevel::ERRORS);
            return;
        }

        UA_CreateSubscriptionRequest greq = UA_CreateSubscriptionRequest_default();
        greq.requestedPublishingInterval = (UA_Double)pubInterval;
        greq.requestedMaxKeepAliveCount = requestedKeepAlive;
        greq.requestedLifetimeCount = requestedLifetime;
        greq.priority = (UA_Byte)std::min(255, priority);
        greq.maxNotificationsPerPublish = (UA_UInt32)maxNotif;

        UA_CreateSubscriptionResponse gsub =
            UA_Client_Subscriptions_create(ctx->client.get(), greq,
                                           nullptr, nullptr, nullptr);
        if(gsub.responseHeader.serviceResult != UA_STATUSCODE_GOOD) {
            log("HotReload AddGroup: subscription creation failed for '" +
                    groupName + "' on " + ctx->endpoint + ": " +
                    UA_StatusCode_name(gsub.responseHeader.serviceResult),
                LogLevel::ERRORS);
            return;
        }

        ctx->subscriptions[groupName] = gsub;
        if(hierarchyId != 0) {
            ctx->groupIdToName[hierarchyId] = groupName;
        }

        log("HotReload AddGroup: subscription '" + groupName +
                "' created on " + ctx->endpoint +
                " (hierarchyId=" + std::to_string(hierarchyId) +
                ", subId=" + std::to_string(gsub.subscriptionId) +
                ", interval=" + std::to_string(gsub.revisedPublishingInterval) +
                "ms)",
            LogLevel::INFO);
    });
}

// Delete every live monitored item on `ctx` that matches `deleteId`.
// The MQTT `id` may be either the tag's hierarchy id (dataPointId / hot reload
// `id`) or a mapped telemetry tagId (key in Mapping). We try registry lookup
// by hierarchy key first, then collect any entries whose LiveMonitorEntry::tagId
// matches.
static bool
applyDeleteSingle(ClientContext *ctx, int deleteId) {
    if(!ctx) return false;

    std::vector<LiveMonitorEntry> snapshot;
    {
        std::lock_guard<std::mutex> regLk(ctx->registryMutex);
        auto it = ctx->liveRegistry.find(deleteId);
        if(it != ctx->liveRegistry.end() && !it->second.empty()) {
            snapshot = std::move(it->second);
            ctx->liveRegistry.erase(it);
        } else {
            for(auto rit = ctx->liveRegistry.begin();
                rit != ctx->liveRegistry.end();) {
                auto &vec = rit->second;
                auto split = std::partition(
                    vec.begin(), vec.end(), [&](const LiveMonitorEntry &e) {
                        return e.tagId != deleteId;
                    });
                for(auto j = split; j != vec.end(); ++j)
                    snapshot.push_back(std::move(*j));
                vec.erase(split, vec.end());
                if(vec.empty())
                    rit = ctx->liveRegistry.erase(rit);
                else
                    ++rit;
            }
            if(snapshot.empty()) {
                log("HotReload Delete: no live monitors for id " +
                        std::to_string(deleteId) +
                        " on " + ctx->endpoint +
                        " (not found as hierarchyId or tagId in registry)",
                    LogLevel::DEBUG);
                return true;
            }
        }
    }

    // Clear global Mapping / TopicMapping for these tag ids up front so MQTT
    // control writes targeting these tags fail fast while the OPC delete is
    // queued.
    for(const auto &e : snapshot) {
        Mapping.erase(e.tagId);
        TopicMapping.erase(e.tagId);
    }

    ctx->postOpcTask([ctx, snapshot = std::move(snapshot), deleteId]() {
        if(!ctx->client) {
            log("HotReload Delete: client null for " + ctx->endpoint,
                LogLevel::ERRORS);
            return;
        }
        size_t ok = 0;
        for(const auto &e : snapshot) {
            UA_StatusCode rc = UA_Client_MonitoredItems_deleteSingle(
                ctx->client.get(), e.subscriptionId, e.monitoredItemId);
            if(rc == UA_STATUSCODE_GOOD)
                ++ok;
            else
                log("HotReload Delete: failed for tagId " +
                        std::to_string(e.tagId) + ": " +
                        UA_StatusCode_name(rc),
                    LogLevel::ERRORS);
        }
        log("HotReload Delete: id " + std::to_string(deleteId) + " on " +
                ctx->endpoint + ": " + std::to_string(ok) + "/" +
                std::to_string(snapshot.size()) + " monitors removed",
            LogLevel::INFO);
    });
    return true;
}

// Delete an entire OPC UA group: tear down every monitored item under the
// group's subscription and delete the subscription itself. The MQTT command
// id is the group's hierarchy id (= GroupInfo::dataPointId at startup).
//
// We never call the API for Delete — `groupHierarchyId` is the only input.
static void
applyDeleteGroup(ClientContext *ctx, int groupHierarchyId) {
    if(!ctx) return;

    // Resolve the subscription via groupIdToName (populated in onConnected).
    std::string groupName;
    {
        auto it = ctx->groupIdToName.find(groupHierarchyId);
        if(it == ctx->groupIdToName.end()) {
            log("HotReload DeleteGroup: id " +
                    std::to_string(groupHierarchyId) + " unknown on " +
                    ctx->endpoint,
                LogLevel::DEBUG);
            return;
        }
        groupName = it->second;
    }

    auto subIt = ctx->subscriptions.find(groupName);
    if(subIt == ctx->subscriptions.end()) {
        log("HotReload DeleteGroup: subscription '" + groupName +
                "' not active on " + ctx->endpoint,
            LogLevel::WARNING);
        return;
    }
    const UA_UInt32 subscriptionId = subIt->second.subscriptionId;

    // Snapshot every live entry under this subscription so we can scrub
    // Mapping/TopicMapping eagerly (MQTT writes that arrive after this point
    // for the affected tag ids fail fast instead of routing into a soon-to-
    // be-deleted subscription).
    std::vector<LiveMonitorEntry> doomed;
    {
        std::lock_guard<std::mutex> regLk(ctx->registryMutex);
        for(auto rit = ctx->liveRegistry.begin();
            rit != ctx->liveRegistry.end();) {
            auto &vec = rit->second;
            // Partition entries: keep ones not under this subscription.
            auto split = std::partition(
                vec.begin(), vec.end(), [&](const LiveMonitorEntry &e) {
                    return e.subscriptionId != subscriptionId;
                });
            for(auto it = split; it != vec.end(); ++it)
                doomed.push_back(std::move(*it));
            vec.erase(split, vec.end());
            if(vec.empty())
                rit = ctx->liveRegistry.erase(rit);
            else
                ++rit;
        }
    }
    for(const auto &e : doomed) {
        Mapping.erase(e.tagId);
        TopicMapping.erase(e.tagId);
    }

    ctx->postOpcTask([ctx, subscriptionId, groupName,
                      groupHierarchyId, removed = doomed.size()]() {
        if(!ctx->client) {
            log("HotReload DeleteGroup: client null for " + ctx->endpoint,
                LogLevel::ERRORS);
            return;
        }
        // Deleting the subscription removes every monitored item it owns,
        // so we don't need a separate per-item delete call here.
        UA_StatusCode rc = UA_Client_Subscriptions_deleteSingle(
            ctx->client.get(), subscriptionId);
        if(rc != UA_STATUSCODE_GOOD) {
            log("HotReload DeleteGroup: subscription delete failed for '" +
                    groupName + "' (id=" + std::to_string(subscriptionId) +
                    "): " + UA_StatusCode_name(rc),
                LogLevel::ERRORS);
        }
        // Drop client-side bookkeeping regardless of UA status — the
        // subscription is gone (or unreachable) either way.
        ctx->subscriptions.erase(groupName);
        ctx->groupIdToName.erase(groupHierarchyId);

        log("HotReload DeleteGroup: '" + groupName + "' (id=" +
                std::to_string(groupHierarchyId) + ") on " + ctx->endpoint +
                " removed (" + std::to_string(removed) +
                " monitored items dropped)",
            LogLevel::INFO);
    });
}

// Update = delete existing live monitors for this hierarchy id, then add
// back using the freshly fetched items. Datapoint-level only; group / server
// updates take separate paths (applyGroupUpdate, applyReload-on-one-server).
static void
applyUpdate(ClientContext *ctx, const HotReloadCommand &cmd,
            const std::vector<HotReloadItem> &items) {
    applyDeleteSingle(ctx, cmd.firstId());
    applyAdd(ctx, cmd, items);
}

static void
applyReload(ClientContext *ctx, const HotReloadCommand &cmd,
            const std::vector<HotReloadItem> & /*items*/) {
    // Reload restarts the OPC UA client connection. The teardown MUST run on
    // the OPC thread so it doesn't race the reconnect logic at the top of
    // ClientContext::startLoop. Critically, `isConnected` is flipped to false
    // ONLY at the very end of the task — otherwise the OPC loop would see
    // !isConnected on its next iteration and reconnect *before* this task
    // ever ran, then we'd tear down the freshly-rebuilt session.
    if(!ctx) return;

    log("HotReload Reload: queuing restart for " + ctx->endpoint +
            " (triggered by id=" + std::to_string(cmd.firstId()) +
            " purpose=" + cmd.purpose + ")",
        LogLevel::INFO);

    ctx->postOpcTask([ctx]() {
        // 1. Cleanly disconnect the live session. We do NOT reset the
        //    UA_Client here: connectOnce() in the reconnect path replaces
        //    ctx->client with a freshly-constructed instance via
        //    `ctx->client.reset(UA_Client_new())`, and we want the existing
        //    iteration to be able to call UA_Client_run_iterate (even on a
        //    disconnected channel) without dereferencing a null pointer.
        if(ctx->client) {
            UA_Client_disconnect(ctx->client.get());
        }

        // 2. Wipe all derived state that onConnected will rebuild.
        ctx->subscriptions.clear();
        ctx->groupIdToName.clear();
        ctx->resolvedNs.store(0, std::memory_order_release);

        // 3. Drain liveRegistry and scrub the corresponding entries from
        //    global Mapping / TopicMapping so MQTT control writes referencing
        //    the reconnect repopulates them. (Removed because onConnected uses cached startup data)
        std::vector<int> ownedTagIds;
        {
            std::lock_guard<std::mutex> regLk(ctx->registryMutex);
            for(const auto &kv : ctx->liveRegistry) {
                for(const auto &e : kv.second)
                    ownedTagIds.push_back(e.tagId);
            }
            ctx->liveRegistry.clear();
        }

        // 4. LAST step: signal the OPC loop to reconnect on its next
        //    iteration. Order matters: any earlier flip would let the loop
        //    reconnect before the cleanup above completes.
        ctx->isConnected.store(false, std::memory_order_release);

        log("HotReload Reload: " + ctx->endpoint +
                " teardown complete; reconnect pending (cleared " +
                std::to_string(ownedTagIds.size()) + " tag mappings)",
            LogLevel::INFO);
    });
}

// Apply an `Update` command whose purpose is `OPC_HI_GROUP`: re-apply the
// group's subscription-level parameters via UA_Client_Subscriptions_modify.
// This neither creates nor destroys monitored items — it only mutates the
// subscription that already drives them.
static void
applyGroupUpdate(ClientContext *ctx, const HotReloadCommand &cmd,
                 const std::vector<HotReloadItem> &items) {
    if(!ctx) return;

    // Find the group-typed item that matches the command id. Hot reload
    // responses for groups always contain a single row, but be defensive.
    const int targetId = cmd.firstId();
    const HotReloadItem *match = nullptr;
    for(const auto &it : items) {
        if(it.typeId == "OPC_HI_GROUP" && it.id == targetId) {
            match = &it;
            break;
        }
    }
    if(!match) {
        // Fall back to the first OPC_HI_GROUP entry if id matching fails.
        for(const auto &it : items) {
            if(it.typeId == "OPC_HI_GROUP") {
                match = &it;
                break;
            }
        }
    }
    if(!match) {
        log("HotReload GroupUpdate: no OPC_HI_GROUP row in API response for "
            "id=" + std::to_string(targetId),
            LogLevel::WARNING);
        return;
    }

    // Look up the live subscription for this group on this context.
    // Primary: match by hierarchy id captured at startup. Fallback: match by
    // the API response's `name` field directly against `ctx->subscriptions`,
    // since startup uses `group.name` as the subscription key. The fallback
    // is essential when the startup hierarchy API doesn't expose a top-level
    // `id` for groups but the hot reload API does.
    std::string groupName;
    auto nameIt = ctx->groupIdToName.find(targetId);
    if(nameIt != ctx->groupIdToName.end()) {
        groupName = nameIt->second;
    } else if(!match->name.empty() &&
              ctx->subscriptions.find(match->name) != ctx->subscriptions.end()) {
        groupName = match->name;
        log("HotReload GroupUpdate: id " + std::to_string(targetId) +
                " not in groupIdToName; resolved by name '" + match->name +
                "' on " + ctx->endpoint,
            LogLevel::DEBUG);
        ctx->groupIdToName[targetId] = match->name;
    } else {
        log("HotReload GroupUpdate: no group with id " +
                std::to_string(targetId) + " or name '" + match->name +
                "' known on " + ctx->endpoint,
            LogLevel::DEBUG);
        return;
    }

    auto subIt = ctx->subscriptions.find(groupName);
    if(subIt == ctx->subscriptions.end()) {
        log("HotReload GroupUpdate: subscription '" + groupName +
                "' not active on " + ctx->endpoint,
            LogLevel::WARNING);
        return;
    }
    const UA_CreateSubscriptionResponse current = subIt->second;

    // Build the Modify request. Where the API didn't ship a value, keep the
    // current revised value so we don't downgrade settings the server has
    // already committed to.
    const HotReloadItem &m = *match;
    UA_ModifySubscriptionRequest mreq;
    UA_ModifySubscriptionRequest_init(&mreq);
    mreq.subscriptionId = current.subscriptionId;
    mreq.requestedPublishingInterval =
        (m.publishingInterval > 0) ? (UA_Double)m.publishingInterval
                                   : current.revisedPublishingInterval;

    const UA_UInt32 newKeepAlive =
        (m.maxKeepAliveCount > 0) ? (UA_UInt32)m.maxKeepAliveCount
                                  : current.revisedMaxKeepAliveCount;
    mreq.requestedMaxKeepAliveCount = newKeepAlive;

    UA_UInt32 newLifetime =
        (m.lifetimeCount > 0)
            ? (UA_UInt32)m.lifetimeCount
            : current.revisedLifetimeCount;
    if(newLifetime < newKeepAlive * 3)
        newLifetime = newKeepAlive * 3; // OPC UA constraint
    mreq.requestedLifetimeCount = newLifetime;

    mreq.maxNotificationsPerPublish =
        (m.maxNotificationsPerPublish >= 0)
            ? (UA_UInt32)m.maxNotificationsPerPublish
            : 0;
    mreq.priority =
        (m.priority >= 0) ? (UA_Byte)std::min(255, m.priority)
                          : (UA_Byte)0;

    log("HotReload GroupUpdate: queuing modify for '" + groupName +
            "' on " + ctx->endpoint +
            " | interval=" + std::to_string(m.publishingInterval) +
            " keepAlive=" + std::to_string(m.maxKeepAliveCount) +
            " priority=" + std::to_string(m.priority),
        LogLevel::INFO);

    ctx->postOpcTask([ctx, mreq, groupName]() {
        if(!ctx->client ||
           !ctx->isConnected.load(std::memory_order_acquire)) {
            log("HotReload GroupUpdate: client not connected for " +
                    ctx->endpoint,
                LogLevel::ERRORS);
            return;
        }

        UA_ModifySubscriptionResponse mresp =
            UA_Client_Subscriptions_modify(ctx->client.get(), mreq);

        if(mresp.responseHeader.serviceResult != UA_STATUSCODE_GOOD) {
            log("HotReload GroupUpdate: modify failed for '" + groupName +
                    "': " +
                    UA_StatusCode_name(mresp.responseHeader.serviceResult),
                LogLevel::ERRORS);
        } else {
            // Update our cached revised values so future modifies see the
            // freshest server-acknowledged numbers.
            auto sIt = ctx->subscriptions.find(groupName);
            if(sIt != ctx->subscriptions.end()) {
                sIt->second.revisedPublishingInterval =
                    mresp.revisedPublishingInterval;
                sIt->second.revisedMaxKeepAliveCount =
                    mresp.revisedMaxKeepAliveCount;
                sIt->second.revisedLifetimeCount =
                    mresp.revisedLifetimeCount;
            }
            log("HotReload GroupUpdate: '" + groupName +
                    "' modified | revisedInterval=" +
                    std::to_string(mresp.revisedPublishingInterval) +
                    "ms revisedKeepAlive=" +
                    std::to_string(mresp.revisedMaxKeepAliveCount) +
                    " revisedLifetime=" +
                    std::to_string(mresp.revisedLifetimeCount),
                LogLevel::INFO);
        }
        UA_ModifySubscriptionResponse_clear(&mresp);
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// OPC_HI_SERVER dispatchers (Add / Delete / Update)
// ─────────────────────────────────────────────────────────────────────────────

// Find a connected ClientContext by serverHierarchyId. Returns nullptr if no
// match. Caller MUST hold g_clientPoolMutex.
static ClientContext *
findServerByHierarchyId_locked(
    int serverHierarchyId,
    std::unordered_map<std::string, ClientContext *> &clientPool) {
    for(auto &kv : clientPool) {
        if(kv.second && kv.second->serverHierarchyId == serverHierarchyId)
            return kv.second;
    }
    return nullptr;
}

// Subscribe to every MQTT control topic owned by a freshly-built server so
// MQTT writes can route into it. Idempotent — duplicate subscriptions are
// no-ops at the broker.
static void
subscribeServerControlTopics(const ServerInfoO &server) {
    if(!g_mqttHandler || !g_mqttHandler->isConnected())
        return;
    std::vector<std::string> topics;
    for(const auto &group : server.groups) {
        for(const auto &tag : group.tags) {
            if(tag.rdWtOpt != "RD_WRT_RW")
                continue;
            if(!tag.mappedInfospaceTags)
                continue;
            for(const auto &mi : *tag.mappedInfospaceTags) {
                if(!mi.namespaces.empty())
                    topics.push_back(mi.namespaces);
            }
        }
    }
    if(topics.empty())
        return;
    try {
        g_mqttHandler->subscribeBatch(topics);
        log("HotReload AddServer: subscribed " +
                std::to_string(topics.size()) +
                " MQTT control topic(s) for " + server.name,
            LogLevel::INFO);
    } catch(const std::exception &e) {
        log(std::string("HotReload AddServer: subscribeBatch failed: ") +
                e.what(),
            LogLevel::WARNING);
    }
}

// POST /api/EdgentHotReloading with automatic bearer refresh: uses the
// in-memory token first, fetches a new one when it is empty, and on HTTP 401
// / 403 re-signs in once and retries (JWT from startup often expires long
// before the OPC UA client process exits).
static json
callHotReloadApiWithTokenRefresh(
    const std::string &apiHost, const std::string &apiPort,
    std::string &bearerToken, const std::string &authUsername,
    const std::string &authPassword, const json &commandBody) {
    std::string tok;
    {
        std::lock_guard<std::mutex> lk(g_bearerTokenMutex);
        tok = bearerToken;
    }

    if(tok.empty()) {
        log("HotReload: bearer token empty — fetching before API call",
            LogLevel::INFO);
        try {
            std::string fresh =
                getBearerTokenNow(apiHost, apiPort, authUsername,
                                  authPassword, true).get();
            if(!fresh.empty()) {
                {
                    std::lock_guard<std::mutex> lk(g_bearerTokenMutex);
                    bearerToken = fresh;
                }
                tok = std::move(fresh);
                if(g_sqliteService)
                    g_sqliteService->SetApiAuth(tok);
            }
        } catch(const std::exception &e) {
            log(std::string("HotReload: initial token fetch failed: ") +
                    e.what(),
                LogLevel::ERRORS);
        }
    }

    unsigned st = 0;
    json resp =
        callHotReloadAPI(apiHost, apiPort, tok, commandBody, &st);
    if(!resp.is_null())
        return resp;

    if(st == 401U || st == 403U) {
        log("HotReload: EdgentHotReloading HTTP " + std::to_string(st) +
                " — refreshing bearer token and retrying once",
            LogLevel::WARNING);
        try {
            std::string fresh = getBearerTokenNow(apiHost, apiPort, authUsername,
                                                  authPassword, true).get();
            if(!fresh.empty()) {
                {
                    std::lock_guard<std::mutex> lk(g_bearerTokenMutex);
                    bearerToken = fresh;
                }
                if(g_sqliteService)
                    g_sqliteService->SetApiAuth(fresh);
                return callHotReloadAPI(apiHost, apiPort, fresh,
                                        commandBody, nullptr);
            }
        } catch(const std::exception &e) {
            log(std::string("HotReload: token refresh failed: ") + e.what(),
                LogLevel::ERRORS);
        }
    }
    return json{};
}

// purpose=OPC_HI_SERVER, action=Add (Option C): hit the API to fetch the
// new server's full descriptor (security, auth, groups, tags), then spawn a
// fresh ClientContext via g_buildContext and register it in the pool.
//
// Idempotent: skips servers whose endpointUrl is already in the pool.
static void
applyAddServer(
    const std::string &apiHost, const std::string &apiPort,
    std::string &bearerToken, const std::string &authUsername,
    const std::string &authPassword, const json &commandBody,
    std::unordered_map<std::string, ClientContext *> &clientPool,
    std::vector<std::unique_ptr<ClientContext>> &clientContexts) {
    if(!g_buildContext) {
        log("HotReload AddServer: g_buildContext not initialized — cannot "
            "spawn new server contexts",
            LogLevel::ERRORS);
        return;
    }

    json resp = callHotReloadApiWithTokenRefresh(
        apiHost, apiPort, bearerToken, authUsername, authPassword,
        commandBody);
    if(resp.is_null()) {
        log("HotReload AddServer: API returned no usable payload",
            LogLevel::ERRORS);
        return;
    }
    std::vector<ServerInfoO> servers = parseHotReloadServerEntries(resp);
    if(servers.empty()) {
        log("HotReload AddServer: API returned no server entries",
            LogLevel::WARNING);
        return;
    }

    for(const auto &server : servers) {
        if(server.endpointUrl.empty()) {
            log("HotReload AddServer: skipping entry with empty endpointUrl",
                LogLevel::WARNING);
            continue;
        }

        // Idempotency: don't re-add an endpoint we already serve.
        {
            std::lock_guard<std::mutex> lk(g_clientPoolMutex);
            if(clientPool.find(server.endpointUrl) != clientPool.end()) {
                log("HotReload AddServer: '" + server.endpointUrl +
                        "' already in pool — skipping",
                    LogLevel::WARNING);
                continue;
            }
        }

        // Subscribe MQTT control topics first so any inbound writes after
        // the context joins the pool can be routed immediately.
        subscribeServerControlTopics(server);

        auto ctx = g_buildContext(server);
        if(!ctx) {
            log("HotReload AddServer: failed to build context for '" +
                    server.endpointUrl + "'",
                LogLevel::ERRORS);
            continue;
        }

        const std::string endpoint = ctx->endpoint;
        const int hierarchyId = ctx->serverHierarchyId;
        {
            std::lock_guard<std::mutex> lk(g_clientPoolMutex);
            clientPool[endpoint] = ctx.get();
            clientContexts.push_back(std::move(ctx));
        }
        if(!server.typeId.empty()) {
            std::lock_guard<std::mutex> lk(g_purposeMutex);
            g_purposeToEndpoints[server.typeId].push_back(endpoint);
        }

        log("HotReload AddServer: '" + endpoint +
                "' added (serverHierarchyId=" + std::to_string(hierarchyId) +
                ")",
            LogLevel::INFO);
    }
}

// purpose=OPC_HI_SERVER, action=Delete (Option B): stop the matching
// ClientContext, scrub global Mapping/TopicMapping/g_purposeToEndpoints, and
// remove it from the pool. NEVER calls the API — driven purely by id.
static void
applyDeleteServer(
    int serverHierarchyId,
    std::unordered_map<std::string, ClientContext *> &clientPool,
    std::vector<std::unique_ptr<ClientContext>> &clientContexts) {
    // Hold the pool mutex only long enough to detach the context from the
    // pool — stopLoop() joins the OPC + worker threads which may take a
    // while, and we don't want to block MQTT control-write lookups for that
    // duration.
    std::unique_ptr<ClientContext> owned;
    std::string endpoint;
    std::string typeId;
    {
        std::lock_guard<std::mutex> lk(g_clientPoolMutex);
        ClientContext *raw =
            findServerByHierarchyId_locked(serverHierarchyId, clientPool);
        if(!raw) {
            log("HotReload DeleteServer: no server with hierarchyId=" +
                    std::to_string(serverHierarchyId) + " in pool",
                LogLevel::WARNING);
            return;
        }
        endpoint = raw->endpoint;
        typeId = raw->typeId;
        clientPool.erase(endpoint);

        auto it = std::find_if(
            clientContexts.begin(), clientContexts.end(),
            [raw](const std::unique_ptr<ClientContext> &up) {
                return up.get() == raw;
            });
        if(it != clientContexts.end()) {
            owned = std::move(*it);
            clientContexts.erase(it);
        }
    }
    if(!owned) {
        log("HotReload DeleteServer: context for hierarchyId=" +
                std::to_string(serverHierarchyId) +
                " was in pool but missing from clientContexts",
            LogLevel::ERRORS);
        return;
    }

    // Scrub global Mapping/TopicMapping for every tag this server owns
    // BEFORE stopping its threads — once we set running=false, no thread
    // will be able to clean up after itself.
    {
        std::lock_guard<std::mutex> regLk(owned->registryMutex);
        for(const auto &kv : owned->liveRegistry) {
            for(const auto &e : kv.second) {
                Mapping.erase(e.tagId);
                TopicMapping.erase(e.tagId);
            }
        }
        owned->liveRegistry.clear();
    }

    // Drop the routing entry so future hot reload commands don't dispatch
    // to a dead context.
    if(!typeId.empty()) {
        std::lock_guard<std::mutex> lk(g_purposeMutex);
        auto it = g_purposeToEndpoints.find(typeId);
        if(it != g_purposeToEndpoints.end()) {
            auto &eps = it->second;
            eps.erase(std::remove(eps.begin(), eps.end(), endpoint),
                      eps.end());
            if(eps.empty())
                g_purposeToEndpoints.erase(it);
        }
    }

    // Stop OPC + worker threads (blocking — joins both). Caller is the hot
    // reload worker thread, which is dedicated to serializing these
    // operations.
    log("HotReload DeleteServer: stopping '" + endpoint +
            "' (serverHierarchyId=" + std::to_string(serverHierarchyId) +
            ")",
        LogLevel::INFO);
    owned->stopLoop();
    if(owned->client) {
        UA_Client_disconnect(owned->client.get());
    }

    log("HotReload DeleteServer: '" + endpoint + "' removed from pool",
        LogLevel::INFO);
    // owned is destroyed here, freeing UA_Client_delete via the deleter.
}

static bool
contextOwnsDatapointDeleteId(ClientContext *ctx, int id) {
    if(!ctx) return false;
    std::lock_guard<std::mutex> regLk(ctx->registryMutex);
    auto it = ctx->liveRegistry.find(id);
    if(it != ctx->liveRegistry.end() && !it->second.empty())
        return true;
    for(const auto &kv : ctx->liveRegistry) {
        for(const auto &e : kv.second) {
            if(e.tagId == id)
                return true;
        }
    }
    return false;
}

static bool
contextOwnsGroupDeleteId(ClientContext *ctx, int id) {
    return ctx && ctx->groupIdToName.find(id) != ctx->groupIdToName.end();
}

// Locate every connected ClientContext that handles a given purpose.
//
// `purpose` only matches a server-level typeId for the top-of-hierarchy case
// (e.g. "OPC_HI_SERVER"). Sub-tree purposes like "OPC_HI_GROUP" or
// "OPC_HI_DATAPOINT" never appear in `g_purposeToEndpoints`, so we fall back
// to every connected client context. Wrong-server dispatches are absorbed by:
//   - applyDelete: liveRegistry only has the matching hierarchyId on the
//     owning server; other contexts no-op.
//   - applyAdd:   the OPC UA service rejects unknown nodeIds with
//     BadNodeIdInvalid, which is logged but harmless.
//
// For `Delete`, we first narrow to contexts that actually own the id(s) —
// via liveRegistry (hierarchyId or tagId), groupIdToName, or global Mapping
// — so we do not spam DEBUG logs on every other server in the pool.
static std::vector<ClientContext *>
resolveHotReloadTargets(
    const HotReloadCommand &cmd,
    const std::unordered_map<std::string, ClientContext *> &clientPool) {
    std::vector<ClientContext *> out;
    std::vector<std::string> endpoints;
    {
        std::lock_guard<std::mutex> lk(g_purposeMutex);
        auto it = g_purposeToEndpoints.find(cmd.purpose);
        if(it != g_purposeToEndpoints.end())
            endpoints = it->second;
    }
    for(const auto &ep : endpoints) {
        auto cit = clientPool.find(ep);
        if(cit == clientPool.end() || !cit->second)
            continue;
        if(!cit->second->isConnected.load(std::memory_order_acquire))
            continue;
        out.push_back(cit->second);
    }

    if(!out.empty())
        return out;

    if(cmd.action == "Delete" &&
       (cmd.purpose == "OPC_HI_DATAPOINT" ||
        cmd.purpose == "OPC_HI_GROUP")) {
        std::lock_guard<std::mutex> plk(g_clientPoolMutex);
        for(const auto &kv : clientPool) {
            ClientContext *ctx = kv.second;
            if(!ctx ||
               !ctx->isConnected.load(std::memory_order_acquire))
                continue;
            bool use = false;
            for(int delId : cmd.ids) {
                if(cmd.purpose == "OPC_HI_GROUP")
                    use = contextOwnsGroupDeleteId(ctx, delId);
                else
                    use = contextOwnsDatapointDeleteId(ctx, delId);
                if(use) break;
            }
            if(use)
                out.push_back(ctx);
        }
        if(!out.empty()) {
            log("HotReload: Delete — routing to " +
                    std::to_string(out.size()) +
                    " context(s) that own the id(s) (registry match)",
                LogLevel::DEBUG);
            return out;
        }

        if(cmd.purpose == "OPC_HI_DATAPOINT") {
            for(int delId : cmd.ids) {
                auto mit = Mapping.find(delId);
                if(mit == Mapping.end())
                    continue;
                auto cit = clientPool.find(mit->second.second);
                if(cit == clientPool.end() || !cit->second ||
                   !cit->second->isConnected.load(std::memory_order_acquire))
                    continue;
                if(std::find(out.begin(), out.end(), cit->second) ==
                   out.end())
                    out.push_back(cit->second);
            }
            if(!out.empty()) {
                log("HotReload: Delete — routing via Mapping endpoint to " +
                        std::to_string(out.size()) + " context(s)",
                    LogLevel::DEBUG);
                return out;
            }
        }
    }

    // Fallback: route to every connected context.
    log("HotReload: purpose '" + cmd.purpose +
            "' is not a server typeId — routing to all connected contexts",
        LogLevel::DEBUG);
    for(const auto &kv : clientPool) {
        if(!kv.second)
            continue;
        if(!kv.second->isConnected.load(std::memory_order_acquire))
            continue;
        out.push_back(kv.second);
    }
    return out;
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

    // ── Build purpose → endpoint(s) map for hot reload routing ───────────────
    // HotReloadCommand::purpose carries the ServerInfoO::typeId of the target
    // hierarchy (e.g. "OPC_HI_SERVER"); we may have multiple connected servers
    // with the same typeId, so the value is a list.
    {
        std::lock_guard<std::mutex> lk(g_purposeMutex);
        g_purposeToEndpoints.clear();
        for(const auto &server : serverList) {
            if(!server.typeId.empty()) {
                g_purposeToEndpoints[server.typeId].push_back(
                    server.endpointUrl);
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
            [applicationEndURLHost, applicationEndURLPort, &edgeCfg,
             &BearerToken]() -> std::string {
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
                    {
                        std::lock_guard<std::mutex> lk(g_bearerTokenMutex);
                        BearerToken = freshToken;
                    }
                    SetEdgeConfigMqttPassword(edgeCfg, freshToken);
                    if(g_sqliteService)
                        g_sqliteService->SetApiAuth(freshToken);
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

    // Hot reload trigger channel — subscribed alongside per-tag control topics
    // so the broker delivers Add/Delete/Update/Reload commands on the same
    // session.
    topicsToSubscribe.push_back("HTRLD/Edgents");

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

    

    // Per-server setup wrapped as a callable so the hot reload worker can
    // spawn brand-new ClientContexts at runtime (purpose=OPC_HI_SERVER, action=Add)
    // using the exact same pipeline as startup. Returns a fully-started
    // ClientContext on success, or nullptr if any blocking error (cert load
    // failure, profile lookup error, etc.) prevents bringing the server up.
    auto buildAndStartContext =
        [&](const ServerInfoO &server) -> std::unique_ptr<ClientContext> {
        auto server_copy = server;
        auto context = std::make_unique<ClientContext>();
        context->name = server.name;
        context->endpoint = server.endpointUrl;
        context->typeId = server.typeId;
        context->serverHierarchyId = server.dataPointId;
        
        
    
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
            return nullptr;
        }

        if(profile.currentOrgId.empty()) {
            log("❌ No currentOrgId in user profile for " + server.name,
                LogLevel::ERRORS);
            return nullptr;
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
        // Cache the URI on the context so hot reload paths (running outside
        // onConnected) can re-resolve the namespace index after a reconnect.
        context->namespaceURI = NamespaceURI;
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
                // The startup hierarchy API ships the group's hierarchy id
                // under `dataPointId`; the hot reload MQTT command refers to
                // the same number as `id`. Populating groupIdToName here lets
                // applyGroupUpdate look up the subscription by id directly.
                if(group.dataPointId != 0) {
                    ctx->groupIdToName[group.dataPointId] = group.name;
                } else {
                    log("Subscription Created with no hierarchy id for '" +
                            group.name +
                            "' — hot reload group updates will resolve by "
                            "name only",
                        LogLevel::DEBUG);
                }
                log("Subscription Created: " + group.name +
                        " (hierarchyId=" + std::to_string(group.dataPointId) +
                        ") | ID: " + std::to_string(gsub.subscriptionId) +
                        " | Interval: " +
                        std::to_string(gsub.revisedPublishingInterval) + "ms",
                    LogLevel::INFO);
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
                        // Cache for hot reload (applyAdd builds node paths
                        // from ctx->resolvedNs + the API's short nodeId).
                        ctx->resolvedNs.store(resolvedNs,
                                              std::memory_order_release);
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
                    {
                        std::lock_guard<std::mutex> regLk(ctx->registryMutex);
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
                                // Record live monitor for later hot-reload
                                // delete/update operations.
                                LiveMonitorEntry entry;
                                entry.hierarchyId =
                                    batchInfoSpaces[i].hierarchyId;
                                entry.tagId = batchInfoSpaces[i].tagId;
                                entry.monitoredItemId =
                                    resp.results[i].monitoredItemId;
                                entry.subscriptionId = sub.subscriptionId;
                                entry.groupName = groupName;
                                entry.endpointUrl = ctx->endpoint;
                                entry.infoSpace = batchInfoSpaces[i];
                                ctx->liveRegistry[entry.hierarchyId]
                                    .push_back(std::move(entry));
                            }
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
        return context;
    };

    // Publish the factory so the hot reload worker can spawn additional
    // ClientContexts at runtime (purpose=OPC_HI_SERVER, action=Add).
    g_buildContext = buildAndStartContext;

    for(const auto &server : serverList) {
        auto ctx = buildAndStartContext(server);
        if(ctx) {
            std::lock_guard<std::mutex> lk(g_clientPoolMutex);
            clientPool[ctx->endpoint] = ctx.get();
            clientContexts.push_back(std::move(ctx));
        } else {
            log("Skipping server '" + server.name +
                    "' (" + server.endpointUrl + "): setup failed",
                LogLevel::ERRORS);
        }
        // ADD THIS: Give each thread time to initialize before starting the next
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    log("Client pool initialized. Press Ctrl+C to stop...");

    // ── Hot reload worker thread ───────────────────────────────────────────
    // Drains g_hrQueue (populated from the MQTT callback when topic ==
    // "HTRLD/Edgents"). Each task is a self-contained lambda that calls the
    // EdgentHotReloading API and dispatches to applyAdd/Delete/Update/Reload.
    g_hrRunning.store(true, std::memory_order_release);
    g_hrWorker = std::thread([]() {
        log("HotReload worker thread started", LogLevel::INFO);
        while(true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lk(g_hrMutex);
                g_hrCv.wait(lk, [] {
                    return !g_hrQueue.empty() ||
                           !g_hrRunning.load(std::memory_order_acquire);
                });
                if(!g_hrRunning.load(std::memory_order_acquire) &&
                   g_hrQueue.empty())
                    break;
                task = std::move(g_hrQueue.front());
                g_hrQueue.pop();
            }
            try {
                task();
            } catch(const std::exception &e) {
                log(std::string("HotReload task threw: ") + e.what(),
                    LogLevel::ERRORS);
            } catch(...) {
                log("HotReload task threw unknown exception",
                    LogLevel::ERRORS);
            }
        }
        log("HotReload worker thread exiting", LogLevel::INFO);
    });

    g_mqttHandler->setCallback([&clientPool, &clientContexts,
                                applicationEndURLHost,
                                applicationEndURLPort, &BearerToken,
                                authUsername, authPassword](
                                   const std::string &topic,
                                   const std::string &payload) {
        //log("Received MQTT message on topic: " + topic, LogLevel::DEBUG);

        // Parse and validate JSON payload
        if (payload.empty()) {
            log("Empty payload received on topic: " + topic, LogLevel::INFO);
            return;
        }

        // ── Hot reload trigger ────────────────────────────────────────────
        // Schema: {
        //   id, orgId, Reference, purpose, action,
        //   item_collection: [id, ...]   // optional, used by Delete
        // }
        // We hand off to the hot reload worker so the MQTT thread isn't
        // blocked on a synchronous HTTP call + UA dispatch.
        if(topic == "HTRLD/Edgents") {
            json hr;
            try {
                hr = json::parse(payload);
            } catch(const std::exception &e) {
                log("HotReload: invalid JSON: " + std::string(e.what()),
                    LogLevel::ERRORS);
                return;
            }

            HotReloadCommand cmd;
            // Parse fields case-insensitively: accept both PascalCase
            // (e.g. "OrgId") and camelCase (e.g. "orgId") from MQTT.
            cmd.orgId = hr.contains("OrgId")    ? hr.value("OrgId", 0)
                       : hr.value("orgId", 0);
            cmd.reference = hr.contains("Reference") ? hr.value("Reference", std::string{})
                           : hr.value("reference", std::string{});
            cmd.purpose = hr.contains("Purpose") ? hr.value("Purpose", std::string{})
                         : hr.value("purpose", std::string{});
            cmd.action = hr.contains("Action") ? hr.value("Action", std::string{})
                        : hr.value("action", std::string{});

            // Delete carries a list of ids under "ItemCollection" /
            // "item_collection". For Delete we ONLY use these IDs and
            // ignore the top-level Id field entirely.
            const char *collKey =
                hr.contains("ItemCollection")   ? "ItemCollection"
              : hr.contains("item_collection")  ? "item_collection"
              : nullptr;
            if(collKey && hr[collKey].is_array()) {
                for(const auto &x : hr[collKey]) {
                    if(x.is_number_integer())
                        cmd.ids.push_back(x.get<int>());
                }
            }

            const bool isDelete = cmd.action == "Delete";

            // Add / Update / Reload use the single "Id"/"id" field when
            // ItemCollection is absent. Delete never uses it.
            if(!isDelete && cmd.ids.empty()) {
                const char *idKey =
                    hr.contains("Id") ? "Id"
                  : (hr.contains("id") ? "id" : nullptr);
                if(idKey) {
                    if(hr[idKey].is_number_integer()) {
                        cmd.ids.push_back(hr[idKey].get<int>());
                    } else if(hr[idKey].is_array()) {
                        for(const auto &x : hr[idKey]) {
                            if(x.is_number_integer())
                                cmd.ids.push_back(x.get<int>());
                        }
                    }
                }
            }

            // Reload acts on the entire client — id is not required.
            // Delete does not require purpose (it tries all types).
            const bool isReload = cmd.action == "Reload";
            if(cmd.action.empty()) {
                log("HotReload: missing action field",
                    LogLevel::ERRORS);
                return;
            }
            if(!isReload && !isDelete &&
               (cmd.purpose.empty() || cmd.ids.empty())) {
                log("HotReload: missing required fields "
                    "(purpose/id) for action=" + cmd.action,
                    LogLevel::ERRORS);
                return;
            }
            if(isDelete && cmd.ids.empty()) {
                log("HotReload: Delete requires ItemCollection with ids",
                    LogLevel::ERRORS);
                return;
            }

            const std::string apiHost = applicationEndURLHost;
            const std::string apiPort = std::to_string(applicationEndURLPort);

            // Build the API payload with PascalCase keys as required by
            // /api/EdgentHotReloading (e.g. "Id", "OrgId", "Purpose").
            json commandBody;
            commandBody["Id"]        = cmd.firstId();
            commandBody["OrgId"]     = cmd.orgId;
            commandBody["Reference"] = cmd.reference;
            commandBody["Purpose"]   = cmd.purpose;
            commandBody["Action"]    = cmd.action;

            postHotReloadTask([&clientPool, &clientContexts, apiHost, apiPort,
                               &BearerToken, authUsername, authPassword,
                               commandBody, cmd]() {
                std::string idsStr;
                for(size_t i = 0; i < cmd.ids.size(); ++i) {
                    if(i) idsStr += ',';
                    idsStr += std::to_string(cmd.ids[i]);
                }
                log("HotReload: action=" + cmd.action +
                        " purpose=" + cmd.purpose +
                        " ids=[" + idsStr + "]",
                    LogLevel::INFO);

                // ── Reload (no purpose dependency): restart EVERY connected
                //    server context.
                if(cmd.action == "Reload" && cmd.reference == "OPCUA") {
                    std::vector<ClientContext *> all;
                    {
                        std::lock_guard<std::mutex> lk(g_clientPoolMutex);
                        all.reserve(clientPool.size());
                        for(const auto &kv : clientPool)
                            if(kv.second) all.push_back(kv.second);
                    }
                    if(all.empty()) {
                        log("HotReload Reload: client pool is empty",
                            LogLevel::WARNING);
                        return;
                    }
                    for(ClientContext *ctx : all)
                        applyReload(ctx, cmd, {});
                    return;
                }

                // ── Delete (purpose-agnostic) ─────────────────────────────
                // Delete ignores `purpose`. For each id in ItemCollection,
                // try server → group → datapoint across all contexts.
                if(cmd.action == "Delete") {
                    for(int id : cmd.ids) {
                        // Try server-level delete first (may destroy a ctx).
                        applyDeleteServer(id, clientPool, clientContexts);
                        // Re-snapshot pool AFTER server delete so we never
                        // dereference a freed context pointer.
                        std::vector<ClientContext *> alive;
                        {
                            std::lock_guard<std::mutex> lk(g_clientPoolMutex);
                            alive.reserve(clientPool.size());
                            for(const auto &kv : clientPool)
                                if(kv.second) alive.push_back(kv.second);
                        }
                        // Then try group and datapoint on every surviving context.
                        for(ClientContext *ctx : alive) {
                            applyDeleteGroup(ctx, id);
                            applyDeleteSingle(ctx, id);
                        }
                    }
                    return;
                }

                // ── Server-level Add / Update ──────────────────────────────
                if(cmd.purpose == "OPC_HI_SERVER") {
                    if(cmd.action == "Add") {
                        applyAddServer(apiHost, apiPort, BearerToken,
                                       authUsername, authPassword,
                                       commandBody, clientPool,
                                       clientContexts);
                    } else if(cmd.action == "Update") {
                        for(int id : cmd.ids) {
                            ClientContext *ctx = nullptr;
                            {
                                std::lock_guard<std::mutex> lk(
                                    g_clientPoolMutex);
                                ctx = findServerByHierarchyId_locked(
                                    id, clientPool);
                            }
                            if(!ctx) {
                                log("HotReload UpdateServer: no server with "
                                    "hierarchyId=" + std::to_string(id),
                                    LogLevel::WARNING);
                                continue;
                            }
                            HotReloadCommand sub = cmd;
                            sub.ids = {id};
                            applyReload(ctx, sub, {});
                        }
                    } else {
                        log("HotReload: unknown action '" + cmd.action +
                                "' for OPC_HI_SERVER",
                            LogLevel::ERRORS);
                    }
                    return;
                }

                // ── Group / Datapoint Add / Update ────────────────────────
                auto targets = resolveHotReloadTargets(cmd, clientPool);
                if(targets.empty()) {
                    log("HotReload: no connected target for purpose=" +
                            cmd.purpose,
                        LogLevel::ERRORS);
                    return;
                }

                // Add and Update need the API payload (tag definitions or
                // updated subscription params).
                json resp =
                    callHotReloadApiWithTokenRefresh(apiHost, apiPort, BearerToken,
                                                     authUsername, authPassword,
                                                     commandBody);
                if(resp.is_null()) {
                    log("HotReload: API call returned no usable payload for "
                        "action=" + cmd.action + " ids=[" + idsStr +
                            "] — aborting dispatch",
                        LogLevel::ERRORS);
                    return;
                }
                std::vector<HotReloadItem> items =
                    parseHotReloadResponse(resp);
                if(items.empty()) {
                    log("HotReload: API returned no items for " + cmd.action +
                            " ids=[" + idsStr + "]",
                        LogLevel::WARNING);
                    return;
                }

                for(ClientContext *ctx : targets) {
                    if(cmd.action == "Add") {
                        if(cmd.purpose == "OPC_HI_GROUP")
                            applyAddGroup(ctx, cmd, items);
                        else
                            applyAdd(ctx, cmd, items);
                    } else if(cmd.action == "Update") {
                        if(cmd.purpose == "OPC_HI_GROUP")
                            applyGroupUpdate(ctx, cmd, items);
                        else
                            applyUpdate(ctx, cmd, items);
                    } else {
                        log("HotReload: unknown action '" + cmd.action + "'",
                            LogLevel::ERRORS);
                    }
                }
            });
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
        ClientContext *context = nullptr;
        {
            std::lock_guard<std::mutex> lk(g_clientPoolMutex);
            auto it = clientPool.find(endpoint);
            if(it != clientPool.end() && it->second &&
               it->second->isConnected.load(std::memory_order_acquire)) {
                context = it->second;
            }
        }
        if(!context) {
            log("Client not connected for endpoint: " + endpoint, LogLevel::ERRORS);
            return;
        }

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

    // Stop the hot reload worker BEFORE tearing down the client pool: queued
    // tasks reference clientPool / ClientContext pointers that are about to
    // be destroyed.
    g_hrRunning.store(false, std::memory_order_release);
    g_hrCv.notify_all();
    if(g_hrWorker.joinable())
        g_hrWorker.join();

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
