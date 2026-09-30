/* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
 * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

 #include <open62541/client_config_default.h>
 #include <open62541/client_highlevel.h>
 #include <open62541/client_highlevel_async.h> // UA_Client_sendAsyncReadRequest (polling)
 #include <open62541/client_subscriptions.h>
 #include <open62541/plugin/log_stdout.h>
 #include <open62541/plugin/create_certificate.h>
 
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
// NOTE: the former hardcoded APIusername/APIpassword fallback was removed
// (C-10). Servers that omit credentials now fall back to the EdgeConfig
// identity (see buildAndStartContext).

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

// Poll interval for quiet tags (CLIENT_POLLING_PLAN.md). Set once from
// appsettings.json (Polling.PollIntervalSeconds) before any ClientContext
// starts. 0 = polling disabled.
static std::atomic<int>                    g_pollIntervalSec{60};

// Forward declaration: a configurable factory that produces a fully-
// configured-and-started ClientContext for a given ServerInfoO. Initialized
// in runClient() once all per-process state (BearerToken, EdgeConfig,
// applicationEndURL, etc.) is loaded so that applyAddServer can reuse the
// exact same setup pipeline used at startup.
struct ClientContext; // defined below
static std::function<std::unique_ptr<ClientContext>(const ServerInfoO &)>
    g_buildContext;

// Re-creates every monitored item that was added at runtime via hot reload
// (ClientContext::hotAdded) after a (re)connect. Defined after the hot reload
// dispatchers below; invoked from the OPC thread right after onConnected().
static void replayHotAddedMonitors(ClientContext *ctx);

// Reads every registered tag that has been quiet for a full poll interval and
// publishes the result (CLIENT_POLLING_PLAN.md). Called from the OPC thread
// loop after each run_iterate; returns immediately when no poll is due.
static void runPollCycle(ClientContext *ctx);

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
// Exit code of runClient() when running as a service, so ServiceMain can
// report a real failure to SCM (C-22).
static std::atomic<int> g_serviceWorkerExitCode{0};

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

    // `subscriptions` and `groupIdToName` are written on the OPC thread
    // (onConnected, AddGroup/DeleteGroup/GroupUpdate tasks, reconnect
    // teardown) and read on the hot-reload worker. Every access MUST hold
    // subsMutex (C-02). Keep the critical sections short - never hold it
    // across a UA_Client_* call that may block on the network.
    std::mutex subsMutex;
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
    // Tasks are microsecond-scale (build a JSON payload, enqueue). 4 per
    // server is plenty; 32 was 33 threads per connected server (C-26).
    static constexpr int WORKER_COUNT = 4;
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
    // Each entry OWNS its MyMonitorContext (shared_ptr) - see structs.h.
    std::mutex registryMutex;
    std::unordered_map<int, std::vector<LiveMonitorEntry>> liveRegistry;

    // Datapoints added at runtime via hot reload. onConnected only replays the
    // startup hierarchy, so these are re-created separately after every
    // (re)connect (C-06). Guarded by registryMutex.
    std::vector<HotAddedMonitor> hotAdded;

    // Maps a group's hierarchy id to its `subscriptions` key (group.name) so
    // hot reload's applyGroupUpdate can look up the OPC UA subscriptionId
    // from the MQTT command's `id`. Guarded by subsMutex.
    std::unordered_map<int, std::string> groupIdToName;

    std::function<UA_StatusCode()> connectOnce;
    std::function<void()> onConnected;
    static constexpr size_t MAX_OPC_TASKS_PER_TICK = 256;

    // Polling (CLIENT_POLLING_PLAN.md). nextPollAt is OPC-thread only; a
    // default value means "schedule one interval from now". pollsInFlight is
    // shared with outstanding read requests (each holds a reference), so their
    // callbacks can decrement it safely even while this context is torn down.
    std::chrono::steady_clock::time_point nextPollAt{};
    std::shared_ptr<std::atomic<int>> pollsInFlight =
        std::make_shared<std::atomic<int>>(0);

    ClientContext() : running(true), isConnected(false) {}

    // Drop all per-session state that a fresh connect will rebuild:
    // subscriptions, group id map, resolved namespace, and every live
    // monitor registry entry (which frees the MyMonitorContexts - C-05).
    //
    // MUST be called on the OPC thread, and only when no UA_Client callback
    // can run concurrently: i.e. after UA_Client_disconnect()/reset() or
    // before the next connectOnce(). Queued worker tasks keep their own
    // shared_ptr to a context, so freeing here can never dangle them.
    //
    // Does NOT touch Mapping/TopicMapping or hotAdded: onConnected and
    // replayHotAddedMonitors need them to rebuild the session.
    void resetSessionState() {
        {
            std::lock_guard<std::mutex> lk(subsMutex);
            subscriptions.clear();
            groupIdToName.clear();
        }
        resolvedNs.store(0, std::memory_order_release);
        // First poll of the next session is one interval after it connects
        // (the new subscriptions deliver every current value on creation).
        nextPollAt = std::chrono::steady_clock::time_point{};
        std::lock_guard<std::mutex> regLk(registryMutex);
        liveRegistry.clear();
    }

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
                        // Whatever session state is left from the previous
                        // connection is dead now (the old UA_Client is either
                        // disconnected or about to be replaced inside
                        // connectOnce). Free it so monitor contexts don't
                        // leak across reconnects (C-05).
                        resetSessionState();

                        UA_StatusCode rc = connectOnce();
                        if(rc == UA_STATUSCODE_GOOD) {
                            isConnected.store(true, std::memory_order_release);
                            if(onConnected) onConnected();
                            // Re-create anything added via hot reload since
                            // startup (C-06). Runs on this thread, after the
                            // startup monitors have been queued.
                            replayHotAddedMonitors(this);
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
                    resetSessionState();
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                    continue;
                }

                // ── Poll quiet tags (no-op unless an interval has elapsed) ───
                runPollCycle(this);

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

// open62541 log messages may use custom conversion specifiers (%N for NodeId,
// %S for UA_String, ...) that the CRT's vsnprintf does not understand; on
// MSVC an unknown specifier trips an invalid-parameter assertion (the server
// hit exactly this). Returns true only if every '%' in `fmt` is a standard C
// conversion that vsnprintf can safely expand.
static bool
formatIsCrtSafe(const char *fmt) {
    for(const char *p = fmt; *p; ++p) {
        if(*p != '%')
            continue;
        ++p;
        if(*p == '%')
            continue; // literal percent
        // flags / width / precision
        while(*p && strchr("-+ #0", *p)) ++p;
        while(*p && (isdigit((unsigned char)*p) || *p == '.' || *p == '*')) ++p;
        // length modifiers
        while(*p && strchr("hlLqjzt", *p)) ++p;
        if(!*p || !strchr("diouxXeEfFgGcsp", *p))
            return false;
    }
    return true;
}

// Custom logger callback for open62541 (C-13). Formats only CRT-safe format
// strings; otherwise logs the raw format string, which still identifies the
// event. Wrapped in try/catch because this is a C callback boundary - a C++
// exception escaping it would terminate the process.
static void
myLog(void *context, UA_LogLevel level, UA_LogCategory category, const char *msg,
      va_list args) {
    try {
        if(!msg)
            return;

        std::string text;
        if(formatIsCrtSafe(msg)) {
            char buffer[1024];
            vsnprintf(buffer, sizeof(buffer), msg, args);
            text = buffer;
        } else {
            text = msg;
        }

        switch(level) {
            case UA_LOGLEVEL_FATAL:
            case UA_LOGLEVEL_ERROR:
                log(text, LogLevel::ERRORS);
                break;
            case UA_LOGLEVEL_WARNING:
            case UA_LOGLEVEL_INFO:
                log(text, LogLevel::INFO);
                break;
            case UA_LOGLEVEL_DEBUG:
            default:
                // Silenced library debug logs
                break;
        }
    } catch(...) {
        // Never let an exception unwind across the C boundary.
    }
}

// Custom logger plugin
static UA_Logger myLogger = {myLog, nullptr, nullptr};

/**
 * Extract the host portion from an OPC UA endpoint URL like
 * "opc.tcp://192.168.1.23:4840" → "192.168.1.23".
 * Returns "" if nothing usable remains.
 */
static std::string
ExtractHostFromUrl(const std::string &rawUrl) {
    std::string host = rawUrl;
    // Trim whitespace
    while(!host.empty() && std::isspace((unsigned char)host.front())) host.erase(host.begin());
    while(!host.empty() && std::isspace((unsigned char)host.back())) host.pop_back();
    // Strip a leading scheme like "opc.tcp://"
    size_t schemePos = host.find("://");
    if(schemePos != std::string::npos)
        host = host.substr(schemePos + 3);
    // Strip anything from the first ':' (port) or '/' (path) onward
    size_t cutPos = host.find_first_of(":/");
    if(cutPos != std::string::npos)
        host = host.substr(0, cutPos);
    return host;
}

/** Current UTC time as "YYYY-MM-DDTHH:MM:SS.mmmZ" (C-32). */
static std::string
currentUtcIso8601() {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const std::time_t t = system_clock::to_time_t(now);
    const int ms = (int)(duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
             tm.tm_sec, ms);
    return buf;
}

/**
 * True if `host` is a plain dotted-quad IPv4 address (e.g. "192.168.1.23"),
 * as opposed to a DNS hostname — used to pick "IP:" vs "DNS:" for a cert SAN entry.
 */
static bool
LooksLikeIpv4Address(const std::string &host) {
    if(host.empty()) return false;
    int dotCount = 0;
    for(char c : host) {
        if(c == '.') { dotCount++; continue; }
        if(!std::isdigit((unsigned char)c)) return false;
    }
    return dotCount == 3;
}


// Sign in with up to 3 attempts, 10 s apart. Returns "" if all attempts fail.
static std::string
fetchBearerTokenWithRetry(const std::string &host, const std::string &port,
                          const std::string &username, const std::string &password) {
    const int retryDelay = 10; // seconds
    const int maxRetries = 3;  // Max retries before giving up (for offline fallback)

    for(int attempt = 1; attempt <= maxRetries; ++attempt) {
        try {
            json tokenResponse = ::getBearerToken(host, port, username, password);
            if(tokenResponse.contains("access_token")) {
                std::cout << "Bearer token fetched successfully!" << std::endl;
                return tokenResponse["access_token"].get<std::string>();
            }
            std::cout << "Invalid token response - retrying in " << retryDelay
                      << " seconds..." << std::endl;
        } catch(const std::exception &e) {
            std::cout << "Token fetch failed (attempt " << attempt << "/" << maxRetries
                      << "): " << e.what() << std::endl;
        }
        if(attempt < maxRetries)
            std::this_thread::sleep_for(std::chrono::seconds(retryDelay));
    }

    std::cout << "WARNING: All " << maxRetries
              << " token attempts failed. Proceeding without token (DB fallback mode)."
              << std::endl;
    return {};
}

// C-17: previously captured its by-value parameters BY REFERENCE into a
// detached std::async, which dangles the moment this function returns when
// blocking == false. Everything is now captured by value; the blocking path
// runs inline (no thread hop) and returns an already-satisfied future.
std::future<std::string>
getBearerTokenNow(std::string applicationEndURLHost, std::string applicationEndURLPort,
                  std::string authUsername, std::string authPassword,
                  bool blocking = true) {
    if(blocking) {
        std::promise<std::string> p;
        p.set_value(fetchBearerTokenWithRetry(applicationEndURLHost, applicationEndURLPort,
                                              authUsername, authPassword));
        return p.get_future();
    }

    return std::async(std::launch::async,
                      [host = std::move(applicationEndURLHost),
                       port = std::move(applicationEndURLPort),
                       user = std::move(authUsername),
                       pass = std::move(authPassword)]() {
                          return fetchBearerTokenWithRetry(host, port, user, pass);
                      });
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

// Parse "ns=<n>;i=<num>" / "ns=<n>;s=<str>" into a UA_NodeId. Returns
// UA_NODEID_NULL on anything else. String ids are heap-allocated; the caller
// must UA_NodeId_clear() the result.
static UA_NodeId
parseUaNodePath(const std::string &p) {
    UA_NodeId nodeId = UA_NODEID_NULL;
    const char *nsStart = strstr(p.c_str(), "ns=");
    if(!nsStart) return nodeId;
    const char *nsEnd = strchr(nsStart + 3, ';');
    if(!nsEnd) return nodeId;
    UA_UInt16 nsI = (UA_UInt16)strtoul(nsStart + 3, nullptr, 10);
    const char *idStart = nsEnd + 1;
    if(strncmp(idStart, "i=", 2) == 0)
        nodeId = UA_NODEID_NUMERIC(nsI, (UA_UInt32)strtoul(idStart + 2, nullptr, 10));
    else if(strncmp(idStart, "s=", 2) == 0)
        nodeId = UA_NODEID_STRING_ALLOC(nsI, idStart + 2);
    return nodeId;
}

// Format a UA_NodeId as "ns=<n>;s=<str>" / "ns=<n>;i=<num>" for logging.
static std::string
uaNodeIdToString(const UA_NodeId &id) {
    std::string s = "ns=" + std::to_string(id.namespaceIndex) + ";";
    switch(id.identifierType) {
        case UA_NODEIDTYPE_NUMERIC:
            s += "i=" + std::to_string(id.identifier.numeric);
            break;
        case UA_NODEIDTYPE_STRING:
            s += "s=" + std::string((char *)id.identifier.string.data,
                                    id.identifier.string.length);
            break;
        case UA_NODEIDTYPE_GUID:
            s += "g=<guid>";
            break;
        case UA_NODEIDTYPE_BYTESTRING:
            s += "b=<bytestring>";
            break;
        default:
            s += "?";
            break;
    }
    return s;
}

// ── Polling (CLIENT_POLLING_PLAN.md) ─────────────────────────────────────────
//
// Every registered tag is also polled: once per poll interval, the tags that
// published nothing during the last interval are read and published. The
// poll list is ctx->liveRegistry, which startup, hot reload and reconnect
// already maintain, so a deleted tag stops being polled and an added one
// starts without any polling-specific code in those paths.

// One outstanding Read request. ctxs[i] is the tag for nodesToRead[i]. The
// shared_ptrs keep the contexts (and their nodeIds) alive until the response
// or cancellation arrives, even if hot reload deletes the tag meanwhile.
struct PollBatch {
    std::string serverName;
    std::int64_t staleMs = 0;
    std::shared_ptr<std::atomic<int>> inFlight;
    std::vector<std::shared_ptr<MyMonitorContext>> ctxs;
};

// Nodes per Read request; servers commonly cap MaxNodesPerRead.
static constexpr size_t POLL_CHUNK = 500;

// open62541 calls this exactly once per sent request, on the OPC thread: with
// the response, or with an empty response carrying BadTimeout (after
// config->timeout) or a shutdown / connection-closed code when the request is
// cancelled by a disconnect. It must not touch the ClientContext.
static void
onPollReadResponse(UA_Client *, void *userdata, UA_UInt32, UA_ReadResponse *rr) {
    std::unique_ptr<PollBatch> batch(static_cast<PollBatch *>(userdata));
    batch->inFlight->fetch_sub(1, std::memory_order_acq_rel);

    const UA_StatusCode sr = rr->responseHeader.serviceResult;
    if(sr != UA_STATUSCODE_GOOD) {
        // Cancellation by a disconnect is expected; the next session polls again.
        const bool cancelled = sr == UA_STATUSCODE_BADSHUTDOWN ||
                               sr == UA_STATUSCODE_BADCONNECTIONCLOSED ||
                               sr == UA_STATUSCODE_BADSECURECHANNELCLOSED ||
                               sr == UA_STATUSCODE_BADSESSIONCLOSED ||
                               sr == UA_STATUSCODE_BADSERVERNOTCONNECTED;
        log("Poll " + batch->serverName + ": read of " +
                std::to_string(batch->ctxs.size()) + " tags failed: " +
                UA_StatusCode_name(sr),
            cancelled ? LogLevel::DEBUG : LogLevel::WARNING);
        return;
    }

    const size_t n = std::min(rr->resultsSize, batch->ctxs.size());
    for(size_t i = 0; i < n; ++i)
        postValueForPublish(batch->ctxs[i], &rr->results[i], PublishOrigin::Poll,
                            batch->staleMs);
}

static void
runPollCycle(ClientContext *ctx) {
    const int intervalSec = g_pollIntervalSec.load(std::memory_order_relaxed);
    if(intervalSec <= 0 || !ctx->client ||
       !ctx->isConnected.load(std::memory_order_acquire))
        return;

    const auto now = std::chrono::steady_clock::now();
    if(ctx->nextPollAt == std::chrono::steady_clock::time_point{}) {
        // New session: its subscriptions just delivered every current value.
        ctx->nextPollAt = now + std::chrono::seconds(intervalSec);
        return;
    }
    if(now < ctx->nextPollAt)
        return;
    ctx->nextPollAt = now + std::chrono::seconds(intervalSec);

    // A slow server is still answering the previous poll: skip this round
    // instead of stacking reads on it.
    if(ctx->pollsInFlight->load(std::memory_order_acquire) > 0) {
        log("Poll " + ctx->name + ": previous poll still in flight, skipping",
            LogLevel::DEBUG);
        return;
    }

    // A tag is "quiet" if its last message is at least one interval old,
    // minus a grace period. Without the grace a quiet tag would be polled only
    // every OTHER cycle: its previous message (initial value on connect, or
    // the last poll result) always lands a little after that cycle started,
    // so one interval later it is a fraction of a second too young.
    // Grace = 10% of the interval, at most 5 s (60 s -> 55 s, 10 s -> 9 s).
    const std::int64_t intervalMs = static_cast<std::int64_t>(intervalSec) * 1000;
    const std::int64_t staleMs =
        intervalMs - std::min<std::int64_t>(5000, intervalMs / 10);
    const std::int64_t nowMs = steadyNowMs();
    std::vector<std::shared_ptr<MyMonitorContext>> stale;
    {
        std::lock_guard<std::mutex> lk(ctx->registryMutex);
        for(const auto &kv : ctx->liveRegistry) {
            for(const auto &entry : kv.second) {
                const std::shared_ptr<MyMonitorContext> &mc = entry.monCtx;
                if(!mc || UA_NodeId_isNull(&mc->nodeId))
                    continue;
                if(nowMs - mc->lastPublishMs.load(std::memory_order_relaxed) >= staleMs)
                    stale.push_back(mc);
            }
        }
    }

    if(stale.empty())
        return;

    for(size_t off = 0; off < stale.size(); off += POLL_CHUNK) {
        const size_t n = std::min(POLL_CHUNK, stale.size() - off);

        auto batch = std::make_unique<PollBatch>();
        batch->serverName = ctx->name;
        batch->staleMs = staleMs;
        batch->inFlight = ctx->pollsInFlight;
        batch->ctxs.assign(stale.begin() + off, stale.begin() + off + n);

        // NodeIds are borrowed from the contexts, not copied: the request is
        // encoded inside the send call and is never cleared here.
        std::vector<UA_ReadValueId> items(n);
        for(size_t i = 0; i < n; ++i) {
            UA_ReadValueId_init(&items[i]);
            items[i].nodeId = batch->ctxs[i]->nodeId;
            items[i].attributeId = UA_ATTRIBUTEID_VALUE;
        }
        UA_ReadRequest req;
        UA_ReadRequest_init(&req);
        req.timestampsToReturn = UA_TIMESTAMPSTORETURN_BOTH;
        req.nodesToRead = items.data();
        req.nodesToReadSize = n;

        ctx->pollsInFlight->fetch_add(1, std::memory_order_acq_rel);
        PollBatch *raw = batch.release();
        const UA_StatusCode rc = UA_Client_sendAsyncReadRequest(
            ctx->client.get(), &req, onPollReadResponse, raw, nullptr);
        if(rc != UA_STATUSCODE_GOOD) {
            // Not sent: open62541 does not invoke the callback, so clean up here.
            ctx->pollsInFlight->fetch_sub(1, std::memory_order_acq_rel);
            delete raw;
            log("Poll " + ctx->name + ": could not send read request: " +
                    UA_StatusCode_name(rc),
                LogLevel::WARNING);
            break;
        }
    }
}

// OPC-thread half of a hot-reload Add: create the monitored items for
// (infoSpaces[i], nodePaths[i]) on `targetSubId` and register the survivors in
// ctx->liveRegistry under `hierarchyId`. Shared by applyAdd (first creation)
// and replayHotAddedMonitors (re-creation after a reconnect). MUST run on the
// OPC thread (it uses ctx->client directly).
static void
createHotReloadMonitorsOnOpc(ClientContext *ctx, UA_UInt32 targetSubId,
                             const std::string &targetGroupName,
                             const std::vector<MappedInfospaceTag> &infoSpaces,
                             const std::vector<std::string> &nodePaths,
                             int hierarchyId, const char *logPrefix) {
    if(!ctx->client || !ctx->isConnected.load(std::memory_order_acquire)) {
        log(std::string(logPrefix) + ": client not connected for " + ctx->endpoint,
            LogLevel::ERRORS);
        return;
    }

    const size_t count = infoSpaces.size();
    if(count == 0)
        return;

    UA_CreateMonitoredItemsRequest batchReq;
    UA_CreateMonitoredItemsRequest_init(&batchReq);
    batchReq.subscriptionId     = targetSubId;
    batchReq.timestampsToReturn = UA_TIMESTAMPSTORETURN_BOTH;
    batchReq.itemsToCreate =
        (UA_MonitoredItemCreateRequest *)UA_Array_new(
            count, &UA_TYPES[UA_TYPES_MONITOREDITEMCREATEREQUEST]);
    batchReq.itemsToCreateSize = count;

    std::vector<std::shared_ptr<MyMonitorContext>> ctxs(count);
    std::vector<void *> rawCtxs(count, nullptr);
    std::vector<UA_Client_DataChangeNotificationCallback> cbs(
        count, handler_NodeValueChanged);
    std::vector<UA_Client_DeleteMonitoredItemCallback> delCbs(count, nullptr);

    for(size_t i = 0; i < count; ++i) {
        UA_NodeId nodeId = parseUaNodePath(nodePaths[i]);

        UA_MonitoredItemCreateRequest &req = batchReq.itemsToCreate[i];
        UA_MonitoredItemCreateRequest_init(&req);
        UA_NodeId_copy(&nodeId, &req.itemToMonitor.nodeId);
        UA_NodeId_clear(&nodeId);
        req.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
        req.monitoringMode = UA_MONITORINGMODE_REPORTING;
        // C-20: honour the per-tag backend settings (0 = server default,
        // which is what the old hardcoded values effectively requested).
        req.requestedParameters.samplingInterval =
            (UA_Double)(infoSpaces[i].samplingInterval > 0 ? infoSpaces[i].samplingInterval : 0);
        req.requestedParameters.queueSize =
            (UA_UInt32)(infoSpaces[i].queuesize > 0 ? infoSpaces[i].queuesize : 10000);
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

        ctxs[i] = std::make_shared<MyMonitorContext>();
        UA_NodeId_copy(&req.itemToMonitor.nodeId, &ctxs[i]->nodeId); // for polling
        ctxs[i]->infoSpace = infoSpaces[i];
        ctxs[i]->mqttHandler = g_mqttHandler;
        ctxs[i]->sqliteService = g_sqliteService;
        ctxs[i]->asyncPublisher = g_asyncPublisher;
        ctxs[i]->postWorkerTask = [ctx](std::function<void()> task) {
            ctx->postTask(std::move(task));
        };
        rawCtxs[i] = ctxs[i].get();
    }

    UA_CreateMonitoredItemsResponse resp =
        UA_Client_MonitoredItems_createDataChanges(
            ctx->client.get(), batchReq, rawCtxs.data(), cbs.data(), delCbs.data());

    UA_CreateMonitoredItemsRequest_clear(&batchReq);

    size_t ok = 0;
    {
        std::lock_guard<std::mutex> regLk(ctx->registryMutex);
        for(size_t i = 0; i < resp.resultsSize && i < count; ++i) {
            if(resp.results[i].statusCode != UA_STATUSCODE_GOOD) {
                log(std::string(logPrefix) + ": monitor failed for tagId " +
                        std::to_string(infoSpaces[i].tagId) + ": " +
                        UA_StatusCode_name(resp.results[i].statusCode),
                    LogLevel::ERRORS);
                // ctxs[i] is dropped when this vector goes out of scope.
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
                entry.monCtx = ctxs[i];
                ctx->liveRegistry[hierarchyId].push_back(std::move(entry));
            }
        }
    }
    UA_CreateMonitoredItemsResponse_clear(&resp);

    log(std::string(logPrefix) + ": hierarchy " + std::to_string(hierarchyId) +
            " on " + ctx->endpoint + ": " + std::to_string(ok) + "/" +
            std::to_string(count) + " monitors created",
        LogLevel::INFO);
}

// Pick the subscription a hot-reloaded datapoint should live on. Prefers the
// group named by `preferredGroup` (C-21: resolved from the item's parentId),
// then any group subscription. Returns false if none exists.
static bool
pickTargetSubscription(ClientContext *ctx, const std::string &preferredGroup,
                       UA_UInt32 &subIdOut, std::string &groupNameOut) {
    std::lock_guard<std::mutex> lk(ctx->subsMutex);
    if(!preferredGroup.empty()) {
        auto it = ctx->subscriptions.find(preferredGroup);
        if(it != ctx->subscriptions.end()) {
            subIdOut = it->second.subscriptionId;
            groupNameOut = it->first;
            return true;
        }
    }
    for(const auto &kv : ctx->subscriptions) {
        if(kv.first == ctx->name)
            continue; // legacy events subscription key, if present
        subIdOut = kv.second.subscriptionId;
        groupNameOut = kv.first;
        return true;
    }
    return false;
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

    // C-21: route the datapoint to its own group when the payload tells us
    // which one that is (parentId == the group's hierarchy id).
    std::string preferredGroup;
    for(const auto &it : items) {
        if(it.parentId.empty()) continue;
        int parentHierarchyId = 0;
        try { parentHierarchyId = std::stoi(it.parentId); } catch(...) { continue; }
        std::lock_guard<std::mutex> lk(ctx->subsMutex);
        auto git = ctx->groupIdToName.find(parentHierarchyId);
        if(git != ctx->groupIdToName.end()) {
            preferredGroup = git->second;
            break;
        }
    }

    UA_UInt32 targetSubId = 0;
    std::string targetGroupName;
    if(!pickTargetSubscription(ctx, preferredGroup, targetSubId, targetGroupName)) {
        log("HotReload Add: no group subscription available on " +
                ctx->endpoint,
            LogLevel::ERRORS);
        return;
    }
    if(!preferredGroup.empty() && targetGroupName != preferredGroup) {
        log("HotReload Add: preferred group '" + preferredGroup +
                "' not active on " + ctx->endpoint + "; using '" +
                targetGroupName + "'",
            LogLevel::WARNING);
    }

    // Pre-build everything that does NOT need the UA_Client.
    // Each (item x mappedTagOption) pair becomes one monitor; if an item has
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

    // A valid OPC UA node path must start with "ns=" and contain a ";<i|s>=..."
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
        // (OPC_HI_GROUP / OPC_HI_SERVER) - those aren't monitorable.
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
                    "') - skipping",
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
            // The hot-reload API ships deadband as a PERCENT, but the monitor
            // filter is ABSOLUTE; applying it as-is would be wrong. Keep the
            // previous behaviour (no deadband) until the API exposes an
            // absolute value.
            info.deadband = 0.0;
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
                {
                    std::lock_guard<std::mutex> mlk(g_mappingMutex);
                    Mapping[tagId] = {path, ctx->endpoint};
                    if(!opt.namespaces.empty())
                        TopicMapping[tagId] = opt.namespaces;
                }
                if(!opt.namespaces.empty())
                    newTopics.push_back(opt.namespaces);
                infoSpaces.push_back(
                    buildInfo(opt.id, tagId, opt.namespaces, opt.orgId));
                nodePaths.push_back(path);
            }
        } else {
            // Fallback: no mappedTagOptions in payload -> use dataPointId / id.
            const int tagId = (it.dataPointId != 0) ? it.dataPointId : it.id;
            {
                std::lock_guard<std::mutex> mlk(g_mappingMutex);
                Mapping[tagId] = {path, ctx->endpoint};
            }
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

    // C-06: remember these so they are re-created after a reconnect/Reload.
    // Recorded up front (not on OPC success) so a transient failure now still
    // gets retried on the next connect.
    {
        const int hierarchyId = cmd.firstId();
        std::lock_guard<std::mutex> regLk(ctx->registryMutex);
        for(size_t i = 0; i < infoSpaces.size(); ++i) {
            // Replace any older record for the same tagId (Update = Delete+Add).
            const int tagId = infoSpaces[i].tagId;
            ctx->hotAdded.erase(
                std::remove_if(ctx->hotAdded.begin(), ctx->hotAdded.end(),
                               [tagId](const HotAddedMonitor &h) {
                                   return h.infoSpace.tagId == tagId;
                               }),
                ctx->hotAdded.end());
            HotAddedMonitor h;
            h.hierarchyId = hierarchyId;
            h.nodePath = nodePaths[i];
            h.groupName = targetGroupName;
            h.infoSpace = infoSpaces[i];
            ctx->hotAdded.push_back(std::move(h));
        }
    }

    ctx->postOpcTask([ctx, targetSubId, targetGroupName,
                      infoSpaces = std::move(infoSpaces),
                      nodePaths  = std::move(nodePaths),
                      hierarchyId = cmd.firstId()]() mutable {
        createHotReloadMonitorsOnOpc(ctx, targetSubId, targetGroupName, infoSpaces,
                                     nodePaths, hierarchyId, "HotReload Add");
    });
}

// C-06: after onConnected has rebuilt the startup subscriptions, re-create
// every datapoint that was added via hot reload since startup. Runs on the
// OPC thread (called from startLoop right after onConnected). The monitors
// are grouped by their original subscription name; if that group no longer
// exists, any available group is used.
static void
replayHotAddedMonitors(ClientContext *ctx) {
    if(!ctx) return;

    std::vector<HotAddedMonitor> snapshot;
    {
        std::lock_guard<std::mutex> regLk(ctx->registryMutex);
        snapshot = ctx->hotAdded;
    }
    if(snapshot.empty())
        return;

    log("HotReload Replay: re-creating " + std::to_string(snapshot.size()) +
            " hot-added monitor(s) on " + ctx->endpoint,
        LogLevel::INFO);

    // Group by (preferred group, hierarchyId) so each batch lands on one
    // subscription and one registry key, exactly as the original Add did.
    std::map<std::pair<std::string, int>, std::vector<size_t>> buckets;
    for(size_t i = 0; i < snapshot.size(); ++i)
        buckets[{snapshot[i].groupName, snapshot[i].hierarchyId}].push_back(i);

    for(const auto &bucket : buckets) {
        UA_UInt32 subId = 0;
        std::string groupName;
        if(!pickTargetSubscription(ctx, bucket.first.first, subId, groupName)) {
            log("HotReload Replay: no group subscription available on " +
                    ctx->endpoint + " - " + std::to_string(bucket.second.size()) +
                    " monitor(s) not restored",
                LogLevel::ERRORS);
            continue;
        }
        std::vector<MappedInfospaceTag> infoSpaces;
        std::vector<std::string> nodePaths;
        infoSpaces.reserve(bucket.second.size());
        nodePaths.reserve(bucket.second.size());
        for(size_t idx : bucket.second) {
            infoSpaces.push_back(snapshot[idx].infoSpace);
            nodePaths.push_back(snapshot[idx].nodePath);
        }
        // Queue rather than run inline so it lands after the startup batches
        // that onConnected has already queued.
        ctx->postOpcTask([ctx, subId, groupName, infoSpaces = std::move(infoSpaces),
                          nodePaths = std::move(nodePaths),
                          hierarchyId = bucket.first.second]() mutable {
            createHotReloadMonitorsOnOpc(ctx, subId, groupName, infoSpaces, nodePaths,
                                         hierarchyId, "HotReload Replay");
        });
    }
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
    {
        std::lock_guard<std::mutex> lk(ctx->subsMutex);
        if(ctx->subscriptions.find(groupName) != ctx->subscriptions.end()) {
            log("HotReload AddGroup: subscription '" + groupName +
                    "' already exists on " + ctx->endpoint + " — skipping",
                LogLevel::WARNING);
            return;
        }
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
            UA_Client_Subscriptions_create(ctx->client.get(), greq, ctx, nullptr, nullptr);
        if(gsub.responseHeader.serviceResult != UA_STATUSCODE_GOOD) {
            log("HotReload AddGroup: subscription creation failed for '" +
                    groupName + "' on " + ctx->endpoint + ": " +
                    UA_StatusCode_name(gsub.responseHeader.serviceResult),
                LogLevel::ERRORS);
            return;
        }

        {
            std::lock_guard<std::mutex> lk(ctx->subsMutex);
            ctx->subscriptions[groupName] = gsub;
            if(hierarchyId != 0) {
                ctx->groupIdToName[hierarchyId] = groupName;
            }
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
    // queued. Also forget them for reconnect replay (C-06).
    {
        std::lock_guard<std::mutex> mlk(g_mappingMutex);
        for(const auto &e : snapshot) {
            Mapping.erase(e.tagId);
            TopicMapping.erase(e.tagId);
        }
    }
    {
        std::lock_guard<std::mutex> regLk(ctx->registryMutex);
        for(const auto &e : snapshot) {
            const int tagId = e.tagId;
            ctx->hotAdded.erase(
                std::remove_if(ctx->hotAdded.begin(), ctx->hotAdded.end(),
                               [tagId](const HotAddedMonitor &h) {
                                   return h.infoSpace.tagId == tagId;
                               }),
                ctx->hotAdded.end());
        }
    }

    // `snapshot` carries the owning shared_ptr for each MyMonitorContext. It
    // is released when this task's closure is destroyed - i.e. AFTER the
    // SDK has dropped the monitored item (deleteSingle is synchronous), so
    // no callback can still be scheduled with the raw pointer (C-05).
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
    UA_UInt32 subscriptionId = 0;
    {
        std::lock_guard<std::mutex> lk(ctx->subsMutex);
        auto it = ctx->groupIdToName.find(groupHierarchyId);
        if(it == ctx->groupIdToName.end()) {
            log("HotReload DeleteGroup: id " +
                    std::to_string(groupHierarchyId) + " unknown on " +
                    ctx->endpoint,
                LogLevel::DEBUG);
            return;
        }
        groupName = it->second;

        auto subIt = ctx->subscriptions.find(groupName);
        if(subIt == ctx->subscriptions.end()) {
            log("HotReload DeleteGroup: subscription '" + groupName +
                    "' not active on " + ctx->endpoint,
                LogLevel::WARNING);
            return;
        }
        subscriptionId = subIt->second.subscriptionId;
    }

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
    {
        std::lock_guard<std::mutex> mlk(g_mappingMutex);
        for(const auto &e : doomed) {
            Mapping.erase(e.tagId);
            TopicMapping.erase(e.tagId);
        }
    }
    {
        // Hot-added datapoints that lived on this group must not be replayed
        // after a reconnect either (C-06).
        std::lock_guard<std::mutex> regLk(ctx->registryMutex);
        for(const auto &e : doomed) {
            const int tagId = e.tagId;
            ctx->hotAdded.erase(
                std::remove_if(ctx->hotAdded.begin(), ctx->hotAdded.end(),
                               [tagId](const HotAddedMonitor &h) {
                                   return h.infoSpace.tagId == tagId;
                               }),
                ctx->hotAdded.end());
        }
    }

    // `doomed` owns the MyMonitorContexts; it is released with this closure,
    // after the subscription (and its items) are gone (C-05).
    ctx->postOpcTask([ctx, subscriptionId, groupName,
                      groupHierarchyId, doomed = std::move(doomed)]() {
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
        {
            std::lock_guard<std::mutex> lk(ctx->subsMutex);
            ctx->subscriptions.erase(groupName);
            ctx->groupIdToName.erase(groupHierarchyId);
        }
        const size_t removed = doomed.size();

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

        // 2. Wipe all derived state that onConnected will rebuild
        //    (subscriptions, group ids, namespace, live monitors + their
        //    contexts). Mapping/TopicMapping and hotAdded are deliberately
        //    kept: onConnected and replayHotAddedMonitors need them.
        size_t released = 0;
        {
            std::lock_guard<std::mutex> regLk(ctx->registryMutex);
            for(const auto &kv : ctx->liveRegistry)
                released += kv.second.size();
        }
        ctx->resetSessionState();

        // 3. LAST step: signal the OPC loop to reconnect on its next
        //    iteration. Order matters: any earlier flip would let the loop
        //    reconnect before the cleanup above completes.
        ctx->isConnected.store(false, std::memory_order_release);

        log("HotReload Reload: " + ctx->endpoint +
                " teardown complete; reconnect pending (released " +
                std::to_string(released) + " live monitors)",
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
    UA_CreateSubscriptionResponse current;
    {
        std::lock_guard<std::mutex> lk(ctx->subsMutex);
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
        current = subIt->second;
    }

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
            {
                std::lock_guard<std::mutex> lk(ctx->subsMutex);
                auto sIt = ctx->subscriptions.find(groupName);
                if(sIt != ctx->subscriptions.end()) {
                    sIt->second.revisedPublishingInterval =
                        mresp.revisedPublishingInterval;
                    sIt->second.revisedMaxKeepAliveCount =
                        mresp.revisedMaxKeepAliveCount;
                    sIt->second.revisedLifetimeCount =
                        mresp.revisedLifetimeCount;
                }
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
    // will be able to clean up after itself. The registry itself (and the
    // monitor contexts it owns) is released below, after the OPC thread has
    // been joined and the client disconnected (C-05).
    {
        std::lock_guard<std::mutex> regLk(owned->registryMutex);
        std::lock_guard<std::mutex> mlk(g_mappingMutex);
        for(const auto &kv : owned->liveRegistry) {
            for(const auto &e : kv.second) {
                Mapping.erase(e.tagId);
                TopicMapping.erase(e.tagId);
            }
        }
        for(const auto &h : owned->hotAdded) {
            Mapping.erase(h.infoSpace.tagId);
            TopicMapping.erase(h.infoSpace.tagId);
        }
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
    // Threads are joined and the client is disconnected: no callback can run
    // any more, so releasing the monitor contexts is safe now.
    owned->resetSessionState();

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
    if(!ctx) return false;
    std::lock_guard<std::mutex> lk(ctx->subsMutex);
    return ctx->groupIdToName.find(id) != ctx->groupIdToName.end();
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
                std::string mappedEndpoint;
                {
                    std::lock_guard<std::mutex> mlk(g_mappingMutex);
                    auto mit = Mapping.find(delId);
                    if(mit == Mapping.end())
                        continue;
                    mappedEndpoint = mit->second.second;
                }
                auto cit = clientPool.find(mappedEndpoint);
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


        // Extract AppSettings. (ApplicationEndURL is Host:Port and is not read
        // separately - Host/Port are the source of truth for every URL we build.)
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

        // Polling of quiet tags (CLIENT_POLLING_PLAN.md P-01). Optional: files
        // without a "Polling" section keep working with the 60 s default.
        // 0 (or negative) disables polling.
        {
            int pollSec = 60;
            try {
                if(config.contains("Polling") && config["Polling"].is_object() &&
                   config["Polling"].contains("PollIntervalSeconds"))
                    pollSec = config["Polling"]["PollIntervalSeconds"].get<int>();
            } catch(const std::exception &e) {
                std::cerr << "[Config] Invalid Polling.PollIntervalSeconds (" << e.what()
                          << "), using 60" << std::endl;
                pollSec = 60;
            }
            g_pollIntervalSec.store(pollSec > 0 ? pollSec : 0, std::memory_order_relaxed);
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


    // C-27: the "Do you want to start?" / "Client Started" modal prompts were
    // removed. They blocked any non-interactive launch (installer post-install
    // run, scheduled task, RDP session without a desktop) and the "Started"
    // box appeared before anything had actually started.

    // Logging: file logging is always on (C-28) so field logs exist for
    // support; --debug additionally echoes to the console.
    g_debug = cliOptions.debug;
    g_logging_enabled = true;

    // Initialize logging with client-specific folder
    std::string logFolder = "logs/client";
    std::string logAppName = "client";
    if(!cliOptions.instanceId.empty()) {
        logFolder = "logs/client_" + cliOptions.instanceId;
        logAppName = "client_" + cliOptions.instanceId;
    }
    init_logging(logFolder, logAppName, true);
    log("Client logging initialized with day-wise log files", LogLevel::INFO);
    if(!isService && !g_debug) {
        // Everything from here on is written to the log file; without this
        // line the console looks "stuck" after the bearer-token message.
        std::cout << "Client starting. Progress is logged to " << logFolder
                  << "/<date>.log (run with --debug to echo it here)." << std::endl;
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

    {
        const int pollSec = g_pollIntervalSec.load(std::memory_order_relaxed);
        log(pollSec > 0 ? "Polling quiet tags every " + std::to_string(pollSec) + " s"
                        : std::string("Polling disabled (Polling.PollIntervalSeconds = 0)"),
            LogLevel::INFO);
    }

    // Initialize SqliteQueueService EARLY so we can use config cache during hierarchy fetch
    try {
        OfflineQueueOptions options;
        // C-19: these come from appsettings.json (Payload.OfflineQueueOptions).
        options.batchSize = (retryBatchSize > 0) ? retryBatchSize : 100;
        options.uploadIntervalSeconds = 15;
        options.retentionDays = (retentionDays > 0) ? retentionDays : 0;
        g_sqliteService = new SqliteQueueService(dbPath, options);
        log("SqliteQueueService initialized (early - for config cache).", LogLevel::INFO);
    } catch (const std::exception& e) {
        log("WARNING: Failed to initialize SqliteQueueService early: " + std::string(e.what()) + ". DB fallback will be unavailable.", LogLevel::WARNING);
    }
        
    // Retry ParseServerHierarchy with constant 10-second intervals until it
    // succeeds or we are asked to stop (C-15: previously ignored g_running,
    // so a service stop during startup could never complete).
    const int retryDelay = 10; // seconds
    bool hierarchySuccess = false;
    int attempt = 1;
    const std::string startupToken = BearerToken;

    while (!hierarchySuccess && g_running) {
        log("Attempt " + std::to_string(attempt) + " - Fetching server hierarchy...", LogLevel::INFO);

        const std::string cacheKey = "OPCUA_HIERARCHY_" + NodeID;

        // Sign in again before every retry. This loop can wait indefinitely
        // (started offline with no cached hierarchy, or the API has no
        // hierarchy for this node yet). Without a fresh token it would keep
        // using the startup token: empty if we started offline, or expired
        // after a long wait, so the client never recovered without a restart.
        // One attempt per retry (not fetchBearerTokenWithRetry's 3 x 10 s) so
        // a service stop during startup stays prompt. On failure the previous
        // token is kept; it may still be valid.
        if(attempt > 1) {
            try {
                json tokenResponse = ::getBearerToken(
                    applicationEndURLHost, std::to_string(applicationEndURLPort),
                    authUsername, authPassword);
                if(tokenResponse.contains("access_token") &&
                   tokenResponse["access_token"].is_string()) {
                    std::lock_guard<std::mutex> lk(g_bearerTokenMutex);
                    BearerToken = tokenResponse["access_token"].get<std::string>();
                } else {
                    log("Sign-in before hierarchy retry returned no token.",
                        LogLevel::WARNING);
                }
            } catch(const std::exception &e) {
                log("Sign-in before hierarchy retry failed: " + std::string(e.what()),
                    LogLevel::WARNING);
            }
        }

        // C-07: API FIRST, local cache only as a fallback.
        //
        // The previous order (cache first, API only on a miss) meant that once a
        // hierarchy had been cached, a restart never picked up backend changes.
        // Now the live API is authoritative whenever it can be reached:
        //   - API reachable, non-empty  -> use it, refresh the cache
        //   - API reachable, empty      -> treat as authoritative "nothing to
        //                                  run", do NOT load a stale cache, retry
        //   - API unreachable / throws  -> load the last cached copy (offline)
        bool apiReachable = false;

        if(BearerToken.empty()) {
            log("No bearer token (offline) - skipping hierarchy API call, using local cache.",
                LogLevel::WARNING);
        } else {
            try {
                std::string target = "/api/GetOpcUaHierarchy";
                json jBody;
                jBody["orgId"] = 0;
                jBody["roleId"] = "";
                jBody["userId"] = 0;
                jBody["moduleId"] = 0;
                jBody["userType"] = "";
                jBody["requestDateTime"] = currentUtcIso8601(); // C-32
                jBody["ipAddress"] = "";
                jBody["originName"] = "";
                jBody["filterModel"]["customValue"] = NodeID;
                std::string json_body = jBody.dump();

                json apiResponse = getResponse(applicationEndURLHost,
                                               std::to_string(applicationEndURLPort),
                                               BearerToken, json_body, target);
                apiReachable = true;

                serverList = ParseServerHierarchyFromJson(apiResponse);
                if(!serverList.empty()) {
                    hierarchySuccess = true;
                    log("Server hierarchy fetched from API successfully! Found " +
                            std::to_string(serverList.size()) + " servers.",
                        LogLevel::INFO);
                    if(g_sqliteService) {
                        try {
                            g_sqliteService->SetConfig(cacheKey, apiResponse.dump());
                            log("Cached fresh hierarchy to local database key: " + cacheKey,
                                LogLevel::INFO);
                        } catch(const std::exception &e) {
                            log("Failed to cache to database: " + std::string(e.what()),
                                LogLevel::WARNING);
                        }
                    }
                } else {
                    log("API returned an empty server hierarchy for " + NodeID +
                            " (authoritative - not using cache). Retrying in " +
                            std::to_string(retryDelay) + " seconds...",
                        LogLevel::ERRORS);
                }
            } catch(const std::exception &e) {
                log("Hierarchy API call failed: " + std::string(e.what()) +
                        " - falling back to local cache.",
                    LogLevel::ERRORS);
            }
        }

        if(!hierarchySuccess && !apiReachable && g_sqliteService) {
            log("Checking local database for key: " + cacheKey, LogLevel::INFO);
            try {
                std::string dbData = g_sqliteService->GetConfig(cacheKey);
                if(!dbData.empty()) {
                    json dbJson = json::parse(dbData);
                    serverList = ParseServerHierarchyFromJson(dbJson);
                    if(!serverList.empty()) {
                        hierarchySuccess = true;
                        log("Hierarchy loaded from local database (API unavailable). Found " +
                                std::to_string(serverList.size()) + " servers.",
                            LogLevel::WARNING);
                    } else {
                        log("Database hierarchy is invalid or empty.", LogLevel::WARNING);
                    }
                } else {
                    log("DB Miss for " + cacheKey + ".", LogLevel::INFO);
                }
            } catch(const std::exception &e) {
                log("Error reading hierarchy from database: " + std::string(e.what()),
                    LogLevel::ERRORS);
            }
        }

        if (!hierarchySuccess) {
            log("Retrying in " + std::to_string(retryDelay) + " seconds...", LogLevel::INFO);
            // Sleep in 1 s slices so a stop request is honoured promptly.
            for(int i = 0; i < retryDelay && g_running; ++i)
                std::this_thread::sleep_for(std::chrono::seconds(1));
            attempt++;
        }
    }

    if(!hierarchySuccess) {
        log("Startup aborted before the server hierarchy could be loaded (stop requested).",
            LogLevel::ERRORS);
        if(g_sqliteService) {
            g_sqliteService->DisposeDB();
            delete g_sqliteService;
            g_sqliteService = nullptr;
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
        return EXIT_FAILURE;
    }

    // The MQTT password is derived from the bearer token and was set before
    // the hierarchy loop. If the loop signed in again, derive it from the new
    // token so the first MQTT connect does not use an empty or expired one.
    if(BearerToken != startupToken) {
        SetEdgeConfigMqttPassword(edgeCfg, BearerToken);
        mqttPassword = edgeCfg.mqttPassword;
        log("MQTT credentials updated from the token obtained while waiting for the hierarchy.",
            LogLevel::INFO);
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

        // C-11: bulk upload goes to the configured API host, not a hardcoded IP.
        g_sqliteService->SetApiUrl("http://" + applicationEndURLHost + ":" +
                                   std::to_string(applicationEndURLPort) +
                                   "/api/UploadBulkTagData");
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

    // Client pool. Declared before the MQTT handler so the message callback
    // (registered right after connect, C-29) can capture it by reference.
    std::vector<std::unique_ptr<ClientContext>> clientContexts;
    std::unordered_map<std::string, ClientContext *> clientPool;

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
        // C-25: capture the EdgeConfig credentials by value. The callback runs
        // on the MQTT thread; it must not mutate `edgeCfg` (which other code
        // reads without synchronization). MQTTHandler stores the returned
        // password itself, so SetEdgeConfigMqttPassword is not needed here.
        g_mqttHandler->setPasswordRefreshCallback(
            [applicationEndURLHost, applicationEndURLPort,
             edgeUser = edgeCfg.username, edgePass = edgeCfg.password,
             &BearerToken]() -> std::string {
                try {
                    log("MQTT token refresh: fetching new bearer token ...", LogLevel::INFO);
                    std::string freshToken = getBearerTokenNow(
                        applicationEndURLHost,
                        std::to_string(applicationEndURLPort),
                        edgeUser,
                        edgePass,
                        true).get();
                    if(freshToken.empty()) {
                        log("MQTT token refresh: empty token returned.", LogLevel::WARNING);
                        return "";
                    }
                    {
                        std::lock_guard<std::mutex> lk(g_bearerTokenMutex);
                        BearerToken = freshToken;
                    }
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

    // C-29: register the inbound message handler NOW, before any topic is
    // subscribed and before the per-server contexts are built. Previously it
    // was registered after the (multi-second) server setup, so HTRLD commands
    // and control writes arriving in that window were silently dropped.
    // Hot-reload tasks posted before the worker starts simply queue up.
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

        // Find client context for this tag (C-01: Mapping is shared with the
        // hot-reload worker; read it under the lock and copy out what we need).
        std::string endpoint;
        std::string mappedNodePath;
        {
            std::lock_guard<std::mutex> mlk(g_mappingMutex);
            auto mit = Mapping.find(tagId);
            if(mit == Mapping.end()) {
                log("TagId " + std::to_string(tagId) + " not found in mapping", LogLevel::ERRORS);
                return;
            }
            mappedNodePath = mit->second.first;
            endpoint = mit->second.second;
        }

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
            // Inbound telemetry echoes are ignored; the client only produces
            // telemetry, it does not consume it.
        } else if(updateType == static_cast<int>(UpdateType::Control)) {
                // Extract the "ns=..;i=.." / "ns=..;s=.." tail of the mapped path
                // (the hierarchy API stores paths like "Node2/ns=3;i=1002").
                std::string nodeIdStr = mappedNodePath;
                size_t lastSlash = nodeIdStr.find_last_of('/');
                if (lastSlash != std::string::npos) {
                    nodeIdStr = nodeIdStr.substr(lastSlash + 1);
                }

                // All UA_Client_* calls run on the OPC thread via postOpcTask.
                context->postOpcTask([context, tagId, data, nodeIdStr]() {
                    log("Executing COMMAND write for TagId: " + std::to_string(tagId), LogLevel::DEBUG);

                    if(!context->client) {
                        log("Control write: client is null for " + context->endpoint, LogLevel::ERRORS);
                        return;
                    }

                    // ── 1. Parse the payload value (keep the raw JSON too, so
                    //       booleans / strings can be written natively). ────
                    if(!data.contains("Value")) {
                        log("Missing Value field in payload", LogLevel::ERRORS);
                        return;
                    }
                    const nlohmann::json &jv = data["Value"];
                    double numericVal = 0.0;
                    bool haveNumeric = false;
                    try {
                        if(jv.is_number()) {
                            numericVal = jv.get<double>();
                            haveNumeric = true;
                        } else if(jv.is_boolean()) {
                            numericVal = jv.get<bool>() ? 1.0 : 0.0;
                            haveNumeric = true;
                        } else if(jv.is_string()) {
                            // Numeric strings are common in control payloads.
                            try { numericVal = std::stod(jv.get<std::string>()); haveNumeric = true; }
                            catch(...) { haveNumeric = false; }
                        } else {
                            log("Invalid Value type in payload", LogLevel::ERRORS);
                            return;
                        }
                    } catch(const std::exception &e) {
                        log("Failed to parse Value: " + std::string(e.what()), LogLevel::ERRORS);
                        return;
                    }

                    // ── 2. Build the target NodeId. C-04: use the namespace
                    //       index resolved from the org URI (what the monitors
                    //       use) when we have it; fall back to the stored index.
                    //       Supports both numeric (i=) and string (s=) ids. ──
                    UA_NodeId nid = parseUaNodePath(nodeIdStr);
                    if(UA_NodeId_isNull(&nid)) {
                        log("Failed to parse NodeId: " + nodeIdStr, LogLevel::ERRORS);
                        return;
                    }
                    const UA_UInt16 resolved = context->resolvedNs.load(std::memory_order_acquire);
                    if(resolved != 0)
                        nid.namespaceIndex = resolved;

                    // ── 3. Read the node's declared DataType so the write uses
                    //       the matching builtin type (C-03). open62541 rejects a
                    //       Double written to an Int32/Boolean node with
                    //       BadTypeMismatch. If the read fails, fall back to the
                    //       previous behaviour (write a Double). ──────────────
                    UA_NodeId dataTypeId = UA_NODEID_NULL;
                    UA_StatusCode dtRc =
                        UA_Client_readDataTypeAttribute(context->client.get(), nid, &dataTypeId);
                    int builtin = -1;
                    if(dtRc == UA_STATUSCODE_GOOD && dataTypeId.namespaceIndex == 0 &&
                       dataTypeId.identifierType == UA_NODEIDTYPE_NUMERIC) {
                        // NS0 numeric ids of the builtin types are 1..25 and index
                        // UA_TYPES directly (Boolean=1 .. DateTime=13 ..).
                        for(int t = 0; t < UA_TYPES_COUNT; ++t) {
                            if(UA_TYPES[t].typeId.identifierType == UA_NODEIDTYPE_NUMERIC &&
                               UA_TYPES[t].typeId.namespaceIndex == 0 &&
                               UA_TYPES[t].typeId.identifier.numeric ==
                                   dataTypeId.identifier.numeric) {
                                builtin = t;
                                break;
                            }
                        }
                    } else if(dtRc != UA_STATUSCODE_GOOD) {
                        log("Control write: could not read DataType of " + nodeIdStr +
                                " (" + std::string(UA_StatusCode_name(dtRc)) +
                                ") - writing as Double",
                            LogLevel::WARNING);
                    }
                    UA_NodeId_clear(&dataTypeId);

                    // ── 4. Build a variant of the right type. ─────────────────
                    UA_Variant value;
                    UA_Variant_init(&value);
                    bool ok = true;
                    std::string typeName = "Double";

                    // Storage for the scalar the variant points at.
                    UA_Boolean vBool; UA_SByte vSByte; UA_Byte vByte;
                    UA_Int16 vI16; UA_UInt16 vU16; UA_Int32 vI32; UA_UInt32 vU32;
                    UA_Int64 vI64; UA_UInt64 vU64; UA_Float vF32; UA_Double vF64;
                    UA_String vStr = UA_STRING_NULL;

                    switch(builtin) {
                        case UA_TYPES_BOOLEAN:
                            typeName = "Boolean";
                            if(jv.is_boolean()) vBool = jv.get<bool>();
                            else if(jv.is_string()) {
                                std::string sv = jv.get<std::string>();
                                std::transform(sv.begin(), sv.end(), sv.begin(), ::tolower);
                                vBool = (sv == "true" || sv == "1" || sv == "on");
                            } else vBool = haveNumeric && numericVal != 0.0;
                            UA_Variant_setScalar(&value, &vBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
                            break;
                        case UA_TYPES_SBYTE:  typeName = "SByte";  vSByte = (UA_SByte)numericVal;  UA_Variant_setScalar(&value, &vSByte, &UA_TYPES[UA_TYPES_SBYTE]);  ok = haveNumeric; break;
                        case UA_TYPES_BYTE:   typeName = "Byte";   vByte  = (UA_Byte)numericVal;   UA_Variant_setScalar(&value, &vByte,  &UA_TYPES[UA_TYPES_BYTE]);   ok = haveNumeric; break;
                        case UA_TYPES_INT16:  typeName = "Int16";  vI16   = (UA_Int16)numericVal;  UA_Variant_setScalar(&value, &vI16,   &UA_TYPES[UA_TYPES_INT16]);  ok = haveNumeric; break;
                        case UA_TYPES_UINT16: typeName = "UInt16"; vU16   = (UA_UInt16)numericVal; UA_Variant_setScalar(&value, &vU16,   &UA_TYPES[UA_TYPES_UINT16]); ok = haveNumeric; break;
                        case UA_TYPES_INT32:  typeName = "Int32";  vI32   = (UA_Int32)numericVal;  UA_Variant_setScalar(&value, &vI32,   &UA_TYPES[UA_TYPES_INT32]);  ok = haveNumeric; break;
                        case UA_TYPES_UINT32: typeName = "UInt32"; vU32   = (UA_UInt32)numericVal; UA_Variant_setScalar(&value, &vU32,   &UA_TYPES[UA_TYPES_UINT32]); ok = haveNumeric; break;
                        case UA_TYPES_INT64:  typeName = "Int64";  vI64   = (UA_Int64)numericVal;  UA_Variant_setScalar(&value, &vI64,   &UA_TYPES[UA_TYPES_INT64]);  ok = haveNumeric; break;
                        case UA_TYPES_UINT64: typeName = "UInt64"; vU64   = (UA_UInt64)numericVal; UA_Variant_setScalar(&value, &vU64,   &UA_TYPES[UA_TYPES_UINT64]); ok = haveNumeric; break;
                        case UA_TYPES_FLOAT:  typeName = "Float";  vF32   = (UA_Float)numericVal;  UA_Variant_setScalar(&value, &vF32,   &UA_TYPES[UA_TYPES_FLOAT]);  ok = haveNumeric; break;
                        case UA_TYPES_STRING: {
                            typeName = "String";
                            std::string sv = jv.is_string() ? jv.get<std::string>() : jv.dump();
                            vStr = UA_STRING_ALLOC(sv.c_str());
                            UA_Variant_setScalar(&value, &vStr, &UA_TYPES[UA_TYPES_STRING]);
                            break;
                        }
                        case UA_TYPES_DOUBLE:
                        default: // unknown / not read: previous behaviour
                            typeName = "Double";
                            vF64 = numericVal;
                            UA_Variant_setScalar(&value, &vF64, &UA_TYPES[UA_TYPES_DOUBLE]);
                            ok = haveNumeric;
                            break;
                    }

                    if(!ok) {
                        log("Control write: payload Value is not numeric but node " +
                                nodeIdStr + " is " + typeName + " - skipped",
                            LogLevel::ERRORS);
                        UA_String_clear(&vStr);
                        UA_NodeId_clear(&nid);
                        return;
                    }

                    log("Writing " + typeName + " value " + jv.dump() + " to ns=" +
                            std::to_string(nid.namespaceIndex) + " (" + nodeIdStr + ")",
                        LogLevel::DEBUG);

                    UA_StatusCode retval =
                        UA_Client_writeValueAttribute(context->client.get(), nid, &value);

                    if (retval == UA_STATUSCODE_GOOD) {
                        log("Value written successfully to OPC UA server", LogLevel::INFO);
                    } else {
                        log("Failed to write value: " + std::string(UA_StatusCode_name(retval)), LogLevel::ERRORS);
                    }

                    // `value` points at stack storage (or vStr), so only the
                    // string payload and the NodeId need explicit cleanup.
                    UA_String_clear(&vStr);
                    UA_NodeId_clear(&nid);
                });
            }
         else if(updateType == static_cast<int>(UpdateType::BulkData)) {
            log("BULKDATA update received", LogLevel::DEBUG);
        } else {
            log("Unknown UpdateType: " + std::to_string(updateType), LogLevel::ERRORS);
        }
    });


#ifdef UA_ENABLE_SUBSCRIPTIONS
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);
#endif




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
        context->connectOnce = [ctx = context.get(), server = server_copy,
                                fallbackUser = authUsername,
                                fallbackPass = authPassword]() -> UA_StatusCode {

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
            
        config->subscriptionInactivityCallback = [](UA_Client *, UA_UInt32, void *subCtx) {
            auto c = static_cast<ClientContext*>(subCtx);
            if(c) {
                log(c->name + ": Subscription inactivity detected! Forcing reconnect.", LogLevel::ERRORS);
                c->isConnected.store(false, std::memory_order_release);
            }
        };
    
        // --- Start of Corrected Security Logic ---
    
        if(server.msgSecurityMode != "NONE" && !server.msgSecurityMode.empty()) {

                        // ADD MUTEX LOCK HERE
                        std::lock_guard<std::mutex> lock(cert_loading_mutex);

            // This server requires security. Load certificates and apply them.
            UA_ByteString client_cert = loadFile("client/own/certs/client_cert.der");
            UA_ByteString client_key = loadFile("client/own/certs/client_key.der");

            // ── Auto-generate if cert or key is missing ──────────────────────
            if(client_cert.length == 0 || client_key.length == 0) {
                log("No client certificate/key found — generating a new self-signed certificate...",
                    LogLevel::WARNING);
                UA_ByteString_clear(&client_cert);
                UA_ByteString_clear(&client_key);

                UA_String certSubject[3] = {
                    UA_STRING_STATIC("C=IN"),
                    UA_STRING_STATIC("O=Anexee"),
                    UA_STRING_STATIC("CN=AnexeeClient")
                };

                // Build SAN entries. For a client cert the server typically
                // only checks the ApplicationURI, but we include the machine
                // hostname + loopback for completeness and to match the
                // server's pattern.
                std::string localHostname = "localhost";
#ifdef _WIN32
                {
                    char hostnameBuf[256];
                    DWORD hostnameLen = sizeof(hostnameBuf);
                    if(GetComputerNameA(hostnameBuf, &hostnameLen))
                        localHostname = std::string(hostnameBuf);
                }
#else
                {
                    char hostnameBuf[256];
                    if(gethostname(hostnameBuf, sizeof(hostnameBuf)) == 0)
                        localHostname = std::string(hostnameBuf);
                }
#endif
                std::string sanHostnameEntry = "DNS:" + localHostname;

                // Optionally include the target OPC UA server's host in the
                // SAN so the cert is also valid if the server validates the
                // client address (rare, but defensive).
                std::string serverHost = ExtractHostFromUrl(server.endpointUrl);
                std::string sanServerHostEntry;
                if(!serverHost.empty() && serverHost != localHostname && serverHost != "localhost") {
                    sanServerHostEntry = (LooksLikeIpv4Address(serverHost) ? "IP:" : "DNS:") + serverHost;
                    log("Including target server address in client cert SAN: " + sanServerHostEntry, LogLevel::INFO);
                }

                std::vector<UA_String> certSubjectAltName = {
                    UA_STRING_STATIC("DNS:localhost"),
                    UA_STRING_ALLOC(sanHostnameEntry.c_str()),
                    UA_STRING_STATIC("IP:127.0.0.1"),
                    UA_STRING_STATIC("URI:urn:Anexee.client.application")
                };
                if(!sanServerHostEntry.empty())
                    certSubjectAltName.push_back(UA_STRING_ALLOC(sanServerHostEntry.c_str()));

                UA_KeyValueMap *certParams = UA_KeyValueMap_new();
                UA_UInt16 certExpiresInDays = 3650;
                UA_KeyValueMap_setScalar(certParams, UA_QUALIFIEDNAME(0, (char*)"expires-in-days"),
                                         (void*)&certExpiresInDays, &UA_TYPES[UA_TYPES_UINT16]);

                UA_StatusCode certGenStatus = UA_CreateCertificate(
                    UA_Log_Stdout, certSubject, 3,
                    certSubjectAltName.data(), certSubjectAltName.size(),
                    UA_CERTIFICATEFORMAT_DER, certParams, &client_key, &client_cert);
                UA_KeyValueMap_delete(certParams);

                // Clean up UA_STRING_ALLOC'd SAN entries
                UA_String_clear(&certSubjectAltName[1]); // hostname
                if(!sanServerHostEntry.empty())
                    UA_String_clear(&certSubjectAltName[4]); // server host

                if(certGenStatus != UA_STATUSCODE_GOOD || client_cert.length == 0 || client_key.length == 0) {
                    log("Failed to auto-generate client certificate: " +
                            std::string(UA_StatusCode_name(certGenStatus)),
                        LogLevel::ERRORS);
                } else {
                    // Persist to disk so subsequent runs reuse the same identity.
                    FILE *certFp = fopen("client/own/certs/client_cert.der", "wb");
                    if(certFp) {
                        fwrite(client_cert.data, sizeof(UA_Byte), client_cert.length, certFp);
                        fclose(certFp);
                    } else {
                        log("WARNING: Could not persist generated client_cert.der to disk", LogLevel::WARNING);
                    }
                    FILE *keyFp = fopen("client/own/certs/client_key.der", "wb");
                    if(keyFp) {
                        fwrite(client_key.data, sizeof(UA_Byte), client_key.length, keyFp);
                        fclose(keyFp);
                    } else {
                        log("WARNING: Could not persist generated client_key.der to disk", LogLevel::WARNING);
                    }
                    log("✓ Generated and saved a new self-signed client certificate (client/own/certs/)",
                        LogLevel::INFO);
                }
            }

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
                 log("Warning: Could not load or generate client certificate for secure server " + server.name, LogLevel::ERRORS);
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
            } else {
                log("Unknown msgSecurityMode '" + server.msgSecurityMode + "' for " +
                        server.name + " - leaving SDK default security mode",
                    LogLevel::WARNING);
            }

            // Map the API's policy name to a policy URI. C-24: clear the URI
            // that setDefaultEncryption already allocated before replacing it
            // (this leaked one string per reconnect), and do NOT force "#None"
            // for an unknown policy - with SIGN/ENCRYPT mode that combination
            // can never connect. Leaving the URI untouched lets the SDK pick
            // the best matching policy the server offers.
            const std::string &policy = server.securityPolicy;
            const char *policyUri = nullptr;
            if(policy == "UA_SP_BASIC256") {
                policyUri = "http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256";
            } else if(policy == "UA_SP_AES128") {
                policyUri = "http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep";
            } else if(policy == "UA_SP_AES256") {
                policyUri = "http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss";
            }
            if(policyUri) {
                UA_String_clear(&config->securityPolicyUri);
                config->securityPolicyUri = UA_STRING_ALLOC(policyUri);
            } else {
                log("Unknown securityPolicy '" + policy + "' for " + server.name +
                        " - letting the SDK negotiate the policy",
                    LogLevel::WARNING);
            }

        } 
        // If msgSecurityMode is "NONE", we do nothing extra. The UA_ClientConfig_setDefault already handled it.



    
        // --- End of Corrected Security Logic ---
            // C-34: the parser already normalizes "anonymous"/"anonymus" to
            // AUTH_STG_ANYMS; an empty authType is treated as anonymous too.
            if(server.authType == "AUTH_STG_ANYMS" || server.authType.empty()) {
                return UA_Client_connect(ctx->client.get(), server.endpointUrl.c_str());
            } else if(server.authType == "AUTH_STG_AUTH") {
                std::string user = server.username;
                std::string pass = server.password;

                // C-10: fall back to this node's EdgeConfig identity (an Anexee
                // login, which is what the Anexee OPC UA server authenticates
                // against) instead of a hardcoded personal account.
                if(user.empty()) user = fallbackUser;
                if(pass.empty()) pass = fallbackPass;

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
            // C-10: the profile belongs to the identity the bearer token was
            // issued for (the EdgeConfig user), so key the cache on that.
            std::string cacheKey = "USER_PROFILE_" + authUsername;
            nlohmann::ordered_json profileJson;
            bool gotProfile = false;

            std::string tokenCopy;
            {
                std::lock_guard<std::mutex> lk(g_bearerTokenMutex);
                tokenCopy = BearerToken;
            }

            // C-07: API first; the cached copy is only used when the API call
            // fails (offline) or returns an unusable profile.
            if(!tokenCopy.empty()) {
                try {
                    profileJson = getResponse(applicationEndURLHost,
                                              std::to_string(applicationEndURLPort),
                                              tokenCopy, json_body, "/api/GetUserProfile");
                    profile = ParseUserProfileFromJson(profileJson);
                    if(!profile.currentOrgCode.empty() && !profile.currentOrgId.empty()) {
                        gotProfile = true;
                        if(g_sqliteService) {
                            try {
                                g_sqliteService->SetConfig(cacheKey, profileJson.dump());
                            } catch(const std::exception& e) {
                                log("⚠️ Failed to cache UserProfile to DB: " + std::string(e.what()), LogLevel::WARNING);
                            }
                        }
                    } else {
                        log("⚠️ API UserProfile response incomplete (missing OrgCode/ID) - trying cache.",
                            LogLevel::WARNING);
                    }
                } catch(const std::exception& e) {
                    log("⚠️ UserProfile API call failed: " + std::string(e.what()) +
                            " - falling back to cached profile.",
                        LogLevel::WARNING);
                }
            } else {
                log("⚠️ No bearer token (offline) - using cached UserProfile if available.",
                    LogLevel::WARNING);
            }

            if(!gotProfile && g_sqliteService) {
                try {
                    std::string dbData = g_sqliteService->GetConfig(cacheKey);
                    if(!dbData.empty()) {
                        log("💾 DB Hit for UserProfile: " + cacheKey, LogLevel::INFO);
                        profileJson = nlohmann::ordered_json::parse(dbData);
                        profile = ParseUserProfileFromJson(profileJson);
                    } else {
                        log("📉 DB Miss for UserProfile: " + cacheKey, LogLevel::INFO);
                    }
                } catch(const std::exception& e) {
                    log("⚠️ DB cache error for UserProfile: " + std::string(e.what()), LogLevel::WARNING);
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

        std::string orgShortCode = profile.currentOrgCode;

        std::string NamespaceURI = "Anexee:" + orgShortCode;
        // Cache the URI on the context so hot reload paths (running outside
        // onConnected) can re-resolve the namespace index after a reconnect.
        context->namespaceURI = NamespaceURI;
        //------------------------------------------------------------------------------------------

        context->onConnected = [ctx = context.get(), server = server_copy,
                                NamespaceURI]() {
            // Runs on the OPC thread right after connectOnce() succeeded.
            //
            // C-31: the per-server "events" subscription (MonitorEvent on the
            // Server object) was removed. Its handler only logged the fields,
            // nothing was forwarded, so it was pure subscription/bandwidth
            // overhead on every server.

            // ── Resolve the org namespace index ONCE, here on the OPC thread ─
            // (UA_Client_getNamespaceIndex is a network round-trip and must use
            // the client from this thread). Worker tasks below read the cached
            // value; they never touch the UA_Client.
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
                    ctx->resolvedNs.store(resolvedNs, std::memory_order_release);
                } else {
                    log(ctx->name + ": namespace '" + NamespaceURI +
                            "' not found on server - using node paths' stored ns index",
                        LogLevel::WARNING);
                }
            }

#ifdef UA_ENABLE_SUBSCRIPTIONS
            // ── Group subscriptions (must stay on the OPC thread) ─────────────
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

                UA_CreateSubscriptionResponse gsub =
                    UA_Client_Subscriptions_create(ctx->client.get(), greq, ctx, nullptr, nullptr);
                if(gsub.responseHeader.serviceResult != UA_STATUSCODE_GOOD) {
                    log("Subscription creation FAILED for '" + group.name + "' on " +
                            ctx->endpoint + ": " +
                            UA_StatusCode_name(gsub.responseHeader.serviceResult),
                        LogLevel::ERRORS);
                    continue;
                }
                {
                    std::lock_guard<std::mutex> lk(ctx->subsMutex);
                    ctx->subscriptions[group.name] = gsub;
                    // The startup hierarchy API ships the group's hierarchy id
                    // under `dataPointId`; the hot reload MQTT command refers to
                    // the same number as `id`. Populating groupIdToName here lets
                    // applyGroupUpdate look up the subscription by id directly.
                    if(group.dataPointId != 0)
                        ctx->groupIdToName[group.dataPointId] = group.name;
                }
                if(group.dataPointId == 0) {
                    log("Subscription Created with no hierarchy id for '" +
                            group.name +
                            "' - hot reload group updates will resolve by "
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

            // ── Monitored items: ONE service call per group ──────────────────
            // C-16: the CPU-heavy preparation (ExprTk compile, NodeId parse,
            // context construction) is done on a worker task so the OPC thread
            // keeps calling run_iterate (keep-alives!) while it happens. Only
            // the final UA_Client_MonitoredItems_createDataChanges call is
            // posted back to the OPC thread.
            for(const auto &group : server.groups) {
                const std::string groupName = group.name;

                ctx->postTask([ctx, group, groupName, resolvedNs, nsResolved]() {
                    std::vector<MappedInfospaceTag>               batchInfoSpaces;
                    std::vector<UA_NodeId>                        batchNodeIds;
                    std::vector<std::shared_ptr<MyMonitorContext>> batchContexts;

                    for(const auto &tag : group.tags) {
                        if(!tag.mappedInfospaceTags) continue;
                        for(const auto &infoSpace : *tag.mappedInfospaceTags) {
                            std::string nodeIdStr;
                            {
                                std::lock_guard<std::mutex> mlk(g_mappingMutex);
                                auto it = Mapping.find(infoSpace.tagId);
                                if(it == Mapping.end()) continue;
                                nodeIdStr = it->second.first;
                            }

                            // 1. Parse NodeId (no UA_Client needed)
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

                            // 2. Build MyMonitorContext (shared - see structs.h)
                            //    and pre-compile the ExprTk AST.
                            auto myCtx = std::make_shared<MyMonitorContext>();
                            UA_NodeId_copy(&nodeId, &myCtx->nodeId); // for polling
                            myCtx->infoSpace = infoSpace;
                            myCtx->mqttHandler = g_mqttHandler;
                            myCtx->sqliteService = g_sqliteService;
                            myCtx->asyncPublisher = g_asyncPublisher;
                            myCtx->postWorkerTask = [ctx](std::function<void()> task) {
                                ctx->postTask(std::move(task));
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
                                if(!parser.compile(exprStr, myCtx->exprContext->expression)) {
                                    log("ExprTk compilation failed for tag " +
                                            std::to_string(infoSpace.tagId) + ": " +
                                            parser.error(),
                                        LogLevel::ERRORS);
                                    myCtx->exprContext.reset();
                                }
                            }

                            batchInfoSpaces.push_back(infoSpace);
                            batchNodeIds.push_back(nodeId);
                            batchContexts.push_back(std::move(myCtx));
                        }
                    }

                    if(batchInfoSpaces.empty()) return;

                    // Hand the prepared vectors to the OPC thread for the single
                    // network call. batchNodeIds are owned by the closure and
                    // cleared there.
                    ctx->postOpcTask([ctx, batchInfoSpaces = std::move(batchInfoSpaces),
                                      batchNodeIds = std::move(batchNodeIds),
                                      batchContexts = std::move(batchContexts),
                                      groupName]() mutable {
                        // Free the parsed NodeIds no matter how we leave.
                        struct NodeIdsCleanup {
                            std::vector<UA_NodeId> &ids;
                            ~NodeIdsCleanup() {
                                for(auto &n : ids) UA_NodeId_clear(&n);
                            }
                        } nodeIdsCleanup{batchNodeIds};

                        if(!ctx->client || !ctx->isConnected.load(std::memory_order_acquire)) {
                            log("Group " + groupName + ": client not connected, skipping "
                                "monitor registration on " + ctx->endpoint,
                                LogLevel::WARNING);
                            return;
                        }

                        UA_UInt32 subscriptionId = 0;
                        {
                            std::lock_guard<std::mutex> lk(ctx->subsMutex);
                            auto sIt = ctx->subscriptions.find(groupName);
                            if(sIt == ctx->subscriptions.end()) {
                                log("Group " + groupName + ": no subscription on " +
                                        ctx->endpoint + " - monitors not registered",
                                    LogLevel::ERRORS);
                                return;
                            }
                            subscriptionId = sIt->second.subscriptionId;
                        }

                        const size_t count = batchInfoSpaces.size();

                        UA_CreateMonitoredItemsRequest batchReq;
                        UA_CreateMonitoredItemsRequest_init(&batchReq);
                        batchReq.subscriptionId     = subscriptionId;
                        batchReq.timestampsToReturn = UA_TIMESTAMPSTORETURN_BOTH;
                        batchReq.itemsToCreate =
                            (UA_MonitoredItemCreateRequest *)UA_Array_new(
                                count,
                                &UA_TYPES[UA_TYPES_MONITOREDITEMCREATEREQUEST]);
                        batchReq.itemsToCreateSize = count;

                        std::vector<void *> rawCtxs(count, nullptr);
                        std::vector<UA_Client_DataChangeNotificationCallback> cbs(
                            count, handler_NodeValueChanged);
                        std::vector<UA_Client_DeleteMonitoredItemCallback>  delCbs(count, nullptr);

                        for(size_t i = 0; i < count; ++i) {
                            const MappedInfospaceTag &infoSpace = batchInfoSpaces[i];

                            UA_MonitoredItemCreateRequest &req = batchReq.itemsToCreate[i];
                            UA_MonitoredItemCreateRequest_init(&req);
                            // Deep copy: UA_CreateMonitoredItemsRequest_clear frees it.
                            UA_NodeId_copy(&batchNodeIds[i], &req.itemToMonitor.nodeId);

                            req.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
                            req.monitoringMode            = UA_MONITORINGMODE_REPORTING;
                            // C-20: per-tag backend settings (0 = server default).
                            req.requestedParameters.samplingInterval =
                                (UA_Double)(infoSpace.samplingInterval > 0 ? infoSpace.samplingInterval : 0);
                            req.requestedParameters.queueSize =
                                (UA_UInt32)(infoSpace.queuesize > 0 ? infoSpace.queuesize : 10000);
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

                            rawCtxs[i] = batchContexts[i].get();
                        }

                        // ── Single network service call for the whole group ───
                        UA_CreateMonitoredItemsResponse resp =
                            UA_Client_MonitoredItems_createDataChanges(
                                ctx->client.get(),
                                batchReq,
                                rawCtxs.data(),
                                cbs.data(),
                                delCbs.data());

                        UA_CreateMonitoredItemsRequest_clear(&batchReq);

                        // ── Process results ──────────────────────────────────
                        size_t ok = 0;
                        {
                            std::lock_guard<std::mutex> regLk(ctx->registryMutex);
                            for(size_t i = 0; i < resp.resultsSize && i < count; ++i) {
                                if(resp.results[i].statusCode != UA_STATUSCODE_GOOD) {
                                    // Include the NodeId we sent so a bad path in
                                    // the hierarchy config is visible in the log.
                                    log("MonitoredItem failed for TagId " +
                                            std::to_string(batchInfoSpaces[i].tagId) +
                                            " [" + uaNodeIdToString(batchNodeIds[i]) + "]: " +
                                            UA_StatusCode_name(resp.results[i].statusCode),
                                        LogLevel::ERRORS);
                                    // batchContexts[i] is released with this closure.
                                } else {
                                    ++ok;
                                    LiveMonitorEntry entry;
                                    entry.hierarchyId = batchInfoSpaces[i].hierarchyId;
                                    entry.tagId = batchInfoSpaces[i].tagId;
                                    entry.monitoredItemId = resp.results[i].monitoredItemId;
                                    entry.subscriptionId = subscriptionId;
                                    entry.groupName = groupName;
                                    entry.endpointUrl = ctx->endpoint;
                                    entry.infoSpace = batchInfoSpaces[i];
                                    entry.monCtx = batchContexts[i];
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

#ifdef _WIN32
// C-37: register / unregister this exe as a Windows service, mirroring the
// server's --install/--uninstall. The service name is unchanged
// ("AnexeeOPCUAClient") so existing registrations keep working. The binPath
// is this exe with no arguments: main() detects SCM via
// StartServiceCtrlDispatcher and falls back to console mode otherwise.
static int
installClientService() {
    char selfPath[MAX_PATH];
    if(GetModuleFileNameA(NULL, selfPath, MAX_PATH) == 0) {
        std::cerr << "Cannot get executable path." << std::endl;
        return 1;
    }
    std::string binPath = "\"" + std::string(selfPath) + "\"";

    SC_HANDLE hSCManager = OpenSCManagerA(NULL, NULL, SC_MANAGER_CREATE_SERVICE);
    if(!hSCManager) {
        DWORD err = GetLastError();
        std::cerr << "OpenSCManager failed: " << err
                  << (err == ERROR_ACCESS_DENIED ? " (run as Administrator)" : "")
                  << std::endl;
        return 1;
    }
    SC_HANDLE hService = CreateServiceA(
        hSCManager, SERVICE_NAME, "Anexee OPC UA Client", SERVICE_ALL_ACCESS,
        SERVICE_WIN32_OWN_PROCESS, SERVICE_AUTO_START, SERVICE_ERROR_NORMAL,
        binPath.c_str(), NULL, NULL, NULL, NULL, NULL);
    if(!hService) {
        DWORD err = GetLastError();
        std::cerr << "CreateService failed: " << err
                  << (err == ERROR_SERVICE_EXISTS ? " (already installed)" : "")
                  << std::endl;
        CloseServiceHandle(hSCManager);
        return err == ERROR_SERVICE_EXISTS ? 0 : 1;
    }
    std::cout << "Service '" << SERVICE_NAME << "' installed. Start with: sc start "
              << SERVICE_NAME << std::endl;
    CloseServiceHandle(hService);
    CloseServiceHandle(hSCManager);
    return 0;
}

static int
uninstallClientService() {
    SC_HANDLE hSCManager = OpenSCManagerA(NULL, NULL, SC_MANAGER_CONNECT);
    if(!hSCManager) {
        std::cerr << "OpenSCManager failed: " << GetLastError() << std::endl;
        return 1;
    }
    SC_HANDLE hService = OpenServiceA(hSCManager, SERVICE_NAME, SERVICE_STOP | DELETE);
    if(!hService) {
        DWORD err = GetLastError();
        std::cerr << "OpenService failed: " << err
                  << (err == ERROR_SERVICE_DOES_NOT_EXIST ? " (not installed)" : "")
                  << std::endl;
        CloseServiceHandle(hSCManager);
        return err == ERROR_SERVICE_DOES_NOT_EXIST ? 0 : 1;
    }
    SERVICE_STATUS st{};
    ControlService(hService, SERVICE_CONTROL_STOP, &st); // best effort
    int rc = 0;
    if(!DeleteService(hService)) {
        std::cerr << "DeleteService failed: " << GetLastError() << std::endl;
        rc = 1;
    } else {
        std::cout << "Service '" << SERVICE_NAME << "' uninstalled." << std::endl;
    }
    CloseServiceHandle(hService);
    CloseServiceHandle(hSCManager);
    return rc;
}
#endif

int
main(int argc, char *argv[]) {
#ifdef _WIN32
    if(argc > 1) {
        if(std::string(argv[1]) == "--install")
            return installClientService();
        if(std::string(argv[1]) == "--uninstall")
            return uninstallClientService();
    }

    // The exe is linked for the Console subsystem, so this main() is the entry
    // point in every launch mode, including when the SCM starts the service.
    // The service is registered with a bare binPath (no flag), so detect SCM
    // by trying to connect the service dispatcher: when the exe was launched
    // from a console this fails immediately with
    // ERROR_FAILED_SERVICE_CONTROLLER_CONNECT and we run in console mode.
    SERVICE_TABLE_ENTRYA serviceTable[] = {
        {const_cast<LPSTR>(SERVICE_NAME), (LPSERVICE_MAIN_FUNCTIONA)ServiceMain},
        {nullptr, nullptr}};
    if(StartServiceCtrlDispatcherA(serviceTable))
        return 0;  // Ran as a service; ServiceMain has finished.
#endif
    return runClient(false, argc, argv);
}

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
    g_serviceWorkerExitCode.store(code);
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

    // C-22: an operator-requested stop is a clean exit. Anything else (the
    // worker returned on its own with a non-zero code, e.g. missing
    // appsettings.json / EdgeConfig) is reported as a service-specific error
    // so SCM records the failure and recovery actions can trigger.
    const int workerCode = g_serviceWorkerExitCode.load();
    if(stopping || workerCode == 0) {
        ReportSvcStatus(SERVICE_STOPPED, NO_ERROR, 0);
    } else {
        g_ServiceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
        g_ServiceStatus.dwCurrentState = SERVICE_STOPPED;
        g_ServiceStatus.dwControlsAccepted = 0;
        g_ServiceStatus.dwWin32ExitCode = ERROR_SERVICE_SPECIFIC_ERROR;
        g_ServiceStatus.dwServiceSpecificExitCode = (DWORD)workerCode;
        g_ServiceStatus.dwCheckPoint = 0;
        g_ServiceStatus.dwWaitHint = 0;
        if(g_StatusHandle)
            SetServiceStatus(g_StatusHandle, &g_ServiceStatus);
    }
}
#endif // _WIN32
