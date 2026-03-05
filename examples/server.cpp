/* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
 * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

#include <open62541/plugin/accesscontrol_default.h>
#include <open62541/plugin/certificategroup_default.h>
#include <open62541/plugin/log_stdout.h>
#include <open62541/plugin/securitypolicy.h>
#include <open62541/plugin/securitypolicy_default.h>
#include <open62541/server.h>
#include <open62541/server_config_default.h>
#include <open62541/util.h>
#include <open62541/client.h>
#include <open62541/client_config_default.h>

#include <algorithm>
#include <iostream>
#include <map>
#include <signal.h>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <set>

#include <chrono>
#include <future>
#include <thread>
#include <mutex>

#include "AandC.h"
#include "AlarmConfig.h"
#include "Logger.h"
#include "fetchAPI.h"
#include "SessionManager.h"
#include "Encryption.h"
#include "alarm_enums.h"
#include "AccessControl.h"
#include "ServerConfig.h"
#include "ServiceUtils.h"
#include "RedisClient.h"
#include "SqliteQueueService.h"
#include "EdgeConfigLoader.h"
#include "InstrumentedMutex.cpp"

#include <async_mqtt/all.hpp>
#include <async_mqtt/asio_bind/predefined_layer/mqtts.hpp>
#include <async_mqtt/asio_bind/predefined_layer/ws.hpp>
#include <async_mqtt/asio_bind/predefined_layer/wss.hpp>
#include <boost/asio.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <nlohmann/json.hpp>
#ifdef _WIN32
#include <windows.h>
#include <malloc.h> // For _heapmin
#include <direct.h>  // For _getcwd
#else
#include <unistd.h>
#include <dirent.h>  // For opendir, readdir, closedir, DIR, dirent
#include <sys/types.h>
#include <sys/wait.h>
#include <limits.h>
#endif

using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

// ============================================================================
// ------------------GLOBALS----------------------------------------//
// ============================================================================
 UA_Boolean running = true;

// Service Globals
#ifdef _WIN32
SERVICE_STATUS g_ServiceStatus;
SERVICE_STATUS_HANDLE g_StatusHandle;
#endif

// MQTT Subscription Queue Globals
std::mutex g_sub_mutex;
std::deque<std::string> g_subscription_queue;
std::deque<std::string> g_unsubscription_queue;
std::unordered_set<std::string> g_pending_subscriptions; // Set for O(1) queue lookups
std::unordered_set<std::string> g_subscribed_topics;
std::unique_ptr<boost::asio::steady_timer> g_sub_timer;

// Global NodeMap (Topic -> NodeId) for Generic Telemetry
std::map<std::string, UA_NodeId> nodeMap;
std::mutex g_nodeMap_mutex;
std::mutex g_topicMap_mutex;
// MULTI-TENANCY: Global Session Manager
SessionManager g_sessionManager;

// ============================================================================
// STRUCTURAL CONCURRENCY: Server Job Queue
// ============================================================================
#include <queue>
#include <functional>
#include <condition_variable>

struct AlarmJobData {
    int AETypeID;
    std::string aeInstanceId;
    bool active;
    bool enabled;
    bool shelved;
    bool acked;
    bool confirmed;
    bool retain;
    UA_UInt16 severity;
    std::string alarmMessage;
    std::string alarmName;
    std::string comment;
    UA_DateTime now;
    std::string quality;
    UA_StatusCode qualityCode;
    std::vector<TriggerToAlarmMapping> mappings;
};

enum class ServerJobType {
    AddNamespace,
    AddNodes,
    AddAlarms,
    WriteValue,
    SetEventNotifier,
    Custom
};

struct ServerJob {
    ServerJobType type;
    std::function<void(UA_Server*)> fn;
};

// Global Job Queue
std::queue<ServerJob> g_serverQueue;
std::mutex g_serverQueueMutex;
std::condition_variable g_serverQueueCv;

// Helper to push jobs safely
void enqueueServerJob(const std::function<void(UA_Server*)>& fn, ServerJobType type = ServerJobType::Custom) {
    {
        std::lock_guard<std::mutex> lock(g_serverQueueMutex);
        g_serverQueue.push({type, fn});
    }
    g_serverQueueCv.notify_one();
}
// ============================================================================

// Store API credentials and org list globally for worker threads and auth
static std::string g_bearerToken;
static std::string g_apiHost;
static std::string g_apiPort;
static std::string g_authUsername;  // From appsettings.json Authorization section
static std::string g_authPassword;  // From appsettings.json Authorization section
static std::vector<OrgConfig> g_organizations;  // List of all organizations
static UA_Server *g_server = nullptr;
SqliteQueueService *g_sqliteService = nullptr;  // For config cache fallback (non-static for extern access from SessionWorker)

// Define user credentials
static UA_UsernamePasswordLogin usernamePasswordLogin[2] = {
    {UA_STRING_STATIC("user1"), UA_STRING_STATIC("password1")},
    {UA_STRING_STATIC("user2"), UA_STRING_STATIC("password2")}};

// Topic Info Map for Generic Telemetry
unordered_map<string, TopicInfo> topicMap;
// ============================================================================
//-----------------------GLOBALS END---------------------------------------//
// ============================================================================
// 
// ============================================================================
//-----------------------CallBacks----------------------------//
// ============================================================================
// Your custom logger callback
static void
myLog(void *context, UA_LogLevel level, UA_LogCategory category, const char *msg,
      va_list args) {

    try {
        std::string text;

        // SAFEGUARD: Only use printf formatting for specific Core messages we need to
        // expand. For everything else, print the raw message to avoid CRT Assertions on
        // invalid specifiers (e.g. "%N").
        if(strchr(msg, '%') != nullptr &&
           strstr(msg, "Adding Condition failed") != nullptr) {

            char buffer[1024];
// Use _vsnprintf_s on Windows if possible, or standard vsnprintf
#ifdef _WIN32
            _vsnprintf_s(buffer, _countof(buffer), _TRUNCATE, msg, args);
#else
            vsnprintf(buffer, 1024, msg, args);
#endif
            text = std::string(buffer);
        } else {
            // Default: strictly literal (safe)
            text = msg;
        }

        // Filter out specific noisy logs
        if(text.find("AddNode: Node could not add") != std::string::npos) {
            return;
        }
        if(text.find("Deleting the MonitoredItem") != std::string::npos) {
            return;
        }

        switch(level) {
            case UA_LOGLEVEL_FATAL:
                log(text, LogLevel::FATAL);
                break;
            case UA_LOGLEVEL_ERROR:
                log(text, LogLevel::ERRORS);
                break;
            case UA_LOGLEVEL_WARNING:
                log(text, LogLevel::WARNING);
                break;
            case UA_LOGLEVEL_INFO:
                log(text, LogLevel::INFO);
                break;
            case UA_LOGLEVEL_DEBUG:
                // log(text, LogLevel::DEBUG);
                break;
            default:
                break;
        }
    } catch(...) {
        // Swallow all exceptions to avoid unwinding across C boundary
    }
}


static UA_StatusCode
myLoginCallback(const UA_String *username, const UA_ByteString *password,
                size_t usernamePasswordLoginSize,
                const UA_UsernamePasswordLogin *usernamePasswordLogin,
                void **sessionContext, void *loginContext) {
    // Safely convert username to string, avoiding problematic format specifiers
    std::string usernameStr;
    if(username && username->data && username->length > 0) {
        // Ensure we don't exceed buffer bounds
        size_t maxLen = std::min(username->length, (size_t)255);
        usernameStr.assign((char *)username->data, maxLen);
    } else {
        usernameStr = "unknown";
    }

    for(size_t i = 0; i < usernamePasswordLoginSize; i++) {
        if(UA_String_equal(username, &usernamePasswordLogin[i].username) &&
           UA_ByteString_equal(password, &usernamePasswordLogin[i].password)) {
            // Grant admin access to user1
            if(UA_String_equal(username, &usernamePasswordLogin[0].username)) {
                *sessionContext = (void *)1;  // Mark as admin
                log("Admin user login successful: " + usernameStr, LogLevel::INFO);
            } else {
                log("Regular user login successful: " + usernameStr, LogLevel::INFO);
            }
            return UA_STATUSCODE_GOOD;
        }
    }

    log("Login failed for user: " + usernameStr, LogLevel::ERRORS);
    UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Login failed for user: %s",
                   usernameStr.c_str());
    return UA_STATUSCODE_BADUSERACCESSDENIED;
}

// ============================================================================
// MULTI-TENANCY: Custom Access Control Session Activation
// ============================================================================
// This callback is invoked when a new session is activated (client connects).
// We extract the user's credentials, authenticate via API, get their orgId,
// and route them to the appropriate worker thread.
// ============================================================================
static UA_StatusCode
customActivateSession(UA_Server *server, UA_AccessControl *ac,
                      const UA_EndpointDescription *endpointDescription,
                      const UA_ByteString *secureChannelRemoteCertificate,
                      const UA_NodeId *sessionId,
                      const UA_ExtensionObject *userIdentityToken,
                      void **sessionContext) {

    // ========================================================================
    // STEP 1: Extract Username and Password
    // ========================================================================
    std::string username;
    std::string password;
    bool isAnonymous = true;

    if(userIdentityToken && userIdentityToken->encoding == UA_EXTENSIONOBJECT_DECODED) {
        if(userIdentityToken->content.decoded.type ==
           &UA_TYPES[UA_TYPES_USERNAMEIDENTITYTOKEN]) {
            UA_UserNameIdentityToken *token =
                (UA_UserNameIdentityToken *)userIdentityToken->content.decoded.data;

            if(token->userName.data && token->userName.length > 0) {
                username =
                    std::string((char *)token->userName.data, token->userName.length);
                isAnonymous = false;
            }

            if(token->password.data && token->password.length > 0) {
                password =
                    std::string((char *)token->password.data, token->password.length);
            }
        }
    }

    // Handle anonymous login - DISABLED
    if(isAnonymous) {
        log("❌ Anonymous login detected and rejected", LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    } else {
        log("  User: " + username, LogLevel::INFO);
    }

    if(username.empty() || password.empty()) {
        log("❌ Missing credentials", LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }

    // Encrypt password for API authentication (ONLY for user credentials, not
    // appsettings)
    std::string finalPassword = password;
    if(!isAnonymous) {
        finalPassword = GetEncryptedString("", password, 0);
        log("DEBUG: Encrypted Password for user '" + username + "': " + finalPassword,
            LogLevel::INFO);
    }

    // ========================================================================
    // STEP 2: Get Bearer Token (skip if offline - DB fallback in STEP 3)
    // ========================================================================
    std::string bearerToken;
    json tokenResponse;
    try {
        tokenResponse = getBearerToken(g_apiHost, g_apiPort, username, finalPassword);
        if(tokenResponse.contains("access_token")) {
            bearerToken = tokenResponse["access_token"].get<std::string>();
            log("✓ Bearer token acquired for session activation", LogLevel::DEBUG);
        } else {
            log("⚠️ No access_token in response. Will try cached profile.", LogLevel::WARNING);
        }
    } catch(const std::exception &e) {
        log("⚠️ Bearer token request failed (offline?): " + std::string(e.what()) + ". Will try cached profile.", LogLevel::WARNING);
    }

    // ========================================================================
    // STEP 3: Get User Profile to Extract OrgID
    // ========================================================================
    UserProfile profile;
    try {
        std::string json_body = "{}";
        std::string cacheKey = "USER_PROFILE_" + username;
        nlohmann::ordered_json profileJson;
        bool cacheHit = false;

        if(true) { 
             auto start_time = std::chrono::steady_clock::now();
             auto cachedVal = g_redisClient.get(cacheKey);
             auto end_time = std::chrono::steady_clock::now();
             auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

             if(cachedVal) {
                 try {
                     profileJson = nlohmann::ordered_json::parse(*cachedVal);
                     profile = ParseUserProfileFromJson(profileJson);
                     if(!profile.currentOrgCode.empty() && !profile.currentOrgId.empty()) {
                        cacheHit = true;
                        log("⚡ Redis Cache HIT for UserProfile (User: " + username + ") - Fetched in " + std::to_string(elapsed_ms) + " ms", LogLevel::INFO);
                     } else {
                        log("⚠️ Cached UserProfile incomplete (missing OrgCode/ID). Forcing refresh.", LogLevel::WARNING);
                        cacheHit = false;
                     }
                 } catch(const std::exception& e) {
                      log("⚠️ Redis Cache Parse Error for UserProfile: " + std::string(e.what()), LogLevel::WARNING);
                 }
             } else {
                 if(g_redisClient.isConnected())
                    log("📉 Redis Cache MISS for UserProfile (User: " + username + ") - Checked in " + std::to_string(elapsed_ms) + " ms", LogLevel::INFO);
             }
        }

        if(!cacheHit) {
            // 2b. Try local database fallback
            if(g_sqliteService) {
                try {
                    std::string dbData = g_sqliteService->GetConfig(cacheKey);
                    if(!dbData.empty()) {
                        log("💾 DB Hit for UserProfile (User: " + username + ")", LogLevel::INFO);
                        profileJson = nlohmann::ordered_json::parse(dbData);
                        profile = ParseUserProfileFromJson(profileJson);
                        if(!profile.currentOrgCode.empty() && !profile.currentOrgId.empty()) {
                            cacheHit = true;
                        } else {
                            log("⚠️ DB UserProfile incomplete. Falling back to API.", LogLevel::WARNING);
                        }
                    } else {
                        log("📉 DB Miss for UserProfile (User: " + username + ")", LogLevel::INFO);
                    }
                } catch(const std::exception& e) {
                    log("⚠️ DB Error for UserProfile: " + std::string(e.what()), LogLevel::WARNING);
                }
            }
        }

        if(!cacheHit) {
            if(bearerToken.empty()) {
                log("⚠️ No bearer token and no cached UserProfile for: " + username + ". Cannot authenticate offline.", LogLevel::WARNING);
            } else {
                auto futureResponse = std::async(std::launch::async, getResponse,
                                                g_apiHost, g_apiPort, bearerToken, json_body, "/api/GetUserProfile");
                
                profileJson = futureResponse.get();
                
                // Store in Redis (Persistent - no TTL)
                g_redisClient.setCompressed(cacheKey, profileJson.dump(), 0);

                // Also store in local database
                if(g_sqliteService) {
                    try {
                        g_sqliteService->SetConfig(cacheKey, profileJson.dump());
                        log("💾 Cached UserProfile to DB for user: " + username, LogLevel::INFO);
                    } catch(const std::exception& e) {
                        log("⚠️ Failed to cache UserProfile to DB: " + std::string(e.what()), LogLevel::WARNING);
                    }
                }
                
                profile = ParseUserProfileFromJson(profileJson);
            }
        }

    } catch(const std::exception &e) {
        log("❌ User profile request failed: " + std::string(e.what()), LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }

    if(profile.currentOrgId.empty()) {
        log("❌ No currentOrgId in user profile", LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }

    log("✓ User profile: " + profile.displayName + " (OrgID: " + profile.currentOrgId +
            ", Org: " + profile.currentOrgName + ")",
        LogLevel::INFO);

    // ========================================================================
    // STEP 4: Find Matching Organization Config
    // ========================================================================
    OrgConfig *targetOrg = nullptr;

    for(auto &org : g_organizations) {

        if(std::to_string(org.orgId) == profile.currentOrgId) {
            targetOrg = &org;
            break;
        }
    }

    if(!targetOrg) {
        log("❌ Organization not found for OrgID: " + profile.currentOrgId,
            LogLevel::ERRORS);
        log("  Available orgs: " + std::to_string(g_organizations.size()),
            LogLevel::DEBUG);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }

    // ========================================================================
    // STEP 5: Create or Join Worker Thread for This Org
    // ========================================================================
    bool registered = g_sessionManager.registerSession(
        *sessionId, targetOrg->shortCode, server,
        bearerToken,  // Use user's token, not server's admin token
        g_apiHost, g_apiPort);

    if(!registered) {
        log("❌ Failed to register session for org: " + targetOrg->shortCode,
            LogLevel::ERRORS);
        return UA_STATUSCODE_BADINTERNALERROR;
    }

    log("✅ Session activated for user '" + username + "' → Org '" +
            targetOrg->shortCode + "' (Active sessions: " +
            std::to_string(g_sessionManager.getActiveSessionCount()) + ")",
        LogLevel::INFO);

    // TODO: Store user profile for future RBAC implementation
    // Can add to sessionContext: *sessionContext = new UserProfile(profile);

    return UA_STATUSCODE_GOOD;
}

// ============================================================================
//----------------------- CallBacks END----------------------------//
// ============================================================================

// ============================================================================
//----------------------HELPER------------------//
// ============================================================================
// Helper to spawn a child server instance with configuration passed via Stdin
#ifdef _WIN32
void SpawnChildServer(const std::string& ignoredPath, const ServerConfig& config, const std::string& bearerToken, bool headless, HANDLE hJob) {
    // 0. Get Absolute Path of Self (Robust against CWD changes)
    char selfPath[MAX_PATH];
    if (GetModuleFileNameA(NULL, selfPath, MAX_PATH) == 0) {
        log("SpawnChild: Failed to get self path", LogLevel::ERRORS);
        return;
    }
    std::string executablePath = std::string(selfPath);

    // 1. Serialize Config to JSON
    json j;
    j["bearerToken"] = bearerToken; // Pass token to child
    
    j["config"]["id"] = config.id;
    j["config"]["name"] = config.name;
    j["config"]["ip"] = config.ip;
    j["config"]["port"] = config.port;
    j["config"]["nodeId"] = config.nodeId;
    
    // Serialize orgMappings
    j["config"]["orgMappings"] = json::array();
    for(const auto& org : config.orgMappingList) {
        j["config"]["orgMappings"].push_back({
            {"id", org.id},
            {"hierarchyId", org.hierarchyId},
            {"mapOrgId", org.mapOrgId},
            {"orgShortCode", org.orgShortCode}
        });
    }

    std::string jsonStr = j.dump();

    // 2. Create Pipe for Stdin
    HANDLE hReadPipe, hWritePipe;
    SECURITY_ATTRIBUTES saAttr;
    saAttr.nLength = sizeof(SECURITY_ATTRIBUTES);
    saAttr.bInheritHandle = TRUE;
    saAttr.lpSecurityDescriptor = NULL;

    if (!CreatePipe(&hReadPipe, &hWritePipe, &saAttr, 0)) {
        log("SpawnChild: CreatePipe failed", LogLevel::ERRORS);
        return;
    }

    // Ensure write handle is NOT inherited
    if (!SetHandleInformation(hWritePipe, HANDLE_FLAG_INHERIT, 0)) {
        log("SpawnChild: SetHandleInformation failed", LogLevel::ERRORS);
        return;
    }

    // 3. Setup Process Info
    STARTUPINFOA si;
    PROCESS_INFORMATION pi;
    ZeroMemory(&si, sizeof(si));
    si.cb = sizeof(si);
    si.hStdError = GetStdHandle(STD_ERROR_HANDLE);
    si.hStdOutput = GetStdHandle(STD_OUTPUT_HANDLE);
    si.hStdInput = hReadPipe; // Redirect Stdin
    si.dwFlags |= STARTF_USESTDHANDLES;

    ZeroMemory(&pi, sizeof(pi));

    // Command Line: using original executable path + --child
    std::string cmdLine = "\"" + executablePath + "\" --child";
    
    // 4. Create Process
    if (!CreateProcessA(NULL, 
                        const_cast<char*>(cmdLine.c_str()), 
                        NULL, 
                        NULL, 
                        TRUE, // Inherit handles
                        headless ? CREATE_NO_WINDOW : CREATE_NEW_CONSOLE, // Flags
                        NULL, 
                        NULL, 
                        &si, 
                        &pi)) 
    {
        log("SpawnChild: CreateProcess failed (" + std::to_string(GetLastError()) + ")", LogLevel::ERRORS);
        return;
    }

    // 5. Write Data to Pipe
    DWORD dwWritten;
    if (!WriteFile(hWritePipe, jsonStr.c_str(), jsonStr.size(), &dwWritten, NULL)) {
        log("SpawnChild: Write to bad pipe", LogLevel::ERRORS);
    }

    // 6. Close Pipes and Handles
    CloseHandle(hWritePipe); // Sending EOF to child
    CloseHandle(hReadPipe);
    
    log("✓ Spawned Child Instance for '" + config.name + "' (PID: " + std::to_string(pi.dwProcessId) + ")", LogLevel::INFO);

    // 7. Assign to Job Object (Auto-termination on parent exit)
    // DISABLED per user request: Child should survive parent exit
    /* 
    if (hJob != NULL) {
        if (!AssignProcessToJobObject(hJob, pi.hProcess)) {
            log("SpawnChild: AssignProcessToJobObject failed (" + std::to_string(GetLastError()) + ")", LogLevel::ERRORS);
        }
    }
    */

    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
}
#else
void SpawnChildServer(const std::string& ignoredPath, const ServerConfig& config, const std::string& bearerToken, bool headless, void* hJob) {
    // 0. Get Absolute Path of Self (Robust against CWD changes)
    char selfPath[PATH_MAX];
    ssize_t count = readlink("/proc/self/exe", selfPath, PATH_MAX);
    if (count < 0) {
        log("SpawnChild: Failed to get self path", LogLevel::ERRORS);
        return;
    }
    selfPath[count] = '\0';
    std::string executablePath = std::string(selfPath);

    // 1. Serialize Config to JSON
    json j;
    j["bearerToken"] = bearerToken; // Pass token to child
    j["config"]["id"] = config.id;
    j["config"]["name"] = config.name;
    j["config"]["ip"] = config.ip;
    j["config"]["port"] = config.port;
    j["config"]["nodeId"] = config.nodeId;
    j["config"]["orgMappings"] = json::array();
    for(const auto& org : config.orgMappingList) {
        j["config"]["orgMappings"].push_back({
            {"id", org.id},
            {"hierarchyId", org.hierarchyId},
            {"mapOrgId", org.mapOrgId},
            {"orgShortCode", org.orgShortCode}
        });
    }

    std::string jsonStr = j.dump();

    // 2. Create Pipe for Stdin
    int pipefd[2];
    if (pipe(pipefd) == -1) {
        log("SpawnChild: pipe failed", LogLevel::ERRORS);
        return;
    }

    // 3. Fork Process
    pid_t pid = fork();
    if (pid == -1) {
        log("SpawnChild: fork failed", LogLevel::ERRORS);
        close(pipefd[0]);
        close(pipefd[1]);
        return;
    }

    if (pid == 0) {
        // Child Process
        close(pipefd[1]); // Close write end
        dup2(pipefd[0], STDIN_FILENO); // Redirect stdin
        close(pipefd[0]);

        execl(executablePath.c_str(), executablePath.c_str(), "--child", NULL);
        // If execl fails:
        log("SpawnChild: execl failed", LogLevel::ERRORS);
        exit(EXIT_FAILURE);
    } else {
        // Parent Process
        close(pipefd[0]); // Close read end
        
        // 5. Write Data to Pipe
        ssize_t written = write(pipefd[1], jsonStr.c_str(), jsonStr.size());
        if (written < 0 || written != (ssize_t)jsonStr.size()) {
            log("SpawnChild: Write to bad pipe", LogLevel::ERRORS);
        }

        // 6. Close Pipe (sends EOF to child)
        close(pipefd[1]);
        
        log("✓ Spawned Child Instance for '" + config.name + "' (PID: " + std::to_string(pid) + ")", LogLevel::INFO);
    }
}
#endif


/**
 * Extract organization ShortCode from endpoint URL
 * Example: "opc.tcp://0.0.0.0:53531/PLANT01" -> "PLANT01"
 */
static std::string
extractShortCodeFromEndpoint(const UA_String *endpointUrl) {
    if(!endpointUrl || endpointUrl->length == 0) {
        return "";
    }

    std::string url((char *)endpointUrl->data, endpointUrl->length);

    // Find last slash to get path component
    size_t lastSlash = url.find_last_of('/');
    if(lastSlash != std::string::npos && lastSlash + 1 < url.length()) {
        return url.substr(lastSlash + 1);
    }

    return "";  // No path component found
}



static void
stopHandler(int sign) {
    log("Received shutdown signal", LogLevel::INFO);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
    running = false;
    g_serverQueueCv.notify_all(); // Wake up main loop immediately
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "stopping server");
}

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

    if(hFind == INVALID_HANDLE_VALUE) {
        return 0;
    }

    std::vector<std::string> derFiles;
    do {
        derFiles.push_back(findData.cFileName);
    } while(FindNextFileA(hFind, &findData) != 0);
    FindClose(hFind);

    size_t count = derFiles.size();
    if(count == 0) {
        return 0;
    }

    *certs = (UA_ByteString *)UA_malloc(sizeof(UA_ByteString) * count);
    for(size_t i = 0; i < count; ++i) {
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
    if(!dir)
        return 0;

    struct dirent *entry;
    std::vector<std::string> derFiles;
    while((entry = readdir(dir)) != NULL) {
        if(strstr(entry->d_name, ".der"))
            derFiles.push_back(entry->d_name);
    }

    size_t count = derFiles.size();
    if(count > 0) {
        *certs = (UA_ByteString *)UA_malloc(sizeof(UA_ByteString) * count);
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



vector<string>
split(const string &s, char delimiter) {
    vector<string> tokens;
    stringstream ss(s);
    string item;
    while(std::getline(ss, item, delimiter)) {
        tokens.push_back(item);
    }
    return tokens;
}

UA_NodeId
getOrCreateFolder(UA_Server *server, const string &path, const string &name,
                  UA_NodeId parent) {
    if(nodeMap.count(path))
        return nodeMap[path];
    UA_ObjectAttributes oAttr = UA_ObjectAttributes_default;
    oAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", name.c_str());

    UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(1, path.c_str());
    UA_QualifiedName qName = UA_QUALIFIEDNAME_ALLOC(1, name.c_str());
    UA_Server_addObjectNode(server, nodeId, parent,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES), qName,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_FOLDERTYPE), oAttr, NULL, NULL);
    UA_QualifiedName_clear(&qName);
    UA_ObjectAttributes_clear(&oAttr);
    nodeMap[path] = nodeId;
    return nodeId;
}


/* Helper to format NodeId as string (e.g., "ns=1;i=12345") */
static std::string
formatNodeId(const UA_NodeId *nodeId) {
    if(!nodeId || UA_NodeId_isNull(nodeId))
        return "";
    char buf[256];
    if(nodeId->identifierType == UA_NODEIDTYPE_NUMERIC) {
        snprintf(buf, sizeof(buf), "ns=%u;i=%u", nodeId->namespaceIndex,
                 nodeId->identifier.numeric);
    } else if(nodeId->identifierType == UA_NODEIDTYPE_STRING) {
        std::string str((char *)nodeId->identifier.string.data,
                        nodeId->identifier.string.length);
        snprintf(buf, sizeof(buf), "ns=%u;s=%s", nodeId->namespaceIndex, str.c_str());
    } else if(nodeId->identifierType == UA_NODEIDTYPE_GUID) {
        snprintf(buf, sizeof(buf), "ns=%u;g=...", nodeId->namespaceIndex);
    } else if(nodeId->identifierType == UA_NODEIDTYPE_BYTESTRING) {
        snprintf(buf, sizeof(buf), "ns=%u;b=...", nodeId->namespaceIndex);
    } else {
        return "";
    }
    return std::string(buf);
}

// ============================================================================
// MULTI-TENANCY: Custom Session Close Callback
// ============================================================================

static void
customCloseSession(UA_Server *server, UA_AccessControl *ac, const UA_NodeId *sessionId,
                   void *sessionContext) {

    log("🔓 Session closing...", LogLevel::INFO);

    // Unregister session and cleanup worker thread
    g_sessionManager.unregisterSession(*sessionId);

    log("  ✓ Session closed (Active sessions: " +
            std::to_string(g_sessionManager.getActiveSessionCount()) + ")",
        LogLevel::INFO);
}


// ============================================================================
//---------------------- HELPER END------------------//
// ============================================================================






// Custom logger plugin
static UA_Logger myLogger = {myLog, nullptr, nullptr};



//---------------- MQTT PUBLISHER GLOBALS AND HELPERS ---------------- //
as::io_context ioc;

// SSL context for MQTTS (TLS)
boost::asio::ssl::context mqtt_ssl_ctx{boost::asio::ssl::context::tlsv12_client};

// MQTT Client type for TLS (MQTTS)
using client_st = am::client<am::protocol_version::v5, am::protocol::mqtts>;
client_st amcl_s{ioc.get_executor(), mqtt_ssl_ctx};

// MQTT Client type for plain TCP (non-TLS)
using client_wt = am::client<am::protocol_version::v5, am::protocol::mqtt>;
client_wt amcl_w{ioc.get_executor()};

// Global MQTT config variables (populated from appsettings.json + EdgeConfig)
bool g_use_tls = false;
std::string g_broker_address;
int g_broker_port = 1883;
std::string g_mqtt_username;
std::string g_mqtt_password;
std::string g_mqtt_client_id;   // MQTT ClientID from EdgeConfig

// MQTT connection state and message queue for reconnection
std::atomic<bool> g_mqtt_connected{false};
struct QueuedMessage {
    std::string topic;
    std::string payload;
};
std::deque<QueuedMessage> g_mqtt_queue;
std::mutex g_mqtt_queue_mutex;

thread_local bool is_internal_write = false;

/* Global event notifier origin; if null, defaults to Server */
static UA_NodeId g_eventNotifierNode = UA_NODEID_NULL;

// Signal flag for MQTT subscription loop wake-up
std::atomic<bool> g_signal_pending{false};

// To publish from any thread:
void
publish_to_mqtt(const std::string &topic, const std::string &payload) {
    // If not connected, queue the message
    if(!g_mqtt_connected.load()) {
        std::lock_guard<std::mutex> lock(g_mqtt_queue_mutex);
        g_mqtt_queue.push_back({topic, payload});
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "[MQTT-QUEUE] Message queued for '%s' (not connected, queue size: %zu)",
                      topic.c_str(), g_mqtt_queue.size());
        return;
    }
    
    // Connected - publish normally

    as::post(ioc, [topic, payload]() {
        as::co_spawn(
            ioc,
            [topic, payload]() -> as::awaitable<void> {
                try {
                    if(g_use_tls) {
                        co_await amcl_s.async_publish(
                            am::v5::publish_packet{
                                topic,
                                payload,
                                am::qos::at_most_once
                            },
                            as::use_awaitable);
                    } else {
                        co_await amcl_w.async_publish(
                            am::v5::publish_packet{
                                topic,
                                payload,
                                am::qos::at_most_once
                            },
                            as::use_awaitable);
                    }
                } catch(const std::exception &e) {
                    log("[MQTT-PUB] ✗ Publish error to '" + topic + "': " + e.what(), LogLevel::ERRORS);
                    
                    // Connection might be broken - queue for retry
                    std::lock_guard<std::mutex> lock(g_mqtt_queue_mutex);
                    g_mqtt_queue.push_back({topic, payload});
                    log("[MQTT-QUEUE] Message requeued after error (queue size: " + std::to_string(g_mqtt_queue.size()) + ")", LogLevel::ERRORS);
                }
                co_return;
            },
            as::detached);
    });
};




// --------------------------------------------------------------------------------------------


// Write callback for OPC UA node value changes
void
writeCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
              const UA_NodeId *nodeId, void *nodeContext, const UA_NumericRange *range,
              const UA_DataValue *data) {
    if (is_internal_write) return;  

    // Find the topic for this node
    std::string topic;
    {
        std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
        for(const auto &pair : nodeMap) {
            if(UA_NodeId_equal(&pair.second, nodeId)) {
                topic = pair.first;
                break;
            }
        }
    }
    
    if(topic.empty())
        return;  // Not a topic node

    // Only proceed if there is a value to write
    if(data && data->hasValue) {
        json payload;
        json dataPoint = json::object();  // Create empty object to maintain order

        // Handle different types
        if(UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_DOUBLE])) {
            double value = *(UA_Double *)data->value.data;
            dataPoint["Value"] = value;
        } else if(UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_INT32])) {
            int value = *(UA_Int32 *)data->value.data;
            dataPoint["Value"] = value;
        } else if(UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_BOOLEAN])) {
            bool value = *(UA_Boolean *)data->value.data;
            dataPoint["Value"] = value;
        } else if(UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_STRING])) {
            UA_String *str = (UA_String *)data->value.data;
            dataPoint["Value"] = std::string((char *)str->data, str->length);
        } else {
            return;
        }

        {
            std::lock_guard<std::mutex> lock(g_topicMap_mutex);
            if(topicMap.find(topic) != topicMap.end()) {
                // Add metadata to the same dataPoint
                dataPoint["TagId"] = topicMap[topic].tagId;
                dataPoint["TagType"] = topicMap[topic].tagType;
                dataPoint["DatapointId"] = topicMap[topic].tagId;
            } else {
                dataPoint["TagId"] = 0;
                dataPoint["TagType"] = "Unknown";
            }
        }

        // Generate ISO 8601 Timestamp with 100ns precision (High Resolution)
        {            
            auto now = getPreciseTimestamp();

            dataPoint["TimeStamp"] = std::string(now);
        }

        dataPoint["Source"] = (int)AlarmSource::OPC;       // 1 (OPC)
        dataPoint["Quality"] = (int)AlarmQuality::Good;    // 1 (Good)
        dataPoint["UpdateType"] = (int)UpdateType::Telemetry; // 1 (Telemetry)
        dataPoint["InfoId"] = 1001; // Hardcoded

        payload["Data"] = json::array({dataPoint});

        publish_to_mqtt(topic, payload.dump());
    }


}

std::string getConditionEventId(UA_Server *server, UA_NodeId alarmId) {
    // 1. Find the "EventId" property node
    UA_NodeId eventIdProperty = findChildNodeIdAnyNS(server, alarmId, (char*)"EventId");
    
    if(UA_NodeId_isNull(&eventIdProperty)) {
        return ""; // Property not found
    }

    // 2. Read the value
    UA_Variant val;
    UA_Variant_init(&val);
    UA_StatusCode sc = UA_Server_readValue(server, eventIdProperty, &val);

    std::string eventIdStr = "";

    // 3. Extract ByteString and convert to std::string
    if(sc == UA_STATUSCODE_GOOD && val.type == &UA_TYPES[UA_TYPES_BYTESTRING]) {
        UA_ByteString *bs = (UA_ByteString*)val.data;
        // NOTE: EventId is raw binary data, not necessarily a printable string.
        // We copy the raw bytes into a std::string container.
        if(bs->length > 0) {
             eventIdStr = std::string((char*)bs->data, bs->length);
        }
    }

    UA_Variant_clear(&val);
    UA_NodeId_clear(&eventIdProperty);
    
    return eventIdStr;
}


// ============================================================================
// MQTT Subscription Serialization
// ============================================================================
// We must serialize subscription requests to avoid concurrent writes to the socket
// and potentially overloading the client or hitting race conditions.

void process_queue_signal();// Forward declaration

void queue_subscription(const std::string &topic) {
    if(topic.empty()) return;
    
    std::lock_guard<std::mutex> lock(g_sub_mutex);

    // OPTIMIZATION: Check pending unsubscription
    auto jt = std::find(g_unsubscription_queue.begin(), g_unsubscription_queue.end(), topic);
    if(jt != g_unsubscription_queue.end()) {
        g_unsubscription_queue.erase(jt);
        g_subscribed_topics.insert(topic);
        return;
    }

    // Check if already subscribed to avoid unnecessary queueing
    if(g_subscribed_topics.find(topic) != g_subscribed_topics.end()) {
        return;
    }
    
    // Check if already in queue (O(1))
    // If not pending, insert into set and push to queue.
    if(g_pending_subscriptions.insert(topic).second) {
         g_subscription_queue.push_back(topic);
    }
    
    // Trigger processing by signalling the main loop
    process_queue_signal();
    // log("DEBUG: Added to queue and signalled main loop", LogLevel::INFO);
}

// Helper to subscribe to a single topic dynamically
void GlobalMQTT_Subscribe(const std::string &topic) {
   
    queue_subscription(topic);
}
// Helper to subscribe to manual batch
void GlobalMQTT_SubscribeBatch(const std::vector<std::string> &topics) {
    if(topics.empty()) return;
    
    std::lock_guard<std::mutex> lock(g_sub_mutex);
    for(const auto& topic : topics) {
        // OPTIMIZATION: Check if topic is currently pending unsubscription
        // If so, just cancel the unsubscription and mark it as subscribed again.
        // This prevents the "Unsubscribe -> Subscribe" churn during session restart.
        auto unsubIt = std::find(g_unsubscription_queue.begin(), g_unsubscription_queue.end(), topic);
        if(unsubIt != g_unsubscription_queue.end()) {
             g_unsubscription_queue.erase(unsubIt);
             g_subscribed_topics.insert(topic);
             continue; // Skip adding to subscription queue
        }

        // OPTIMIZATION (O(1)): Check if already pending subscription
        if(g_pending_subscriptions.contains(topic)) {
            continue;
        }

        if(g_subscribed_topics.find(topic) == g_subscribed_topics.end()) {
             // Not subscribed and not pending -> Add to queue
             g_subscription_queue.push_back(topic);
             g_pending_subscriptions.insert(topic);
        }
    }
    
    // Trigger ONCE
    process_queue_signal();
}

// Helper to queue unsubscription
void queue_unsubscription(const std::string &topic) {
    if(topic.empty()) return;
    
    std::lock_guard<std::mutex> lock(g_sub_mutex);
    
    // Check if we are actually subscribed
    auto it = g_subscribed_topics.find(topic);
    if(it == g_subscribed_topics.end()) {
        return; // Not subscribed, ignore
    }
    
    // Check if already in unsubscription queue
    if(std::find(g_unsubscription_queue.begin(), g_unsubscription_queue.end(), topic) != g_unsubscription_queue.end()) {
        return;
    }
    
    // Also check if it's currently in the SUBSCRIPTION queue (race condition: sub -> unsub quickly)
    // If so, remove from subscription queue instead of queuing an unsub
    auto subIt = std::find(g_subscription_queue.begin(), g_subscription_queue.end(), topic);
    if(subIt != g_subscription_queue.end()) {
        g_subscription_queue.erase(subIt);
        // log("DEBUG: Cancelled pending subscription for '" + topic + "'", LogLevel::INFO);
        return;
    }

    g_unsubscription_queue.push_back(topic);
    
    // Remove from local tracking immediately to prevent logic from thinking we are still subbed
    g_subscribed_topics.erase(it);
    
    // Trigger processing
    process_queue_signal();
}

// Helper to unsubscribe from manual batch
// Optimizes locking by taking lock once for all topics
// Helper to unsubscribe from manual batch
// Optimizes locking by taking lock once for all topics
void GlobalMQTT_UnsubscribeBatch(const std::vector<std::string> &topics) {
    if(topics.empty()) return;

    std::lock_guard<std::mutex> lock(g_sub_mutex);
    
    // We use a temporary vector to collect topics that actually need an UNSUBSCRIBE packet
    // i.e., they were NOT just cancelled from the subscription queue
    std::vector<std::string> real_unsubs;
    real_unsubs.reserve(topics.size());

    for(const auto& topic : topics) {
        if(topic.empty()) continue;

        // 1. Check SUBSCRIPTION QUEUE (Cancellation Optimization)
        // If it's waiting to be subscribed, just remove it.
        // O(1) Optimization: Check pending set
        if(g_pending_subscriptions.erase(topic)) {
            // We removed it from the authoritative set.
            // It remains in the deque (Zombie), but perform_subscriptions will ignore it.
            continue;
        }

        // 2. Check SUBSCRIBED SET
        auto it = g_subscribed_topics.find(topic);
        if(it != g_subscribed_topics.end()) {
            // It IS subscribed. We must send UNSUBSCRIBE.
            // Check if already in unsubscription queue to prevent dupes
            if(std::find(g_unsubscription_queue.begin(), g_unsubscription_queue.end(), topic) == g_unsubscription_queue.end()) {
                 g_unsubscription_queue.push_back(topic);
                 // Remove from local tracking immediately
                 g_subscribed_topics.erase(it);
            }
        } 
        
        // 3. What if it's "In Flight"? (Not in Queue, Not in Set)
        // We can't easily detect this. We skip it.
        // If it eventually connects, it will be a "Zombie" subscription.
        // Ideally we would track "In Flight".
    }
    
    process_queue_signal();
}

// Helper to unsubscribe from a single topic dynamically
void GlobalMQTT_Unsubscribe(const std::string &topic) {
    queue_unsubscription(topic);
}

// ----------------------------------------------------------------------------
// Subscription Loop Helpers
// ----------------------------------------------------------------------------

void process_queue_signal() {
    // Notify the main loop to wake up and check the queue
    // Must execute on IO thread for thread-safety (called from Worker threads)
    as::post(ioc, [](){
        g_signal_pending.store(true); // Signal intention to wake up
        if(g_sub_timer) {
            std::size_t n = g_sub_timer->cancel();
            // log("DEBUG: [IO_THREAD] Cancelling timer for signal. Cancelled: " + std::to_string(n), LogLevel::INFO);
        } else {
            // log("DEBUG: [IO_THREAD] g_sub_timer is NULL!", LogLevel::ERRORS);
        }
    });
}

// Subscribe logic moves inside the loop controller
as::awaitable<void> perform_subscriptions() {
    // Safety: Do not attempt to send packets if not connected
    if(!g_mqtt_connected.load()) {
        co_return; // Exit coroutine immediately
    }
    
    constexpr size_t MAX_BATCH = 2000;

    /* ============================================================
     * 1. Process UNSUBSCRIPTIONS first
     * ============================================================ */
    std::vector<std::string> unsub_batch;
    unsub_batch.reserve(MAX_BATCH * 2);

    {
        std::lock_guard<std::mutex> lock(g_sub_mutex);
        size_t count = 0;

        while(!g_unsubscription_queue.empty() && count < MAX_BATCH) {
            const std::string topic = std::move(g_unsubscription_queue.front());
            g_unsubscription_queue.pop_front();

            // Only unsubscribe topics we actually subscribed to
            if(g_subscribed_topics.erase(topic)) {
                unsub_batch.emplace_back(topic);
            }

            const std::string event_topic = topic + "/Event";
            if(g_subscribed_topics.erase(event_topic)) {
                unsub_batch.emplace_back(event_topic);
            }

            ++count;
        }
    }

    if(!unsub_batch.empty()) {
        try {
            std::vector<am::topic_sharename> final_unsub;
            final_unsub.reserve(unsub_batch.size());

            for(const auto &t : unsub_batch) {
                final_unsub.emplace_back(t);
            }

            if(g_use_tls) {
                co_await amcl_s.async_unsubscribe(
                    am::v5::unsubscribe_packet{*amcl_s.acquire_unique_packet_id(),
                                               am::force_move(final_unsub)},
                    as::use_awaitable);
            } else {
                co_await amcl_w.async_unsubscribe(
                    am::v5::unsubscribe_packet{*amcl_w.acquire_unique_packet_id(),
                                               am::force_move(final_unsub)},
                    as::use_awaitable);
            }

            log("✓ Unsubscribed from " + std::to_string(unsub_batch.size()) +
                    " topics",
                LogLevel::INFO);

        } catch(const std::exception &e) {
            log("ERROR: Unsubscription batch failed: " + std::string(e.what()),
                LogLevel::ERRORS);
        }
    }


    /* ============================================================
     * 2. Process SUBSCRIPTIONS
     * ============================================================ */
    std::vector<std::string> sub_batch;
    sub_batch.reserve(MAX_BATCH);

    {
        std::lock_guard<std::mutex> lock(g_sub_mutex);
        size_t count = 0;

        while(!g_subscription_queue.empty() && count < MAX_BATCH) {
            std::string topic = std::move(g_subscription_queue.front());
            g_subscription_queue.pop_front();
            
            // O(1) Check: Is it still pending?
            if(g_pending_subscriptions.erase(topic) > 0) {
                 sub_batch.emplace_back(std::move(topic));
                 ++count;
            }
            // Else: It was cancelled (removed from set), so we drop it (Zombie).
        }
    }

    if(sub_batch.empty()) {
        co_return;
    }

    std::vector<am::topic_subopts> sub_entries;
    sub_entries.reserve(sub_batch.size() * 2);

    {
        InstrumentedGuard alarmLock(g_alarmMutex);

        for(const auto &topic : sub_batch) {
            sub_entries.emplace_back(topic, am::qos::at_most_once);

            // Subscribe to /Event ONLY if this topic is an alarm trigger
            if(g_triggerToAlarmMap.contains(topic)) {
                sub_entries.emplace_back(topic + "/Event", am::qos::at_most_once);
            }
        }
    }

    try {
        if(g_use_tls) {
            co_await amcl_s.async_subscribe(
                am::v5::subscribe_packet{*amcl_s.acquire_unique_packet_id(),
                                         am::force_move(sub_entries)},
                as::use_awaitable);
        } else {
            co_await amcl_w.async_subscribe(
                am::v5::subscribe_packet{*amcl_w.acquire_unique_packet_id(),
                                         am::force_move(sub_entries)},
                as::use_awaitable);
        }

        {
            std::lock_guard<std::mutex> lock(g_sub_mutex);
            for(const auto &topic : sub_batch) {
                g_subscribed_topics.insert(topic);
                // log("DEBUG: Subscribing to: " + topic, LogLevel::INFO);
                if(g_triggerToAlarmMap.contains(topic)) {
                    g_subscribed_topics.insert(topic + "/Event");
                    log("DEBUG: Subscribing to/Event: " + topic + "/Event", LogLevel::INFO);
                }
            }

            log("✓ Subscribed to " + std::to_string(sub_batch.size()) + " topics",
                LogLevel::INFO);
        }

    } catch(const std::exception &e) {
        log("ERROR: Subscription batch failed: " + std::string(e.what()),
            LogLevel::ERRORS);
    }
}

// Forward declaration for MQTT packet processing
// This function handles both alarm and telemetry messages
void process_mqtt_packet(UA_Server* server, const std::string& topic, const std::string& payload);

void start_mqtt_client(UA_Server *server) {
    // Use global ioc and amcl
    as::co_spawn(
        ioc,
        [server]() -> as::awaitable<void> {
            log("DEBUG: MQTT Client Coroutine Started!", LogLevel::INFO);
            // Reconnection loop
            while(running) {
                // FORCE RESET CLIENT to clear any "packet_not_allowed" or stale state
                // This resolves the infinite error loop 388 on reconnect.
                if(g_use_tls) {
                    amcl_s = client_st{ioc.get_executor(), mqtt_ssl_ctx};
                } else {
                    amcl_w = client_wt{ioc.get_executor()};
                }
                
                try {
                    // Refresh the JWT token before every connect attempt.
                    // This prevents permanent not_authorized when the token expires.
                    try {
                        log("MQTT token refresh: fetching new bearer token ...", LogLevel::INFO);
                        json tokenResp = getBearerToken(g_apiHost, g_apiPort,
                                                        g_authUsername, g_authPassword);
                        if(tokenResp.contains("access_token")) {
                            std::string freshToken = tokenResp["access_token"].get<std::string>();
                            g_bearerToken    = freshToken;
                            nlohmann::json pw;
                            pw["token"]      = freshToken;
                            g_mqtt_password  = pw.dump();
                            log("MQTT token refresh: new token obtained.", LogLevel::INFO);
                        } else {
                            log("MQTT token refresh: no access_token in response, using existing password.", LogLevel::WARNING);
                        }
                    } catch(const std::exception& e) {
                        log("MQTT token refresh failed: " + std::string(e.what()) + " — using existing password.", LogLevel::WARNING);
                    }

                    log("Attempting to connect to MQTT broker: " + g_broker_address + ":" + std::to_string(g_broker_port) + " (TLS: " + (g_use_tls ? "true" : "false") + ")", LogLevel::INFO);
                    
                    if(g_use_tls) {
                        co_await amcl_s.async_underlying_handshake(g_broker_address, std::to_string(g_broker_port), as::use_awaitable);
                        auto connack_opt = co_await amcl_s.async_start(
                            am::v5::connect_packet{ true, 0x1234, g_mqtt_client_id, std::nullopt, g_mqtt_username, g_mqtt_password },
                            as::use_awaitable);
                        if(!connack_opt) throw std::runtime_error("Failed to start MQTTS session");
                    } else {
                        co_await amcl_w.async_underlying_handshake(g_broker_address, std::to_string(g_broker_port), as::use_awaitable);
                        auto connack_opt = co_await amcl_w.async_start(
                            am::v5::connect_packet{ true, 0x1234, g_mqtt_client_id, std::nullopt, g_mqtt_username, g_mqtt_password },
                            as::use_awaitable);
                        if(!connack_opt) throw std::runtime_error("Failed to start MQTT session");
                    }
                    

                    log("Successfully connected.", LogLevel::INFO);
                    g_mqtt_connected.store(true);
                                        // Clear and queue
                    {
                        InstrumentedGuard alarmLock(g_alarmMutex);
                        std::lock_guard<std::mutex> lock(g_sub_mutex);
                        g_subscribed_topics.clear(); 
                        g_pending_subscriptions.clear(); // Ensure clean state
                        
                        // Re-queue Alarms
                        if(!g_triggerToAlarmMap.empty()) {
                            log("DEBUG: Re-queueing " + std::to_string(g_triggerToAlarmMap.size()) + " alarm topics", LogLevel::INFO);
                            for(const auto &pair : g_triggerToAlarmMap) {
                                g_subscription_queue.push_back(pair.first);
                                g_pending_subscriptions.insert(pair.first); // REQUIRED
                            }
                        }
                        
                        // Re-queue Generic Telemetry
                        {
                            std::lock_guard<std::mutex> nodeLock(g_nodeMap_mutex);
                            if(!nodeMap.empty()) {
                                log("DEBUG: Re-queueing " + std::to_string(nodeMap.size()) + " generic topics", LogLevel::INFO);
                                for(const auto &pair : nodeMap) {
                                    g_subscription_queue.push_back(pair.first);
                                    g_pending_subscriptions.insert(pair.first); // REQUIRED
                                }
                            }
                        }
                    }
                    
                    // Initialize Timer
                    g_sub_timer = std::make_unique<as::steady_timer>(ioc);

                    // Concurrent Receive and Subscribe Tasks to avoid cancellation deadlock
                    // Task 1: Receiver (Dedicated to incoming packets)
                    auto recv_task = [server]() -> as::awaitable<void> {
                        log("DEBUG: Receive Loop Started", LogLevel::INFO);
                        while(running && g_mqtt_connected.load()) {
                             if(g_use_tls) {
                                 auto pv_opt = co_await amcl_s.async_recv(as::use_awaitable);
                                 if(!pv_opt) {
                                     log("DEBUG: MQTT disconnected in Recv Loop", LogLevel::INFO);
                                     throw std::runtime_error("Disconnected");
                                 }
                                 pv_opt->visit(am::overload{
                                    [&](client_st::publish_packet &p) {
                                    std::string topic = p.topic();
                                    std::string payload = p.payload();

                                    if (payload.empty())
                                    {
                                        log("Empty payload received on topic: " + topic, LogLevel::INFO);
                                        return;
                                    }

                                    /* Check if this is a trigger topic for alarm conditions (.alarm.pub suffix) */
                                    std::string baseTopic = topic;
                                    
                                    // DEBUG LOGGING FOR ALARMS
                                    if(topic.find("Event") != std::string::npos || topic.find("Alarm") != std::string::npos) {
                                         log("DEBUG: MQTT Recv: '" + topic + "' Payload: " + (payload.size() > 50 ? payload.substr(0,50) + "..." : payload), LogLevel::INFO);
                                    }

                                    // Check if topic ends with /Event and extract base topic
                                    if(topic.size() > 6 && topic.rfind("/Event") == topic.size() - 6) {
                                        baseTopic = topic.substr(0, topic.size() - 6);
                                        // log("DEBUG: Detected /Event topic, base='" + baseTopic + "'", LogLevel::INFO);
                                    }

                                    // Thread-Safe Lookup: Copy mappings to local vector under lock
                                    std::vector<TriggerToAlarmMapping> mappings;
                                    {
                                        InstrumentedGuard lock(g_alarmMutex);
                                        auto triggerIt = g_triggerToAlarmMap.find(baseTopic);
                                        if(triggerIt != g_triggerToAlarmMap.end()) {
                                            mappings = triggerIt->second;
                                        }
                                    }

                                    bool isAlarmEvent = false;
                                    if(!mappings.empty()) {
                                        try {
                                            auto check = json::parse(payload);
                                            if(check.contains("Event")) isAlarmEvent = true;
                                        } catch(...) {}
                                    } else {
                                        // log("DEBUG: Mappings vector is empty for topic: " + baseTopic, LogLevel::DEBUG);
                                    }
                                    
                                    if(isAlarmEvent) {
                                        // This is a trigger topic, process the alarm payload
                                        try {
                                            auto alarmPayload = json::parse(payload);
                                            
                                            if(alarmPayload.contains("Event")) {
                                                log("DEBUG: 'Event' field found. Extracting data...", LogLevel::INFO);
                                                auto &alarm = alarmPayload["Event"];
                                                
                                                // Extract AeInstanceID GUID
                                                std::string aeInstanceId = "";
                                                if(alarm.contains("AeInstanceID")) {
                                                    if(alarm["AeInstanceID"].is_string()) {
                                                        aeInstanceId = alarm["AeInstanceID"].get<std::string>();
                                                    } else if(alarm["AeInstanceID"].is_number()) {
                                                        aeInstanceId = std::to_string(alarm["AeInstanceID"].get<int>());
                                                    } else if(!alarm["AeInstanceID"].is_null()) {
                                                        aeInstanceId = alarm["AeInstanceID"].dump();
                                                    }
                                                }
                                                
                                                // Extract AeTypeID
                                                int AETypeID = 0;
                                                if(alarm.contains("AeTypeID")) {
                                                    if(alarm["AeTypeID"].is_string()) {
                                                        try {
                                                            AETypeID = std::stoi(alarm["AeTypeID"].get<std::string>());
                                                        } catch(...) { log("DEBUG: Failed to convert AeTypeID string to int", LogLevel::INFO); }
                                                    } else {
                                                        AETypeID = alarm["AeTypeID"].get<int>();
                                                    }
                                                }
                                                
                                                bool active = alarm.value("Active", false);
                                                bool enabled = alarm.value("Enabled", true);
                                                bool shelved = alarm.value("Shelved", false);
                                                bool acked = alarm.value("Acked", false);
                                                bool confirmed = alarm.value("Confirmed", false);
                                                
                                                UA_UInt16 severity = static_cast<UA_UInt16>(alarm.value("Severity", 500));
                                                std::string alarmMessage = alarm.value("AlarmMessage", "Alarm triggered");
                                                std::string alarmName = alarm.value("Name", "");
                                                std::string comment = alarm.value("Comment", "");
                                                
                                                // Source/Quality/UpdateType enums
                                                int qualityEnumValue = alarm.value("Quality", 1); // 1=Good
                                                
                                                AlarmQuality qualityEnum = intToAlarmQuality(qualityEnumValue);
                                                std::string quality = alarmQualityToString(qualityEnum);
                                                
                                                bool retain = alarm.value("Retain", false);

                                                // Populate Job Data
                                                AlarmJobData jobData;
                                                jobData.AETypeID = AETypeID;
                                                jobData.aeInstanceId = aeInstanceId;
                                                jobData.active = active;
                                                jobData.enabled = enabled;
                                                jobData.shelved = shelved;
                                                jobData.acked = acked;
                                                jobData.confirmed = confirmed;
                                                jobData.retain = retain;
                                                jobData.severity = severity;
                                                jobData.alarmMessage = alarmMessage;
                                                jobData.alarmName = alarmName;
                                                jobData.comment = comment;
                                                jobData.now = UA_DateTime_now(); 
                                                jobData.quality = quality;
                                                jobData.qualityCode = (quality == "Good") ? UA_STATUSCODE_GOOD : UA_STATUSCODE_BAD;
                                                jobData.mappings = mappings;

                                                
                                                enqueueServerJob([jobData](UA_Server* server) {
                                                    for(const auto &mapping : jobData.mappings) {
                                                        if(mapping.alarmId != jobData.AETypeID) {
                                                             log("DEBUG: AlarmID Mismatch! Mapping ID: " + std::to_string(mapping.alarmId) + " != Payload ID: " + std::to_string(jobData.AETypeID), LogLevel::INFO);
                                                             continue;
                                                        }
                                                        
                                                        // 1. Find Alarm Node (using global map safely on server thread)
                                                        UA_NodeId alarmId = UA_NODEID_NULL;
                                                        {
                                                            InstrumentedGuard lock(
                                                                g_alarmMutex);
                                                            if(g_alarmByKey.count(mapping.alarmKey)) {
                                                                alarmId = g_alarmByKey[mapping.alarmKey];
                                                            } else {
                                                                 log("INFO: Alarm Key not found in global map: " + mapping.alarmKey, LogLevel::INFO);
                                                            }
                                                        }
                                                        
                                                        // LOG: Log the parsed job data to verify inputs
                                                        log("INFO: Updating Alarm '" + mapping.alarmKey + 
                                                            "' Active=" + std::to_string(jobData.active) + 
                                                            " Acked=" + std::to_string(jobData.acked) + 
                                                            " Confirmed=" + std::to_string(jobData.confirmed) +
                                                            " Retain=" + std::to_string(jobData.retain), LogLevel::INFO);


                                                        if(UA_NodeId_isNull(&alarmId)) {
                                                             // Only log periodically if needed, silent failure for now to avoid spam
                                                             continue; 
                                                        }

                                                        // =========================================================
                                                        // STEP 1: Handle Branching (SET THIS FIRST!)
                                                        // =========================================================
                                                        // We set BranchId BEFORE updating properties so that any internal events
                                                        // (if they occur) are associated with the correct Branch.
                                                        
                                                        UA_NodeId branchNodeId = UA_NODEID_NULL;
                                                        UA_NodeId_copy(&alarmId, &branchNodeId); // Default to alarm itself if no branch

                                                        if(!jobData.aeInstanceId.empty() && jobData.aeInstanceId != "0") {
                                                            // Logic to get/create branch
                                                            {
                                                                InstrumentedGuard mapLock (g_alarmMutex);  // Ensure
                                                                                    // thread
                                                                                    // safety
                                                                                    // for
                                                                                    // map
                                                                                    // access
                                                                UA_StatusCode sc = getOrCreateAlarmBranch(server, alarmId, jobData.aeInstanceId, mapping.alarmKey, &branchNodeId);
                                                                if(sc != UA_STATUSCODE_GOOD) {
                                                                     // cleanup
                                                                     UA_NodeId_clear(&branchNodeId);
                                                                     continue;
                                                                }
                                                            } 
                                                            
                                                            // Set BranchId on the Alarm Node
                                                            setStealthValueByPath(server, alarmId, {"BranchId"}, &branchNodeId, &UA_TYPES[UA_TYPES_NODEID]);
                                                        } else {
                                                            // Explicitly set BranchId to Null (Aggregate)
                                                            UA_NodeId nullId = UA_NODEID_NULL;
                                                            setStealthValueByPath(server, alarmId, {"BranchId"}, &nullId, &UA_TYPES[UA_TYPES_NODEID]);
                                                        }

                                                        // =========================================================
                                                        // STEP 2: Update Alarm Properties
                                                        // =========================================================
                                                        // CRITICAL ORDERING CHANGE:
                                                        // Update Severity FIRST. Changing Severity often triggers a reset of 
                                                        // "AckedState" or "ConfirmedState" in the SDK logic (requiring re-ack).
                                                        // By doing this first, we allow the reset to happen, and THEN we overwrite
                                                        // correctly with the payload values in Group A.
                                                        
                                                        // --- GROUP B: RETAIN SANDWICH (SEVERITY ONLY) ---
                                                        // Moved to TOP
                                                        UA_Boolean bRetainFalse = UA_FALSE;
                                                        UA_Boolean bRetainTrue = jobData.retain; 

                                                        // 1. Set Retain = False
                                                        setStealthValueByPath(server, alarmId, {"Retain"}, &bRetainFalse, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        // 2. Update Severity
                                                        UA_UInt16 sev = jobData.severity;
                                                        setStealthValueByPath(server, alarmId, {"Severity"}, &sev, &UA_TYPES[UA_TYPES_UINT16]);

                                                        // 3. Restore Retain (Set Retain = True)
                                                        setStealthValueByPath(server, alarmId, {"Retain"}, &bRetainTrue, &UA_TYPES[UA_TYPES_BOOLEAN]);

                                                        // --- GROUP A: DEDUPLICATION ONLY (Direct Updates) ---
                                                        // Now runs AFTER severity update
                                                        
                                                        // ActiveState
                                                        UA_Boolean bAct = jobData.active;
                                                        UA_LocalizedText tAct = bAct ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Active") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Inactive");
                                                        setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &bAct, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        setStealthValueByPath(server, alarmId, {"ActiveState"}, &tAct, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                                                        // EnabledState
                                                        UA_Boolean bEnabled = jobData.enabled;
                                                        UA_LocalizedText tEnabled = bEnabled ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Enabled") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Disabled");
                                                        setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &bEnabled, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        setStealthValueByPath(server, alarmId, {"EnabledState"}, &tEnabled, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        
                                                        // AckedState
                                                        UA_Boolean bAck = jobData.acked;
                                                        UA_LocalizedText tAck = bAck ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Acknowledged") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unacknowledged");
                                                        UA_StatusCode scAckId = setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &bAck, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        UA_StatusCode scAckVal = setStealthValueByPath(server, alarmId, {"AckedState"}, &tAck, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        
                                                        if(scAckId != UA_STATUSCODE_GOOD || scAckVal != UA_STATUSCODE_GOOD) {
                                                            log("ERROR: Failed to update AckedState! IdSC: " + std::string(UA_StatusCode_name(scAckId)) + " ValSC: " + std::string(UA_StatusCode_name(scAckVal)), LogLevel::ERRORS);
                                                        }

                                                        // ConfirmedState
                                                        UA_Boolean bConf = jobData.confirmed;
                                                        UA_LocalizedText tConf = bConf ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Confirmed") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unconfirmed");
                                                        UA_StatusCode scConfId = setStealthValueByPath(server, alarmId, {"ConfirmedState", "Id"}, &bConf, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        UA_StatusCode scConfVal = setStealthValueByPath(server, alarmId, {"ConfirmedState"}, &tConf, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                                                        if(scConfId != UA_STATUSCODE_GOOD || scConfVal != UA_STATUSCODE_GOOD) {
                                                            log("ERROR: Failed to update ConfirmedState! IdSC: " + std::string(UA_StatusCode_name(scConfId)) + " ValSC: " + std::string(UA_StatusCode_name(scConfVal)), LogLevel::ERRORS);
                                                        }

                                                        // Message
                                                        UA_LocalizedText msg = UA_LOCALIZEDTEXT((char*)"en", (char*)jobData.alarmMessage.c_str());
                                                        setStealthValueByPath(server, alarmId, {"Message"}, &msg, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                                                        // Comment
                                                        if(!jobData.comment.empty()) {
                                                            UA_LocalizedText comment = UA_LOCALIZEDTEXT((char*)"en", (char*)jobData.comment.c_str());
                                                            setStealthValueByPath(server, alarmId, {"Comment"}, &comment, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        }
                                                        
                                                        // =========================================================
                                                        // STEP 3: Trigger Event
                                                        // =========================================================
                                                        // Find Source Node
                                                        std::string emitterName = mapping.alarmKey.substr(0, mapping.alarmKey.find("-"));
                                                        UA_NodeId sourceNode = alarmId; 
                                                        {
                                                             std::lock_guard<std::mutex> mapLock(g_nodeMap_mutex);
                                                             if(nodeMap.count(emitterName)) sourceNode = nodeMap[emitterName];
                                                        }
                                                        
                                                        UA_ByteString eventId = UA_BYTESTRING_NULL;
                                                        UA_Server_triggerConditionEvent(server, alarmId, sourceNode, &eventId);
                                                        
                                                        // Store EventId
                                                        if(eventId.length > 0 && !jobData.aeInstanceId.empty()) {
                                                            g_branchStates[mapping.alarmKey][jobData.aeInstanceId].addEventId(&eventId);
                                                        }
                                                        UA_ByteString_clear(&eventId);
                                                        
                                                        // 5. Cleanup Branches
                                                        cleanupBranches(mapping.alarmKey);
                                                        
                                                        UA_NodeId_clear(&branchNodeId);
                                                    }
                                                }, ServerJobType::SetEventNotifier);

                                            } // End if(alarm.contains("AeTypeID")) logic??
                                            // Actually, this block started with checking "mappings".
                                            // The replacement covers the logic inside "if(!isAlarmEvent)" else block? 
                                            // No, this is inside "if(isAlarmEvent)".

                                        } catch(const std::exception& e) { log("JSON/Processing Error: " + std::string(e.what()), LogLevel::ERRORS); }
                                        is_internal_write = false; // 🔓 Reset callback loop
                                    }

                                    if(!isAlarmEvent) {
                                        /* Regular data update path (Generic Telemetry) */
                                        try {
                                            // Parse the incoming MQTT payload
                                            auto j = json::parse(payload);

                                            // 1. Handle the "Data" array format (New Structure)
                                            if (j.contains("Data") && j["Data"].is_array()) {
                                                for (auto& entry : j["Data"]) {
                                                    // Determine the topic/node. Usually, the 'topic' variable from MQTT 
                                                    // is our key, but if your JSON provides a 'TagId' mapping, 
                                                    // you might need to look up by ID instead.
                                                    UA_NodeId nodeId = UA_NODEID_NULL;
                                                    {
                                                        std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                                                        auto it = nodeMap.find(topic);
                                                        if(it != nodeMap.end()) nodeId = it->second;
                                                    }

                                                    if(UA_NodeId_isNull(&nodeId)) continue;

                                                    // Handle the "Value" field which could be Array, Number, or Boolean
                                                    if (entry.contains("Value")) {
                                                        auto& valField = entry["Value"];

                                                        if (valField.is_array()) {
                                                            // CASE: Array of Values (e.g., [1211, 1211, ...])
                                                            std::vector<double> values;
                                                            for (auto& v : valField) {
                                                                if (v.is_number()) values.push_back(v.get<double>());
                                                            }

                                                            if (!values.empty()) {
                                                                enqueueServerJob([nodeId, values](UA_Server* server) {
                                                                    ScopedVariant myVar;
                                                                    // UA_Variant_setArrayCopy handles allocating the UA array and copying data
                                                                    UA_Variant_setArrayCopy(myVar.get(), values.data(), values.size(), &UA_TYPES[UA_TYPES_DOUBLE]);
                                                                    
                                                                    is_internal_write = true;
                                                                    UA_Server_writeValue(server, nodeId, myVar.var);
                                                                    is_internal_write = false;
                                                                }, ServerJobType::WriteValue);
                                                            }
                                                        } 
                                                        else if (valField.is_number()) {
                                                            // CASE: Scalar Number
                                                            double val = valField.get<double>();
                                                            enqueueServerJob([nodeId, val](UA_Server* server) {
                                                                ScopedVariant myVar;
                                                                UA_Variant_setScalarCopy(myVar.get(), &val, &UA_TYPES[UA_TYPES_DOUBLE]);
                                                                is_internal_write = true;
                                                                UA_Server_writeValue(server, nodeId, myVar.var);
                                                                is_internal_write = false;
                                                            }, ServerJobType::WriteValue);
                                                        }
                                                        else if (valField.is_boolean()) {
                                                            // CASE: Scalar Boolean
                                                            UA_Boolean val = valField.get<bool>() ? UA_TRUE : UA_FALSE;
                                                            enqueueServerJob([nodeId, val](UA_Server* server) {
                                                                ScopedVariant myVar;
                                                                UA_Variant_setScalarCopy(myVar.get(), &val, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                                is_internal_write = true;
                                                                UA_Server_writeValue(server, nodeId, myVar.var);
                                                                is_internal_write = false;
                                                            }, ServerJobType::WriteValue);
                                                        }
                                                    }
                                                }
                                            }
                                            // 2. Fallback for the Old/Simple JSON format (Direct Object)
                                            else {
                                                UA_NodeId nodeId = UA_NODEID_NULL;
                                                {
                                                    std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                                                    auto it = nodeMap.find(topic);
                                                    if(it != nodeMap.end()) nodeId = it->second;
                                                }

                                                if(!UA_NodeId_isNull(&nodeId)) {
                                                    // Iterate through keys in the root object (e.g., {"Temperature": 22.5})
                                                    for (auto& [key, value] : j.items()) {
                                                        if (value.is_number()) {
                                                            double v = value.get<double>();
                                                            enqueueServerJob([nodeId, v](UA_Server* server) {
                                                                ScopedVariant myVar;
                                                                UA_Variant_setScalarCopy(myVar.get(), &v, &UA_TYPES[UA_TYPES_DOUBLE]);
                                                                is_internal_write = true;
                                                                UA_Server_writeValue(server, nodeId, myVar.var);
                                                                is_internal_write = false;
                                                            }, ServerJobType::WriteValue);
                                                        }
                                                        else if (value.is_boolean()) {
                                                            UA_Boolean v = value.get<bool>() ? UA_TRUE : UA_FALSE;
                                                            enqueueServerJob([nodeId, v](UA_Server* server) {
                                                                ScopedVariant myVar;
                                                                UA_Variant_setScalarCopy(myVar.get(), &v, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                                is_internal_write = true;
                                                                UA_Server_writeValue(server, nodeId, myVar.var);
                                                                is_internal_write = false;
                                                            }, ServerJobType::WriteValue);
                                                        }
                                                    }
                                                }
                                            }
                                        } catch (const std::exception& e) {
                                            log("Generic Telemetry Parser Error: " + std::string(e.what()), LogLevel::ERRORS);
                                        }
                                    }
                                },
                                [](auto const&) {}
                             });
                             } else {
                                 // Non-TLS receive
                                 auto pv_opt = co_await amcl_w.async_recv(as::use_awaitable);
                                 if(!pv_opt) {
                                     log("DEBUG: MQTT disconnected in Recv Loop", LogLevel::INFO);
                                     throw std::runtime_error("Disconnected");
                                 }
                                 pv_opt->visit(am::overload{
                                    [&](client_wt::publish_packet &p) {
                                        std::string topic = p.topic();
                                        std::string payload = p.payload();

                                        if (payload.empty()){
                                            log("Empty payload received on topic: " + topic, LogLevel::INFO);
                                            return;
                                        }

                                        /* Check if this is a trigger topic for alarm conditions (.alarm.pub suffix) */
                                        std::string baseTopic = topic;
                                        
                                        // DEBUG LOGGING FOR ALARMS
                                        if(topic.find("Event") != std::string::npos || topic.find("Alarm") != std::string::npos) {
                                             log("DEBUG: MQTT Recv: '" + topic + "' Payload: " + (payload.size() > 50 ? payload.substr(0,50) + "..." : payload), LogLevel::INFO);
                                        }

                                        // Check if topic ends with /Event and extract base topic
                                        if(topic.size() > 6 && topic.rfind("/Event") == topic.size() - 6) {
                                            baseTopic = topic.substr(0, topic.size() - 6);
                                        }

                                        // Thread-Safe Lookup: Copy mappings to local vector under lock
                                        std::vector<TriggerToAlarmMapping> mappings;
                                        {
                                            InstrumentedGuard lock(g_alarmMutex);
                                            auto triggerIt = g_triggerToAlarmMap.find(baseTopic);
                                            if(triggerIt != g_triggerToAlarmMap.end()) {
                                                mappings = triggerIt->second;
                                            }
                                        }

                                        bool isAlarmEvent = false;
                                        if(!mappings.empty()) {
                                            try {
                                                auto check = json::parse(payload);
                                                if(check.contains("Event")) isAlarmEvent = true;
                                            } catch(...) {}
                                        }
                                        
                                        if(isAlarmEvent) {
                                            // This is a trigger topic, process the alarm payload
                                            try {
                                                auto alarmPayload = json::parse(payload);
                                                
                                                if(alarmPayload.contains("Event")) {
                                                    log("DEBUG: 'Event' field found. Extracting data...", LogLevel::INFO);
                                                    auto &alarm = alarmPayload["Event"];
                                                    
                                                    // Extract AeInstanceID GUID
                                                    std::string aeInstanceId = "";
                                                    if(alarm.contains("AeInstanceID")) {
                                                        if(alarm["AeInstanceID"].is_string()) {
                                                            aeInstanceId = alarm["AeInstanceID"].get<std::string>();
                                                        } else if(alarm["AeInstanceID"].is_number()) {
                                                            aeInstanceId = std::to_string(alarm["AeInstanceID"].get<int>());
                                                        } else if(!alarm["AeInstanceID"].is_null()) {
                                                            aeInstanceId = alarm["AeInstanceID"].dump();
                                                        }
                                                    }
                                                    
                                                    // Extract AeTypeID
                                                    int AETypeID = 0;
                                                    if(alarm.contains("AeTypeID")) {
                                                        if(alarm["AeTypeID"].is_string()) {
                                                            try {
                                                                AETypeID = std::stoi(alarm["AeTypeID"].get<std::string>());
                                                            } catch(...) { log("DEBUG: Failed to convert AeTypeID string to int", LogLevel::INFO); }
                                                        } else {
                                                            AETypeID = alarm["AeTypeID"].get<int>();
                                                        }
                                                    }
                                                    
                                                    bool active = alarm.value("Active", false);
                                                    bool enabled = alarm.value("Enabled", true);
                                                    bool shelved = alarm.value("Shelved", false);
                                                    bool acked = alarm.value("Acked", false);
                                                    bool confirmed = alarm.value("Confirmed", false);
                                                    
                                                    UA_UInt16 severity = static_cast<UA_UInt16>(alarm.value("Severity", 500));
                                                    std::string alarmMessage = alarm.value("AlarmMessage", "Alarm triggered");
                                                    std::string alarmName = alarm.value("Name", "");
                                                    std::string comment = alarm.value("Comment", "");
                                                    
                                                    // Source/Quality/UpdateType enums
                                                    int qualityEnumValue = alarm.value("Quality", 1); // 1=Good
                                                    
                                                    AlarmQuality qualityEnum = intToAlarmQuality(qualityEnumValue);
                                                    std::string quality = alarmQualityToString(qualityEnum);
                                                    
                                                    bool retain = alarm.value("Retain", false);

                                                    // Populate Job Data
                                                    AlarmJobData jobData;
                                                    jobData.AETypeID = AETypeID;
                                                    jobData.aeInstanceId = aeInstanceId;
                                                    jobData.active = active;
                                                    jobData.enabled = enabled;
                                                    jobData.shelved = shelved;
                                                    jobData.acked = acked;
                                                    jobData.confirmed = confirmed;
                                                    jobData.retain = retain;
                                                    jobData.severity = severity;
                                                    jobData.alarmMessage = alarmMessage;
                                                    jobData.alarmName = alarmName;
                                                    jobData.comment = comment;
                                                    jobData.now = UA_DateTime_now(); 
                                                    jobData.quality = quality;
                                                    jobData.qualityCode = (quality == "Good") ? UA_STATUSCODE_GOOD : UA_STATUSCODE_BAD;
                                                    jobData.mappings = mappings;

                                                    
                                                    enqueueServerJob([jobData](UA_Server* server) {
                                                        for(const auto &mapping : jobData.mappings) {
                                                            if(mapping.alarmId != jobData.AETypeID) {
                                                                 log("DEBUG: AlarmID Mismatch! Mapping ID: " + std::to_string(mapping.alarmId) + " != Payload ID: " + std::to_string(jobData.AETypeID), LogLevel::INFO);
                                                                 continue;
                                                            }
                                                            
                                                            // 1. Find Alarm Node (using global map safely on server thread)
                                                            UA_NodeId alarmId = UA_NODEID_NULL;
                                                            {
                                                                InstrumentedGuard lock(g_alarmMutex);
                                                                if(g_alarmByKey.count(mapping.alarmKey)) {
                                                                    alarmId = g_alarmByKey[mapping.alarmKey];
                                                                } else {
                                                                     log("INFO: Alarm Key not found in global map: " + mapping.alarmKey, LogLevel::INFO);
                                                                }
                                                            }
                                                            
                                                            // LOG: Log the parsed job data to verify inputs
                                                            log("INFO: Updating Alarm '" + mapping.alarmKey + 
                                                                "' Active=" + std::to_string(jobData.active) + 
                                                                " Acked=" + std::to_string(jobData.acked) + 
                                                                " Confirmed=" + std::to_string(jobData.confirmed) +
                                                                " Retain=" + std::to_string(jobData.retain), LogLevel::INFO);


                                                            if(UA_NodeId_isNull(&alarmId)) {
                                                                 continue; 
                                                            }

                                                            // STEP 1: Handle Branching
                                                            UA_NodeId branchNodeId = UA_NODEID_NULL;
                                                            UA_NodeId_copy(&alarmId, &branchNodeId);

                                                            if(!jobData.aeInstanceId.empty() && jobData.aeInstanceId != "0") {
                                                                {
                                                                    InstrumentedGuard mapLock(g_alarmMutex);
                                                                    UA_StatusCode sc = getOrCreateAlarmBranch(server, alarmId, jobData.aeInstanceId, mapping.alarmKey, &branchNodeId);
                                                                    if(sc != UA_STATUSCODE_GOOD) {
                                                                         UA_NodeId_clear(&branchNodeId);
                                                                         continue;
                                                                    }
                                                                } 
                                                                setStealthValueByPath(server, alarmId, {"BranchId"}, &branchNodeId, &UA_TYPES[UA_TYPES_NODEID]);
                                                            } else {
                                                                UA_NodeId nullId = UA_NODEID_NULL;
                                                                setStealthValueByPath(server, alarmId, {"BranchId"}, &nullId, &UA_TYPES[UA_TYPES_NODEID]);
                                                            }

                                                            // STEP 2: Update Alarm Properties
                                                            UA_Boolean bRetainFalse = UA_FALSE;
                                                            UA_Boolean bRetainTrue = jobData.retain; 
                                                            setStealthValueByPath(server, alarmId, {"Retain"}, &bRetainFalse, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                            UA_UInt16 sev = jobData.severity;
                                                            setStealthValueByPath(server, alarmId, {"Severity"}, &sev, &UA_TYPES[UA_TYPES_UINT16]);
                                                            setStealthValueByPath(server, alarmId, {"Retain"}, &bRetainTrue, &UA_TYPES[UA_TYPES_BOOLEAN]);

                                                            // ActiveState
                                                            UA_Boolean bAct = jobData.active;
                                                            UA_LocalizedText tAct = bAct ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Active") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Inactive");
                                                            setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &bAct, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                            setStealthValueByPath(server, alarmId, {"ActiveState"}, &tAct, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                                                            // EnabledState
                                                            UA_Boolean bEnabled = jobData.enabled;
                                                            UA_LocalizedText tEnabled = bEnabled ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Enabled") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Disabled");
                                                            setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &bEnabled, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                            setStealthValueByPath(server, alarmId, {"EnabledState"}, &tEnabled, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                            
                                                            // AckedState
                                                            UA_Boolean bAck = jobData.acked;
                                                            UA_LocalizedText tAck = bAck ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Acknowledged") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unacknowledged");
                                                            setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &bAck, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                            setStealthValueByPath(server, alarmId, {"AckedState"}, &tAck, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                                                            // ConfirmedState
                                                            UA_Boolean bConf = jobData.confirmed;
                                                            UA_LocalizedText tConf = bConf ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Confirmed") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unconfirmed");
                                                            setStealthValueByPath(server, alarmId, {"ConfirmedState", "Id"}, &bConf, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                            setStealthValueByPath(server, alarmId, {"ConfirmedState"}, &tConf, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                                                            // Message
                                                            UA_LocalizedText msg = UA_LOCALIZEDTEXT((char*)"en", (char*)jobData.alarmMessage.c_str());
                                                            setStealthValueByPath(server, alarmId, {"Message"}, &msg, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                                                            // Comment
                                                            if(!jobData.comment.empty()) {
                                                                UA_LocalizedText comment = UA_LOCALIZEDTEXT((char*)"en", (char*)jobData.comment.c_str());
                                                                setStealthValueByPath(server, alarmId, {"Comment"}, &comment, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                            }
                                                            
                                                            // STEP 3: Trigger Event
                                                            std::string emitterName = mapping.alarmKey.substr(0, mapping.alarmKey.find("-"));
                                                            UA_NodeId sourceNode = alarmId; 
                                                            {
                                                                 std::lock_guard<std::mutex> mapLock(g_nodeMap_mutex);
                                                                 if(nodeMap.count(emitterName)) sourceNode = nodeMap[emitterName];
                                                            }
                                                            
                                                            UA_ByteString eventId = UA_BYTESTRING_NULL;
                                                            UA_Server_triggerConditionEvent(server, alarmId, sourceNode, &eventId);
                                                            
                                                            if(eventId.length > 0 && !jobData.aeInstanceId.empty()) {
                                                                g_branchStates[mapping.alarmKey][jobData.aeInstanceId].addEventId(&eventId);
                                                            }
                                                            UA_ByteString_clear(&eventId);
                                                            
                                                            cleanupBranches(mapping.alarmKey);
                                                            UA_NodeId_clear(&branchNodeId);
                                                        }
                                                    }, ServerJobType::SetEventNotifier);

                                                }
                                            } catch(const std::exception& e) { log("JSON/Processing Error: " + std::string(e.what()), LogLevel::ERRORS); }
                                            is_internal_write = false;
                                        }

                                        if(!isAlarmEvent) {
                                            /* Regular data update path (Generic Telemetry) */
                                            try {
                                                auto j = json::parse(payload);

                                                if (j.contains("Data") && j["Data"].is_array()) {
                                                    for (auto& entry : j["Data"]) {
                                                        UA_NodeId nodeId = UA_NODEID_NULL;
                                                        {
                                                            std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                                                            auto it = nodeMap.find(topic);
                                                            if(it != nodeMap.end()) nodeId = it->second;
                                                        }

                                                        if(UA_NodeId_isNull(&nodeId)) continue;

                                                        if (entry.contains("Value")) {
                                                            auto& valField = entry["Value"];

                                                            if (valField.is_array()) {
                                                                std::vector<double> values;
                                                                for (auto& v : valField) {
                                                                    if (v.is_number()) values.push_back(v.get<double>());
                                                                }
                                                                if (!values.empty()) {
                                                                    enqueueServerJob([nodeId, values](UA_Server* server) {
                                                                        ScopedVariant myVar;
                                                                        UA_Variant_setArrayCopy(myVar.get(), values.data(), values.size(), &UA_TYPES[UA_TYPES_DOUBLE]);
                                                                        is_internal_write = true;
                                                                        UA_Server_writeValue(server, nodeId, myVar.var);
                                                                        is_internal_write = false;
                                                                    }, ServerJobType::WriteValue);
                                                                }
                                                            } 
                                                            else if (valField.is_number()) {
                                                                double val = valField.get<double>();
                                                                enqueueServerJob([nodeId, val](UA_Server* server) {
                                                                    ScopedVariant myVar;
                                                                    UA_Variant_setScalarCopy(myVar.get(), &val, &UA_TYPES[UA_TYPES_DOUBLE]);
                                                                    is_internal_write = true;
                                                                    UA_Server_writeValue(server, nodeId, myVar.var);
                                                                    is_internal_write = false;
                                                                }, ServerJobType::WriteValue);
                                                            }
                                                            else if (valField.is_boolean()) {
                                                                UA_Boolean val = valField.get<bool>() ? UA_TRUE : UA_FALSE;
                                                                enqueueServerJob([nodeId, val](UA_Server* server) {
                                                                    ScopedVariant myVar;
                                                                    UA_Variant_setScalarCopy(myVar.get(), &val, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                                    is_internal_write = true;
                                                                    UA_Server_writeValue(server, nodeId, myVar.var);
                                                                    is_internal_write = false;
                                                                }, ServerJobType::WriteValue);
                                                            }
                                                        }
                                                    }
                                                }
                                                else {
                                                    UA_NodeId nodeId = UA_NODEID_NULL;
                                                    {
                                                        std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                                                        auto it = nodeMap.find(topic);
                                                        if(it != nodeMap.end()) nodeId = it->second;
                                                    }

                                                    if(!UA_NodeId_isNull(&nodeId)) {
                                                        for (auto& [key, value] : j.items()) {
                                                            if (value.is_number()) {
                                                                double v = value.get<double>();
                                                                enqueueServerJob([nodeId, v](UA_Server* server) {
                                                                    ScopedVariant myVar;
                                                                    UA_Variant_setScalarCopy(myVar.get(), &v, &UA_TYPES[UA_TYPES_DOUBLE]);
                                                                    is_internal_write = true;
                                                                    UA_Server_writeValue(server, nodeId, myVar.var);
                                                                    is_internal_write = false;
                                                                }, ServerJobType::WriteValue);
                                                            }
                                                            else if (value.is_boolean()) {
                                                                UA_Boolean v = value.get<bool>() ? UA_TRUE : UA_FALSE;
                                                                enqueueServerJob([nodeId, v](UA_Server* server) {
                                                                    ScopedVariant myVar;
                                                                    UA_Variant_setScalarCopy(myVar.get(), &v, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                                    is_internal_write = true;
                                                                    UA_Server_writeValue(server, nodeId, myVar.var);
                                                                    is_internal_write = false;
                                                                }, ServerJobType::WriteValue);
                                                            }
                                                        }
                                                    }
                                                }
                                            } catch (const std::exception& e) {
                                                log("Generic Telemetry Parser Error: " + std::string(e.what()), LogLevel::ERRORS);
                                            }
                                        }
                                    },
                                    [](auto const&) {}
                                 });
                             }
                        }
                    };
                    
                    // Task 2: Subscriber (Dedicated to queue processing)
                    auto sub_task = []() -> as::awaitable<void> {
                        log("DEBUG: Subscription Loop Started", LogLevel::INFO);
                        while(running && g_mqtt_connected.load()) {
                            bool has_pending = false;
                            {
                                std::lock_guard<std::mutex> lock(g_sub_mutex);
                                has_pending = !g_subscription_queue.empty();
                            }
                            
                            if(has_pending) {
                                co_await perform_subscriptions();
                                continue;
                            }
                            
                            if(g_sub_timer) {
                                g_sub_timer->expires_at(std::chrono::steady_clock::time_point::max());
                                try {
                                    co_await g_sub_timer->async_wait(as::use_awaitable);
                                } catch (const boost::system::system_error& e) {
                                    if (e.code() == boost::asio::error::operation_aborted) {
                                        // log("DEBUG: Signal received (Timer cancelled)", LogLevel::DEBUG);
                                        // Only continue if this was an explicit queue signal
                                        if(g_signal_pending.exchange(false)) {
                                            continue;
                                        }
                                        // Otherwise, it's a shutdown signal (from sibling task failure) -> Exit loop
                                        log("DEBUG: Subscription Loop Cancelled (Sibling failure or Shutdown)", LogLevel::INFO);
                                        co_return;
                                    }
                                    throw;
                                }
                            } else {
                                as::steady_timer t(ioc); t.expires_after(std::chrono::seconds(1));
                                co_await t.async_wait(as::use_awaitable);
                            }
                        }
                    };

                    // Run both loops safely
                    using namespace boost::asio::experimental::awaitable_operators;
                    co_await (recv_task() && sub_task());
                } catch(const std::exception &e) {
                     log("MQTT error: " + std::string(e.what()), LogLevel::ERRORS);
                     g_mqtt_connected.store(false);
                }
                
                // Reconnect delay
                as::steady_timer reconnect_timer(ioc);
                reconnect_timer.expires_after(std::chrono::seconds(2));
                co_await reconnect_timer.async_wait(as::use_awaitable);
            }
        }, as::detached);
}


// Migrated logic from main()
int RunServer(int argc, char **argv) {
    // Generate Unique Instance ID for Redis Isolation
    // Generate Unique Instance ID for Redis Isolation
    // CHANGED: Use Static Prefix for Persistence (as requested)
    std::string uniquePrefix = "OPCUA_SERVER:";
    
    // auto now = std::chrono::system_clock::now().time_since_epoch().count();
    // DWORD pid = GetCurrentProcessId();
    // std::stringstream ss;
    // ss << "OPC_UA:" << std::hex << now << "_" << pid;
    // std::string uniquePrefix = ss.str();

    // Initialize Redis Cache
    // Host: 216.48.184.131, Port: 6379, Pass: xeeredis@techd, DB: 0
    // Prefix: OPC_UA:<TimestampHex>_<PID>
    log("Redis Cache Prefix: " + uniquePrefix, LogLevel::INFO);
    // Redis Initialization moved after config loading
    // g_redisClient.init(...) 


    // Determine if running as service (via arguments or context)
    bool isService = false;
    for(int i=0; i<argc; i++) {
        if(std::string(argv[i]) == "--service") {
            isService = true;
            break;
        }
    }

    // Force logging configuration for service mode
    // BEFORE any logging happens
    if(isService) {
        g_logging_enabled = true;
        g_debug = false; 
    }
    
    // Load configuration from appsettings.json
    log("Loading configuration from appsettings.json...", LogLevel::INFO);
    std::ifstream file("appsettings.json");
    if (!file.is_open()) {
        std::string cwd;
        std::string cwdStr;
#ifdef _WIN32
        char cwdBuf[1024];
        if(_getcwd(cwdBuf, sizeof(cwdBuf)))
            cwdStr = std::string(cwdBuf);
        else
            cwdStr = "";
#else
        char cwdBuf[1024];
        if(getcwd(cwdBuf, sizeof(cwdBuf)))
            cwdStr = std::string(cwdBuf);
        else
            cwdStr = "";
#endif
        std::cerr << "ERROR: Could not open appsettings.json. CWD=" << cwdStr << std::endl;
        std::cerr << "Press Enter to exit..." << std::endl;
        std::cin.get();
        return 1;
    }
    std::cout << "✓ Configuration file loaded successfully" << std::endl;

    // Parse JSON
    nlohmann::json Settingsconfig;
    try {
        std::cerr << "[server] Parsing appsettings.json ..." << std::endl;
        std::string jsonStr((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
        Settingsconfig = nlohmann::json::parse(jsonStr,
                                               /*callback=*/nullptr,
                                               /*allow_exceptions=*/true,
                                               /*ignore_comments=*/true);
        std::cerr << "[server] JSON parsed OK." << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "[server] FATAL: appsettings.json parse error: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Extract values
    std::string applicationEndURL, applicationEndURLHost;
    int applicationEndURLPort = 0;
    bool protocol = false;
    std::string brokerAddress;
    int brokerPort = 15776;

    try {
        std::cerr << "[server] Extracting AppSettings ..." << std::endl;
        applicationEndURL     = Settingsconfig["AppSettings"]["ApplicationEndURL"].get<std::string>();
        applicationEndURLHost = Settingsconfig["AppSettings"]["ApplicationEndURLHost"].get<std::string>();
        applicationEndURLPort = Settingsconfig["AppSettings"]["ApplicationEndURLPort"].get<int>();
        std::cerr << "[server] AppSettings OK." << std::endl;

        std::cerr << "[server] Extracting MqttConfig ..." << std::endl;
        protocol      = Settingsconfig["MqttConfig"]["MqttSettings"][0]["UseTLS"].get<bool>();
        brokerAddress = Settingsconfig["MqttConfig"]["MqttSettings"][0]["BrokerAddress"].get<std::string>();
        brokerPort    = Settingsconfig["MqttConfig"]["MqttSettings"][0]["BrokerPort"].get<int>();
        std::cerr << "[server] MqttConfig OK — broker=" << brokerAddress << ":" << brokerPort << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "[server] FATAL: appsettings.json key extraction error: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    g_use_tls        = protocol;
    g_broker_address = brokerAddress;
    g_broker_port    = brokerPort;

    // MQTT username/password and ClientId now come from EdgeConfig (set below after bearer token)
    // Temporary placeholders — will be overwritten after EdgeConfig is loaded
    std::string mqttUsername;
    std::string mqttPassword;

    // Extract Authorization & NodeID — now sourced from EdgeConfig file
    std::cerr << "[EdgeConfig] Attempting to load EdgeConfig_ND07_Server_*.txt ..." << std::endl;
    EdgeConfigData edgeCfg;
    try {
        edgeCfg = LoadEdgeConfig("EdgeConfig_Server_");
    } catch (const std::exception& ex) {
        std::cerr << "\n[EdgeConfig] FATAL ERROR: " << ex.what() << std::endl;
        std::cerr << "[EdgeConfig] Server cannot start without a valid EdgeConfig file." << std::endl;
        std::cout << "STARTUP FAILED: EdgeConfig error — " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
    std::string authUsername = edgeCfg.username;
    std::string authPassword = edgeCfg.password;
    std::string NodeID       = edgeCfg.shortCode;

    // Extract Payload
    std::string dbPath =
        Settingsconfig["Payload"]["OfflineQueueOptions"]["DbPath"].get<std::string>();
    int retentionDays =
        Settingsconfig["Payload"]["OfflineQueueOptions"]["RetentionDays"].get<int>();


    // Extract RedisConfig
    std::string redisHost = Settingsconfig["RedisConfig"]["Host"].get<std::string>();
    int redisPort = Settingsconfig["RedisConfig"]["Port"].get<int>();
    std::string redisPassword = Settingsconfig["RedisConfig"]["Password"].get<std::string>();
    int redisDb = Settingsconfig["RedisConfig"]["DbIndex"].get<int>();

    // Initialize Redis Client
    log("Redis Cache Prefix: " + uniquePrefix, LogLevel::INFO);
    g_redisClient.init(redisHost, redisPort, redisPassword, redisDb, uniquePrefix);

    // Initialize SqliteQueueService for config cache fallback
    try {
        OfflineQueueOptions sqliteOpts;
        sqliteOpts.batchSize = 100;
        sqliteOpts.uploadIntervalSeconds = 15;
        g_sqliteService = new SqliteQueueService(dbPath, sqliteOpts);
        log("SqliteQueueService initialized for config cache at: " + dbPath, LogLevel::INFO);
    } catch(const std::exception& e) {
        log("WARNING: Failed to initialize SqliteQueueService: " + std::string(e.what()) + ". DB fallback will be unavailable.", LogLevel::WARNING);
    }

    std::cout << "✓ Configuration parsed successfully" << std::endl;
    std::cout << "API Host: " << applicationEndURLHost << ":" << applicationEndURLPort << std::endl;

    // Store auth credentials globally for authentication callback
    g_authUsername = authUsername;
    g_authPassword = authPassword;
    g_apiHost = applicationEndURLHost;
    g_apiPort = std::to_string(applicationEndURLPort);

    // Acquire bearer token for API authentication with Retry Logic
    std::string BearerToken;
    int retryDelay = 5;
    int tokenAttempt = 0;
    const int maxTokenRetries = 3;
    
    while(tokenAttempt < maxTokenRetries) {
        tokenAttempt++;
        std::cout << "Acquiring bearer token (attempt " << tokenAttempt << "/" << maxTokenRetries << ")..." << std::endl;
        log("Acquiring bearer token (attempt " + std::to_string(tokenAttempt) + "/" + std::to_string(maxTokenRetries) + ")...", LogLevel::INFO);
        
        try {
            json authResponse = getBearerToken(applicationEndURLHost, 
                                               std::to_string(applicationEndURLPort),
                                               authUsername, authPassword);
            
            if(authResponse.contains("access_token")) {
                BearerToken = authResponse["access_token"].get<std::string>();
                g_bearerToken = BearerToken; // Ensure global is set
                std::cout << "✓ Bearer token acquired successfully" << std::endl;
                log("Bearer token acquired successfully", LogLevel::INFO);
                break; // Success
            } else {
                std::cerr << "ERROR: Bearer token response missing 'access_token' field" << std::endl;
                log("Bearer token response missing 'access_token' field", LogLevel::ERRORS);
            }
        } catch(const std::exception& e) {
            std::cerr << "ERROR: Failed to acquire bearer token: " << e.what() << std::endl;
            log("Failed to acquire bearer token: " + std::string(e.what()), LogLevel::ERRORS);
        }
        
        if(tokenAttempt < maxTokenRetries) {
            std::cout << " Retrying in " << retryDelay << " seconds..." << std::endl;
            log(" Retrying in " + std::to_string(retryDelay) + " seconds...", LogLevel::INFO);
            std::this_thread::sleep_for(std::chrono::seconds(retryDelay));
        
            if(retryDelay < 15) {
                retryDelay += 5;
            }
        }
    }

    if(BearerToken.empty()) {
        log("WARNING: All " + std::to_string(maxTokenRetries) + " bearer token attempts failed. Running in OFFLINE mode (DB fallback only).", LogLevel::WARNING);
        std::cout << "⚠️ Running in OFFLINE mode - using local database for configuration." << std::endl;
    }

    // Derive MQTT credentials from EdgeConfig now that bearer token is known
    SetEdgeConfigMqttPassword(edgeCfg, BearerToken);
    mqttUsername      = edgeCfg.mqttUsername;
    mqttPassword      = edgeCfg.mqttPassword;
    g_mqtt_username   = mqttUsername;
    g_mqtt_password   = mqttPassword;
    g_mqtt_client_id  = edgeCfg.clientId;
    log("MQTT credentials set from EdgeConfig (ClientId=" + edgeCfg.clientId + ")", LogLevel::INFO);

    // Global logging control - DISABLED BY DEFAULT
    g_logging_enabled = true;   // Keep file logging enabled
    g_debug = false;             // Disable console debug output by default

    if(argc > 1 && std::string(argv[1]) == "--debug") {
        g_debug = true;
        log("Debug mode enabled via --debug flag", LogLevel::INFO);
    }

    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);

    // Initialize logging with server-specific folder
    init_logging("logs/server", "server", true);
    log("Server logging initialized with day-wise log files", LogLevel::INFO);

    UA_ByteString certificate = loadFile("server/own/certs/server_cert.der");
    UA_ByteString privateKey = loadFile("server/own/certs/server_key.der");

    if(certificate.length == 0 || privateKey.length == 0) {
        log("Failed to load server certificate or key", LogLevel::ERRORS);
        UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "Could not load server certificate or key from certs/own/");
        return EXIT_FAILURE;
    }

    log("Server certificate and private key loaded successfully", LogLevel::INFO);

    UA_ByteString *trustList = NULL;
    size_t trustListSize = loadCertsFromDirectory("server/trusted/certs", &trustList);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "Loaded %zu trusted certificate(s).", trustListSize);
    log("Loaded " + std::to_string(trustListSize) + " trusted certificate(s)",
        LogLevel::INFO);

    UA_ByteString *issuerList = NULL;
    size_t issuerListSize = loadCertsFromDirectory("server/issuers/certs", &issuerList);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Loaded %zu issuer certificate(s).",
                issuerListSize);
    log("Loaded " + std::to_string(issuerListSize) + " issuer certificate(s)",
        LogLevel::INFO);

    UA_ByteString *revocationList = NULL;
    size_t revocationListSize = loadCertsFromDirectory("server/issuers/crl", &revocationList);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Loaded %zu revocation certificate(s).",
                revocationListSize);
    log("Loaded " + std::to_string(revocationListSize) + " revocation certificate(s)",
        LogLevel::INFO);

    UA_Server *server = UA_Server_new();
    UA_ServerConfig *config = UA_Server_getConfig(server);

    // Custom logger is now enabled with safe format string handling
    // The myLog function now sanitizes problematic format specifiers before processing
    config->logging = &myLogger;
    

    log("OPC UA Server initialized", LogLevel::INFO);
    log("Setting up server configuration", LogLevel::DEBUG);


    // #ifdef UA_ENABLE_ENCRYPTION
    // UA_ServerConfig_setDefaultWithSecurityPolicies(config, 4840, &certificate,
    // &privateKey, trustList, trustListSize, NULL, 0, NULL, 0);
    //     UA_ServerConfig_setDefaultWithSecurityPolicies(config, 4840, &certificate,
    //     &privateKey, NULL, NULL, NULL, 0, NULL, 0);
    // config->applicationDescription.applicationUri =
    // UA_STRING_ALLOC("urn:Anexee.server.application");

    // UA_StatusCode retval =
    // UA_ServerConfig_setDefaultWithSecurityPolicies(config, 4840,
    //     &certificate, &privateKey,
    //     trustList, trustListSize,
    //     issuerList, issuerListSize,
    //     revocationList, revocationListSize);

    std::string json_body = std::format(R"(
    {{
        "data": {{ "nodeId": "{}" }}
    }}
    )", NodeID);

    std::string target = "/api/GetOpcUaServersWithOrgMappings";

    ServerConfig current_config;
    std::string bearerToken;

    // Check for Child Mode
    bool isChild = (argc > 1 && strcmp(argv[1], "--child") == 0);

    if (isChild) {
        log("👶 Starting in CHILD mode", LogLevel::INFO);
        
        // Read "Bootstrap Bundle" from Stdin
        std::string inputJSON;
        // Read until EOF
        for (std::string line; std::getline(std::cin, line);) {
            inputJSON += line;
        }

        try {
             json j = json::parse(inputJSON); 
             if(j.contains("bearerToken")) {
                 bearerToken = j["bearerToken"].get<std::string>();
                 // g_bearerToken = bearerToken; // If global exists?
             }
             if(j.contains("config")) {
                 current_config = ServerConfigFromJSON(j["config"]);
             } else {
                 log("❌ Child received invalid JSON: missing 'config'", LogLevel::ERRORS);
                 return EXIT_FAILURE;
             }
             
             // Re-initialize logging for Child to avoid file lock contention with Manager
             init_logging("logs/" + current_config.name, current_config.name, true);
             log("✓ Child Configured: " + current_config.name + " (" + std::to_string(current_config.port) + ")", LogLevel::INFO);
             log("Baby Starting in CHILD mode (Log switched)", LogLevel::INFO);
        } catch(const std::exception& e) {
             log("❌ Child failed to parse stdin JSON: " + std::string(e.what()), LogLevel::ERRORS);
             return EXIT_FAILURE;
        }

        // Child authenticates? It has the token. 
        // We can skip the `getBearerToken` call below if we already have it.

    } else {
        log("👑 Starting in MANAGER mode", LogLevel::INFO);
        
        bearerToken = g_bearerToken;

        std::vector<ServerConfig> configs;
        std::string serverConfigCacheKey = "SERVER_CONFIGS_" + NodeID;
        try {
            log("Fetching server configurations...", LogLevel::INFO);
            configs = ParseServerConfig(applicationEndURLHost, std::to_string(applicationEndURLPort), 
                                        bearerToken, json_body, target);
            
            // If API succeeded, cache the raw response to DB for offline use
            if(!configs.empty() && g_sqliteService) {
                try {
                    json configsJson = json::array();
                    for(const auto& cfg : configs) {
                        json j;
                        j["id"] = cfg.id;
                        j["dataPointId"] = cfg.dataPointId;
                        j["name"] = cfg.name;
                        j["ip"] = cfg.ip;
                        j["port"] = cfg.port;
                        j["nodeId"] = cfg.nodeId;
                        json mappingsArr = json::array();
                        for(const auto& om : cfg.orgMappingList) {
                            json mj;
                            mj["id"] = om.id;
                            mj["hierarchyId"] = om.hierarchyId;
                            mj["mapOrgId"] = om.mapOrgId;
                            mj["orgShortCode"] = om.orgShortCode;
                            mappingsArr.push_back(mj);
                        }
                        j["orgMappings"] = mappingsArr;
                        configsJson.push_back(j);
                    }
                    g_sqliteService->SetConfig(serverConfigCacheKey, configsJson.dump());
                    log("💾 Cached server configs to DB (" + std::to_string(configs.size()) + " configs)", LogLevel::INFO);
                } catch(const std::exception& e) {
                    log("⚠️ Failed to cache server configs to DB: " + std::string(e.what()), LogLevel::WARNING);
                }
            }

            // If API returned empty (e.g. token was empty/offline), try DB fallback
            if(configs.empty() && g_sqliteService) {
                log("📉 API returned no configs. Trying local database fallback...", LogLevel::INFO);
                try {
                    std::string dbData = g_sqliteService->GetConfig(serverConfigCacheKey);
                    if(!dbData.empty()) {
                        json cachedConfigs = json::parse(dbData);
                        for(const auto& item : cachedConfigs) {
                            configs.push_back(ServerConfigFromJSON(item));
                        }
                        log("💾 DB Hit! Loaded " + std::to_string(configs.size()) + " server configs from local database.", LogLevel::INFO);
                    } else {
                        log("📉 DB Miss for server configs.", LogLevel::INFO);
                    }
                } catch(const std::exception& e) {
                    log("⚠️ DB Error for server configs: " + std::string(e.what()), LogLevel::WARNING);
                }
            }

            if(configs.empty()) {
                log("❌ No server configurations found (API + DB). Exiting.", LogLevel::ERRORS);
                return EXIT_FAILURE;
            }

            // 1. Configure Manager (Instance 0)
            current_config = configs[0];
            log("✓ Manager Configured: " + current_config.name + " (" + std::to_string(current_config.port) + ")", LogLevel::INFO);
            
            // DEBUG: Log all configs
            for(size_t i=0; i<configs.size(); i++) {
                log("  Config[" + std::to_string(i) + "]: " + configs[i].name + " Port: " + std::to_string(configs[i].port), LogLevel::DEBUG);
            }

            // 2. Spawn Children (Instances 1..N)
            // Create Job Object to ensure child processes are terminated when parent exits
#ifdef _WIN32
            HANDLE hJob = CreateJobObject(NULL, NULL);
            if (hJob) {
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION jeli = { 0 };
                jeli.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                SetInformationJobObject(hJob, JobObjectExtendedLimitInformation, &jeli, sizeof(jeli));
            } else {
                log("Failed to create Job Object. Child termination relies on manual cleanup.", LogLevel::ERRORS);
            }
#else
            void* hJob = nullptr;
#endif

            for(size_t i = 1; i < configs.size(); i++) {
                log("🚀 Spawning child instance details for: " + configs[i].name, LogLevel::INFO);
                SpawnChildServer(argv[0], configs[i], bearerToken, isService, hJob);
            }

        } catch(const std::exception& e) {
            log("Failed to fetch server configs: " + std::string(e.what()), LogLevel::ERRORS);
            return EXIT_FAILURE;
        }
    }

    // Common Logic: Apply Configuration
    // 0. Set Global Bearer Token
    g_bearerToken = bearerToken;
    log("✓ Global Bearer Token set", LogLevel::DEBUG);

    // Append the instance ID to the MQTT Client ID to ensure uniqueness across spawned instances
    g_mqtt_client_id = g_mqtt_client_id + "_" + std::to_string(current_config.id);
    log("Dynamic MQTT Client ID set to: " + g_mqtt_client_id, LogLevel::INFO);

    // 1. Override Port
    if(current_config.port > 0) {
        log("Configuring server on port " + std::to_string(current_config.port), LogLevel::INFO);
        // Explicit clearing removed due to API access restrictions.
        // Relying on UA_ServerConfig_setMinimal to handle configuration override.
        
        log("Attempting to set server port to: " + std::to_string(current_config.port), LogLevel::INFO);
        UA_StatusCode retval = UA_ServerConfig_setMinimal(config, current_config.port, &certificate);
        if(retval != UA_STATUSCODE_GOOD) {
             log("Failed to set server port configuration: " + std::string(UA_StatusCode_name(retval)), LogLevel::ERRORS);
        } else {
             log("✓ UA_ServerConfig_setMinimal succeeded for port " + std::to_string(current_config.port), LogLevel::INFO);
             // Verify actual configuration (Logging only, no struct access)
             log("Server configuration applied.", LogLevel::INFO);
        }

        
        // Re-apply custom logger as setDefault resets it
        config->logging = &myLogger;
    }

    // 2. Map Organizations
    // Replace legacy ParseOrgConfig call
    vector<OrgConfig> orgs;
    if(!current_config.orgMappingList.empty()) {
        log("Mapping " + std::to_string(current_config.orgMappingList.size()) + " organizations from config...", LogLevel::INFO);
        for(const auto& map : current_config.orgMappingList) {
            OrgConfig org;
            org.id = map.id; // Critical for routing
            org.shortCode = map.orgShortCode; // Critical for topics?
            org.orgId = map.mapOrgId; // Maybe needed?
            // Convert other fields if needed, or leave defaults
            org.name = map.orgShortCode; // Fallback
            orgs.push_back(org);
        }
        
        // Populate global organizations list immediately
        g_organizations = orgs;
        
    } else {
        log("⚠️ No orgMappings found for this instance.", LogLevel::INFO);
    }

    // ========================================================================
    // MULTI-TENANCY: Initialize Session Manager
    // ========================================================================
    if(!orgs.empty()) {
        log("Initializing multi-tenancy session manager with " + 
            std::to_string(orgs.size()) + " organizations", LogLevel::INFO);
        
        // Store organizations globally for authentication callback
        g_organizations = orgs;
        
        g_sessionManager.initialize(orgs);
        
        // Set global variables for worker threads
        g_server = server;
        g_bearerToken = BearerToken;
        
        log("✓ Session manager initialized with " + std::to_string(orgs.size()) + 
            " organizations", LogLevel::INFO);
        log("ℹ️ Multi-tenancy: Organizations will load on-demand when accessed", LogLevel::INFO);
        
    } else {
         log("No organizations available - multi-tenancy disabled", LogLevel::INFO);
    }
    // ========================================================================
    
    
    // ========================================================================
    // SERVER CONFIGURATION: Standard Endpoints (Authentication-Based Routing)
    // ========================================================================
    
    UA_StatusCode retval = UA_ServerConfig_setDefaultWithSecurityPolicies(
        config, current_config.port, &certificate, &privateKey, trustList, trustListSize, issuerList,
        issuerListSize, revocationList, revocationListSize);

    if(retval != UA_STATUSCODE_GOOD) {
        log("Failed to configure security policies", LogLevel::ERRORS);
        return EXIT_FAILURE;
    }

    log("✓ Server configured with standard security policies", LogLevel::INFO);
    log("  Authentication-based multi-tenancy enabled", LogLevel::INFO);
    log("  " + std::to_string(orgs.size()) + " organizations available", LogLevel::INFO);
    // ========================================================================




    log("Configured server endpoints", LogLevel::DEBUG);

    // Accept all certificates for demo/testing
    // config->secureChannelPKI.clear(&config->secureChannelPKI);
    // config->sessionPKI.clear(&config->sessionPKI);
    // UA_CertificateGroup_AcceptAll(&config->secureChannelPKI);
    // UA_CertificateGroup_AcceptAll(&config->sessionPKI);
;
    config->applicationDescription.applicationUri =
        UA_STRING_ALLOC("urn:Anexee.server.application");
    config->applicationDescription.productUri = UA_STRING_ALLOC("urn:Anexee:product");
    config->applicationDescription.applicationName =
        UA_LOCALIZEDTEXT_ALLOC("en-US", "AnexeeServer");

    log("Application description configured: Anexee Server", LogLevel::DEBUG);

    // ========================================================================
    // PERFORMANCE TUNING: Limit Queues to prevent Memory Leaks
    // ======================================================================== 
    config->maxSessions = 100;
    config->maxSecureChannels = 200;      // Limit concurrent TCP connections
    config->maxSessionTimeout = 10000.0; // Prune detached sessions after 10s to free MonitoredItems
    //config->maxSubscriptionsPerSession = 50;
    //config->maxMonitoredItemsPerSubscription = 1000;
    config->maxMonitoredItems = 0;   // Global limit to prevent TimerTree explosion
    config->queueSizeLimits.max = 100;  // Global limit for MonitoredItems
    //config->maxSubscriptions = 200;      // Global limit for subscriptions
    config->publishingIntervalLimits.min = 50.0; // Enforce min 100ms publishing interval
    config->samplingIntervalLimits.min = 0.0;   // Throttle sampling to max 5Hz to prevent notification flood
    config->publishingIntervalLimits.max = 10000.0;
    //config->publishingIntervalLimits.max = 3600.0 * 1000.0;
    config->enableRetransmissionQueue = true;  // Enable retransmission queue
    config->maxRetransmissionQueueSize = 100;         // Standard: Unlimited (was 1/10 for debugging)
    config->maxNotificationsPerPublish = 20000;      // Limit per PublishResponse

    config->keepAliveCountLimits.min = 3;  // Prune dead subscriptions faster
    config->keepAliveCountLimits.max = 1200;
   
    // Allow the server to send larger packets (1 MB chunks, 10 MB total message)
    config->tcpBufSize = 16 * 1024 * 1024;     // 16 MB TCP Buffer (Excellent)
    config->tcpMaxMsgSize = 10 * 1024 * 1024;  // 10 MB - Allows massive Browse responses
    config->tcpMaxChunks = 100;                // Allow splitting big messages into 100 chunks
    config->tcpReuseAddr = true;               // Allows immediate restart after a crash
    
    //Operations
    config->maxNodesPerBrowse = 5000;
    config->maxNodesPerRead = 5000;
    
    log("Performance Limits used: MaxSessions=100, GlobalMAXMI=20000", LogLevel::INFO);



    // ----------------
    UA_AccessControl_defaultWithLoginCallback(
        config, true, NULL, 2, usernamePasswordLogin, myLoginCallback, NULL);
    log("Access control configured with login callback", LogLevel::DEBUG);

    // Setup custom access control for multi-tenancy isolation
    AccessControl_setup(config);
    log("Multi-tenancy access control rules applied", LogLevel::INFO);

    for(size_t i = 0; i < config->endpointsSize; i++) {
        UA_EndpointDescription *ep = &config->endpoints[i];
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Server Endpoint %zu: %.*s", i,
                    (int)ep->endpointUrl.length, ep->endpointUrl.data);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "  Server SecurityPolicy: %.*s",
                    (int)ep->securityPolicyUri.length, ep->securityPolicyUri.data);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "  Server SecurityMode: %d",
                    ep->securityMode);
        for(size_t j = 0; j < ep->userIdentityTokensSize; j++) {
            UA_UserTokenPolicy *pol = &ep->userIdentityTokens[j];
            UA_LOG_INFO(
                UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                "    Server TokenType: %d, PolicyId: %.*s, SecurityPolicyUri: %.*s",
                pol->tokenType, (int)pol->policyId.length, pol->policyId.data,
                (int)pol->securityPolicyUri.length, pol->securityPolicyUri.data);
        }
    }




    //// Add an ALARM FOLDER INSIDE SERVER THEN ALL NODES WITH HASEVENTSOURCE WILL BE ADDED
    //// TO THIS FOLDER
    //UA_NodeId areaNodeId = UA_NODEID_NUMERIC(0, 54624);
    //UA_ObjectAttributes objAttr = UA_ObjectAttributes_default;
    //objAttr.displayName = UA_LOCALIZEDTEXT((char *)"en", (char *)"Alarms");
    //UA_Server_addObjectNode(
    //    server, UA_NODEID_NULL, UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER),
    //    UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES), UA_QUALIFIEDNAME(1, (char *)"Alarms"),
    //    UA_NODEID_NUMERIC(0, UA_NS0ID_BASEOBJECTTYPE), objAttr, NULL, &areaNodeId);

    //UA_Server_addReference(server, UA_NODEID_NUMERIC(0, 2253),  // Server
    //                       UA_NODEID_NUMERIC(0, UA_NS0ID_HASNOTIFIER),
    //                       UA_EXPANDEDNODEID_NUMERIC(areaNodeId.namespaceIndex,
    //                                                 areaNodeId.identifier.numeric),
    //                       UA_TRUE);

    ///* Use the Alarms object as the default Event Notifier origin */
    //g_eventNotifierNode = areaNodeId;



    // ========================================================================
    // ALARMS & CONDITIONS: Register Refresh Callback
    // ========================================================================
    // Register the ConditionRefresh method callback on the standard ConditionType node
    UA_NodeId refreshId = UA_NODEID_NUMERIC(0, UA_NS0ID_CONDITIONTYPE_CONDITIONREFRESH);
    UA_StatusCode refreshRc = UA_Server_setMethodNodeCallback(server, refreshId, ConditionRefreshMethodCallback);
    if(refreshRc == UA_STATUSCODE_GOOD) {
        log("✓ Registered ConditionRefresh callback", LogLevel::INFO);
    } else {
        log("WARNING: Failed to register ConditionRefresh callback: " + std::string(UA_StatusCode_name(refreshRc)), LogLevel::ERRORS);
    }

    // Start MQTT Client (Async)
    start_mqtt_client(server);

    std::thread mqtt_thread([&]() { ioc.run(); });
    // mqtt_thread.detach(); // FIXED: Do not detach, we must join it to prevent crash on exit

    log("Starting OPC UA Server...", LogLevel::INFO);
    log("Added repeated callback for counter updates", LogLevel::DEBUG);

    UA_StatusCode startupRc = UA_Server_run_startup(server);
    if(startupRc != UA_STATUSCODE_GOOD) {
         log(" Server startup failed with code: " + std::string(UA_StatusCode_name(startupRc)), LogLevel::ERRORS);
         return EXIT_FAILURE;
    }
    log("Server startup completed successfully", LogLevel::INFO);
    
    // ========================================================================
    // CRITICAL: Register access control callbacks AFTER server startup
    // ========================================================================
    // UA_Server_run_startup resets the access control configuration, so we
    // must set our callbacks AFTER startup completes but BEFORE accepting
    // connections. This ensures multi-tenancy session detection works.
    // ========================================================================
    log(" Configuring multi-tenancy session detection...", LogLevel::INFO);
    
    UA_ServerConfig *runningConfig = UA_Server_getConfig(server);
    
    if(runningConfig && runningConfig->accessControl.activateSession) {
        log("  Replacing existing activateSession callback", LogLevel::DEBUG);
    }
    
    runningConfig->accessControl.activateSession = customActivateSession;
    runningConfig->accessControl.closeSession = customCloseSession;
    
    // Setup fine-grained access control (browsing, read, write rights)
    AccessControl_setup(runningConfig);
    
    log("✅ Multi-tenancy session callbacks active - worker threads will be created on-demand", LogLevel::INFO);
    // ========================================================================
    
    log("Server is ready to accept connections", LogLevel::INFO);


    log("Server is now running and listening for connections", LogLevel::INFO);

    auto last_trim = std::chrono::steady_clock::now();

    try {
        while(running) {
            UA_UInt16 timeout = 0;
            {
                // STRUCTURAL CONCURRENCY: Single-Threaded Job Processing
                // No global mutex needed here because this thread is the ONLY
                // thread allowed to touch UA_Server (except for job enqueuing).
                
                // 1. Process Pending Jobs (Limit batch size to remain responsive)
                const int BATCH_SIZE = 20;
                int jobsProcessed = 0;
                
                while(jobsProcessed < BATCH_SIZE) {
                    ServerJob job;
                    {
                        std::unique_lock<std::mutex> lock(g_serverQueueMutex);
                        if(g_serverQueue.empty()) break;
                        job = std::move(g_serverQueue.front());
                        g_serverQueue.pop();
                    }
                    
                    try {
                        if(job.fn) job.fn(server); 
                    } catch(const std::exception& e) {
                        log("CRITICAL: ServerJob failed: " + std::string(e.what()), LogLevel::ERRORS);
                    }
                    jobsProcessed++;
                }

                // 2. Run Open62541 internal tasks (Network, timers)
                // non-blocking (waitInternal=false)
                timeout = UA_Server_run_iterate(server, false);
            }

            // Sleep logic for CPU conservation
            if(timeout > 50) timeout = 50; 
            if(timeout > 0) {
                 // But wake up early if new jobs arrive!
                 std::unique_lock<std::mutex> lock(g_serverQueueMutex);
                 g_serverQueueCv.wait_for(lock, std::chrono::milliseconds(timeout), 
                    []{ return !g_serverQueue.empty(); });
            }
            
            // FRAGMENTATION CONTROL: Release unused heap memory to OS periodically
            // This is the Windows equivalent of malloc_trim(0)
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_trim).count() > 300) {
                #ifdef _WIN32
                int res = _heapmin();
                if(res == 0) {
                     log("DEBUG: Performed _heapmin() (Released unused heap to OS)", LogLevel::DEBUG); 
                }
                #endif
              

                last_trim = now;
            }
        }
    } catch (const std::exception& e) {
        log("CRITICAL: Unhandled exception in main loop: " + std::string(e.what()), LogLevel::ERRORS);
        std::cerr << "CRITICAL: Unhandled exception: " << e.what() << std::endl;
    } catch (...) {
        log(" CRITICAL: Unknown exception in main loop", LogLevel::ERRORS);
        std::cerr << "CRITICAL: Unknown exception in main loop" << std::endl;
    }

    // FIXED: graceful shutdown of MQTT thread
    ioc.stop();
    if(mqtt_thread.joinable()) {
        mqtt_thread.join();
    }
    
    log("Server loop exited - running flag is now false", LogLevel::INFO);
    log("Server shutdown initiated", LogLevel::INFO);


    log("Cleaning up server resources", LogLevel::DEBUG);

    // Clear Redis Cache (OPC_UA prefix)
    g_redisClient.clearCache();



    // Clean up security policies
    for(size_t i = 0; i < config->securityPoliciesSize; i++) {
        config->securityPolicies[i].clear(&config->securityPolicies[i]);
    }

    UA_ByteString_clear(&certificate);
    UA_ByteString_clear(&privateKey);

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
    // Clean up method callback contexts
    // Note: In a production environment, you might want to keep track of all allocated
    // contexts and clean them up individually. For this example, the server will handle
    // most cleanup. The MethodCallbackContext structures are stored as node contexts and
    // will be cleaned up when the server is deleted.
    
    // Explicitly shut down SessionManager to ensure worker threads stop BEFORE server is deleted
    // (Worker threads access UA_Server, so they must be dead before we kill the server)
    log("Shutting down SessionManager...", LogLevel::DEBUG);
    g_sessionManager.shutdown();

    log("Deleting server instance", LogLevel::DEBUG);
    UA_Server_delete(server);

    log("Server shutdown completed successfully", LogLevel::INFO);
    return EXIT_SUCCESS;
}

// ========================================================================
// WINDOWS SERVICE IMPLEMENTATION
// ========================================================================

#ifdef _WIN32
void WINAPI ServiceCtrlHandler(DWORD CtrlCode) {
    switch (CtrlCode) {
    case SERVICE_CONTROL_STOP:
        if (g_ServiceStatus.dwCurrentState != SERVICE_RUNNING)
            break;

        g_ServiceStatus.dwCurrentState = SERVICE_STOP_PENDING;
        g_ServiceStatus.dwCheckPoint = 4;
        g_ServiceStatus.dwWaitHint = 0;
        SetServiceStatus(g_StatusHandle, &g_ServiceStatus);

        // Signal Server to Stop
        running = false;
        g_serverQueueCv.notify_all(); // Wake up main loop immediately
        // Optionally raise SIGINT if running logic relies on it?
        // But running=false should be enough for the loop.
        break;
    default:
        break;
    }
}

void WINAPI ServiceMain(DWORD argc, LPSTR *argv) {
    g_StatusHandle = RegisterServiceCtrlHandlerA(SERVICE_NAME, ServiceCtrlHandler);
    if (!g_StatusHandle) {
        return;
    }

    g_ServiceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    g_ServiceStatus.dwCurrentState = SERVICE_START_PENDING;
    g_ServiceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    g_ServiceStatus.dwWin32ExitCode = 0;
    g_ServiceStatus.dwServiceSpecificExitCode = 0;
    g_ServiceStatus.dwCheckPoint = 0;
    g_ServiceStatus.dwWaitHint = 3000;
    SetServiceStatus(g_StatusHandle, &g_ServiceStatus);

    // FIX: Services start in C:\Windows\System32. We must change CWD to executable directory
    // so that relative paths for logs, certificates, and config files work correctly.
    char modulePath[MAX_PATH];
    if (GetModuleFileNameA(NULL, modulePath, MAX_PATH) > 0) {
        std::string path(modulePath);
        std::string dir = path.substr(0, path.find_last_of("\\/"));
        if (!SetCurrentDirectoryA(dir.c_str())) {
             // Log error but proceed? Or fail? 
             // Without this, everything else will likely fail anyway.
        }
    }

    // Initialize Logging for Service
    init_logging("logs/service", "anexee_service", true);
    log("Service Starting...", LogLevel::INFO);

    // Report Running
    g_ServiceStatus.dwCurrentState = SERVICE_RUNNING;
    SetServiceStatus(g_StatusHandle, &g_ServiceStatus);

    // Prepare arguments for RunServer
    // We can pass empty args or constructor args if needed
    // But RunServer usually handles "console" args. 
    // We should pass --service explicitly just in case RunServer checks it again
    int s_argc = 2;
    char* s_argv[] = { (char*)"server.exe", (char*)"--service", NULL };

    // RUN THE SERVER
    RunServer(s_argc, s_argv);

    // After RunServer returns
    log("Service Stopping...", LogLevel::INFO);
    g_ServiceStatus.dwCurrentState = SERVICE_STOPPED;
    SetServiceStatus(g_StatusHandle, &g_ServiceStatus);
}
#endif

// ========================================================================
// ENTRY POINT
// ========================================================================

int main(int argc, char **argv) {
    // 1. Check for Service Management Flags
    if (argc > 1) {
        if (std::string(argv[1]) == "--install") {
            if (InstallService(SERVICE_NAME, DISPLAY_NAME)) {
                std::cout << "To start the service, run: sc start " << SERVICE_NAME << std::endl;
                return 0;
            } else {
                return 1;
            }
        } 
        else if (std::string(argv[1]) == "--uninstall") {
             if (UninstallService(SERVICE_NAME)) {
                 return 0;
             } else {
                 return 1;
             }
        }
    }

    // 2. Check if started as a Service (by SCM)
    
    // HEURISTIC: If --service is passed, we definitely try SCM dispatch
    bool tryService = false;
    for(int i=1; i<argc; i++) {
        if(std::string(argv[i]) == "--service") tryService = true;
    }

    if(tryService) {
#ifdef _WIN32
        SERVICE_TABLE_ENTRYA ServiceTable[] = {
            { (LPSTR)SERVICE_NAME, (LPSERVICE_MAIN_FUNCTIONA)ServiceMain },
            { NULL, NULL }
        };

        if (StartServiceCtrlDispatcherA(ServiceTable)) {
            return 0;
        } else {
            // Failed to start as service? Fallback or Error?
            // If user ran --service from console, this error is expected (ERROR_FAILED_SERVICE_CONTROLLER_CONNECT)
            std::cerr << "StartServiceCtrlDispatcher failed (Code: " << GetLastError() << ")." << std::endl;
            std::cerr << "Run without --service to start in console mode." << std::endl;
            return 1;
        }
#else
        std::cerr << "Windows services are not supported on Linux. Please run without --service." << std::endl;
        return 1;
#endif
    }

    // 3. Normal Console / Child Mode
    // If not --install/uninstall/service, run normally
    return RunServer(argc, argv);
}

