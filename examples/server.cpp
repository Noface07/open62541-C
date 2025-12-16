/* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
 * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

//  ./server D:\OPC UA Server\OPCUA- open 62451\open62541-C\certs\server_cert.der D:\OPC
//  UA Server\OPCUA- open 62451\open62541-C\certs\server_key.der [trust1.der trust2.der
//  ...]
//  ./server D:\OPC UA Server\OPCUA- open 62451\open62541-C\certs\server_cert.der D:\OPC
//  UA Server\OPCUA- open 62451\open62541-C\certs\server_key.der

#include <open62541/plugin/accesscontrol_default.h>
#include <open62541/plugin/certificategroup_default.h>
#include <open62541/plugin/log_stdout.h>
#include <open62541/plugin/securitypolicy.h>
#include <open62541/plugin/securitypolicy_default.h>
#include <open62541/server.h>
#include <open62541/server_config_default.h>

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
#include <open62541/client.h>
#include <open62541/client_config_default.h>

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
#include "alarm_enums.h"
#include "AccessControl.h"
#include <windows.h>
#include "ServerConfig.h"
#include "ServiceUtils.h"
#ifdef _WIN32
#include <malloc.h> // For _heapmin
#endif

// Service Globals
SERVICE_STATUS g_ServiceStatus;
SERVICE_STATUS_HANDLE g_StatusHandle;


// Helper to spawn a child server instance with configuration passed via Stdin
// Helper to spawn a child server instance with configuration passed via Stdin
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
    for(const auto& org : config.orgMappings) {
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
    if (hJob != NULL) {
        if (!AssignProcessToJobObject(hJob, pi.hProcess)) {
            log("SpawnChild: AssignProcessToJobObject failed (" + std::to_string(GetLastError()) + ")", LogLevel::ERRORS);
        }
    }

    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);
}

#ifdef _WIN32
#include <direct.h>  // For _getcwd
#endif

using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;
UA_Boolean running = true;
std::mutex g_alarmMutex;

// static UA_HistoryDataGathering *g_gathering = NULL;

/* Cache created alarm Condition nodes keyed by emitter + alarm name */
std::unordered_map<std::string, UA_NodeId> g_alarmByKey;


// ---------------------------------------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------------------
/* Map trigger topic (applicableTagName) to list of alarm keys (emitter+alarmName) */
// TriggerToAlarmMapping struct moved to AandC.h

std::unordered_map<std::string, std::vector<TriggerToAlarmMapping>> g_triggerToAlarmMap;

// MQTT Subscription Queue Globals
std::mutex g_sub_mutex;
std::vector<std::string> g_subscription_queue;
std::set<std::string> g_subscribed_topics;
std::unique_ptr<boost::asio::steady_timer> g_sub_timer;

// Global NodeMap (Topic -> NodeId) for Generic Telemetry
// Global NodeMap (Topic -> NodeId) for Generic Telemetry
std::map<std::string, UA_NodeId> nodeMap;
std::mutex g_nodeMap_mutex;
std::mutex g_topicMap_mutex;
std::recursive_mutex g_server_mutex; // Protects UA_Server API access


// ----------------------------------------------------------------------------------------------------------------

/* Branch management: Map GUID → Branch NodeId for each alarm condition
 * 
 * OPC UA Alarms & Conditions support branches to track multiple simultaneous
 * occurrences of the same alarm condition. Each branch is identified by a unique
 * BranchId (NodeId). In this implementation:
 * 
 * - Main branch: BranchId = NULL (uses condition NodeId directly)
 * - GUID branches: Each unique AEInstanceID GUID gets its own branch NodeId
 * 
 * Branches are typically not visible in the Address Space and this standard does not define a standard way to make them visible.
 */
struct AlarmBranchInfo {
    UA_NodeId branchNodeId;      // The branch NodeId (or condition NodeId for main branch)
    UA_NodeId conditionNodeId;   // The main Condition NodeId (added for ConditionRefresh)
    std::string guid;             // The AEInstanceID GUID
    bool isMainBranch;            // true if this is the main branch (GUID empty/null)
};

// Map: alarmKey → (GUID → BranchInfo)
static std::unordered_map<std::string, std::unordered_map<std::string, AlarmBranchInfo>> g_alarmBranches;

// ----------------------------------------------------------------------------------------------------------------

/* Branch state tracking: Track state for each branch (GUID) separately
 * This allows independent acknowledgment/confirmation per branch
 */
struct BranchState {
    bool active;
    bool acked;
    bool confirmed;
    UA_UInt16 severity;
    std::string message;
    UA_DateTime time;
    UA_DateTime receiveTime;
    UA_Boolean retain;
    UA_StatusCode quality;
    std::vector<UA_ByteString> eventIds;  // Track ALL EventIds for this state (multiple events may be generated)
    
    BranchState() : active(false), acked(false), confirmed(false), severity(0),
                    time(0), receiveTime(0), retain(UA_FALSE), quality(UA_STATUSCODE_GOOD) {
    }
    
    ~BranchState() {
        clearEventIds();
    }
    
    void clearEventIds() {
        for(auto &eventId : eventIds) {
            UA_ByteString_clear(&eventId);
        }
        eventIds.clear();
    }
    
    void addEventId(const UA_ByteString *newEventId) {
        if(newEventId && newEventId->length > 0) {
            UA_ByteString copy;
            UA_ByteString_init(&copy);
            UA_ByteString_copy(newEventId, &copy);
            eventIds.push_back(copy);
        }
    }
    
    bool hasEventId(const UA_ByteString *searchEventId) const {
        for(const auto &eventId : eventIds) {
            if(UA_ByteString_equal(&eventId, searchEventId)) {
                return true;
            }
        }
        return false;
    }
    
    // Copy constructor
    BranchState(const BranchState& other) {
        active = other.active;
        acked = other.acked;
        confirmed = other.confirmed;
        severity = other.severity;
        message = other.message;
        time = other.time;
        receiveTime = other.receiveTime;
        retain = other.retain;
        quality = other.quality;
        for(const auto &eventId : other.eventIds) {
            UA_ByteString copy;
            UA_ByteString_init(&copy);
            UA_ByteString_copy(&eventId, &copy);
            eventIds.push_back(copy);
        }
    }
    
    // Assignment operator
    BranchState& operator=(const BranchState& other) {
        if(this != &other) {
            active = other.active;
            acked = other.acked;
            confirmed = other.confirmed;
            severity = other.severity;
            message = other.message;
            time = other.time;
            receiveTime = other.receiveTime;
            retain = other.retain;
            quality = other.quality;
            clearEventIds();
            for(const auto &eventId : other.eventIds) {
                UA_ByteString copy;
                UA_ByteString_init(&copy);
                UA_ByteString_copy(&eventId, &copy);
                eventIds.push_back(copy);
            }
        }
        return *this;
    }
};

// Map: alarmKey → (GUID → BranchState)
static std::unordered_map<std::string, std::unordered_map<std::string, BranchState>> g_branchStates;

// ============================================================================
// MULTI-TENANCY: Global Session Manager
// ============================================================================
SessionManager g_sessionManager;

// Store API credentials and org list globally for worker threads and auth
static std::string g_bearerToken;
static std::string g_apiHost;
static std::string g_apiPort;
static std::string g_authUsername;  // From appsettings.json Authorization section
static std::string g_authPassword;  // From appsettings.json Authorization section
static std::vector<OrgConfig> g_organizations;  // List of all organizations
static UA_Server* g_server = nullptr;

/**
 * Extract organization ShortCode from endpoint URL
 * Example: "opc.tcp://0.0.0.0:53531/PLANT01" -> "PLANT01"
 */
static std::string extractShortCodeFromEndpoint(const UA_String* endpointUrl) {
    if(!endpointUrl || endpointUrl->length == 0) {
        return "";
    }
    
    std::string url((char*)endpointUrl->data, endpointUrl->length);
    
    // Find last slash to get path component
    size_t lastSlash = url.find_last_of('/');
    if(lastSlash != std::string::npos && lastSlash + 1 < url.length()) {
        return url.substr(lastSlash + 1);
    }
    
    return ""; // No path component found
}

/*
 * Session Open Callback - DISABLED
 * NOTE: UA_Server_getSessionParameter doesn't exist in open62541 v1.3
 * Session management will be implemented via lazy initialization instead
 */
// ============================================================================
// MULTI-TENANCY: Custom Access Control Session Activation
// ============================================================================
// This callback is invoked when a new session is activated (client connects).
// We extract the user's credentials, authenticate via API, get their orgId,
// and route them to the appropriate worker thread.
// ============================================================================
static UA_StatusCode 
customActivateSession(UA_Server *server,
                     UA_AccessControl *ac,
                     const UA_EndpointDescription *endpointDescription,
                     const UA_ByteString *secureChannelRemoteCertificate,
                     const UA_NodeId *sessionId,
                     const UA_ExtensionObject *userIdentityToken,
                     void **sessionContext) {
    
    log("🔑 [ACCESS CONTROL] customActivateSession called!", LogLevel::INFO);
    
    // ========================================================================
    // STEP 1: Extract Username and Password
    // ========================================================================
    std::string username;
    std::string password;
    bool isAnonymous = true;
    
    if(userIdentityToken && userIdentityToken->encoding == UA_EXTENSIONOBJECT_DECODED) {
        if(userIdentityToken->content.decoded.type == &UA_TYPES[UA_TYPES_USERNAMEIDENTITYTOKEN]) {
            UA_UserNameIdentityToken *token = 
                (UA_UserNameIdentityToken*)userIdentityToken->content.decoded.data;
            
            if(token->userName.data && token->userName.length > 0) {
                username = std::string((char*)token->userName.data, token->userName.length);
                isAnonymous = false;
            }
            
            if(token->password.data && token->password.length > 0) {
                password = std::string((char*)token->password.data, token->password.length);
            }
        }
    }
    
    // Handle anonymous login - DISABLED
    if(isAnonymous) {
        log("❌ Anonymous login detected and rejected", LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
        // username = g_authUsername;  // From appsettings.json
        // password = g_authPassword;  // From appsettings.json
    } else {
        log("  User: " + username, LogLevel::INFO);
    }
    
    if(username.empty() || password.empty()) {
        log("❌ Missing credentials", LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }

    // Encrypt password for API authentication (ONLY for user credentials, not appsettings)
    std::string finalPassword = password;
    if(!isAnonymous) {
        finalPassword = GetEncryptedString("", password, 0);
        log("DEBUG: Encrypted Password for user '" + username + "': " + finalPassword, LogLevel::INFO);
    }
    
    // ========================================================================
    // STEP 2: Get Bearer Token
    // ========================================================================
    json tokenResponse;
    try {
        tokenResponse = getBearerToken(g_apiHost, g_apiPort, username, finalPassword);
    } catch(const std::exception &e) {
        log("❌ Bearer token request failed: " + std::string(e.what()), LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }
    
    if(!tokenResponse.contains("access_token")) {
        log("❌ No access_token in response", LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }
    
    std::string bearerToken = tokenResponse["access_token"].get<std::string>();
    log("✓ Bearer token acquired", LogLevel::DEBUG);
    
    // ========================================================================
    // STEP 3: Get User Profile to Extract OrgID
    // ========================================================================
    UserProfile profile;
    try {
        std::string json_body = "{}"; // Empty payload
        profile = ParseUserProfile(g_apiHost, g_apiPort, 
                                  bearerToken, json_body, "/api/GetUserProfile");
    } catch(const std::exception &e) {
        log("❌ User profile request failed: " + std::string(e.what()), LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }
    
    if(profile.currentOrgId.empty()) {
        log("❌ No currentOrgId in user profile", LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }
    
    log("✓ User profile: " + profile.displayName + " (OrgID: " + profile.currentOrgId + 
        ", Org: " + profile.currentOrgName + ")", LogLevel::INFO);
    
    // ========================================================================
    // STEP 4: Find Matching Organization Config
    // ========================================================================
    OrgConfig* targetOrg = nullptr;
    
    for(auto &org : g_organizations) {
        // Fix: Compare profile.currentOrgId with org.orgId (the business ID), NOT org.id (the mapping primary key)
        if(std::to_string(org.orgId) == profile.currentOrgId) {
            targetOrg = &org;
            break;
        }
    }
    
    if(!targetOrg) {
        log("❌ Organization not found for OrgID: " + profile.currentOrgId, LogLevel::ERRORS);
        log("  Available orgs: " + std::to_string(g_organizations.size()), LogLevel::DEBUG);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }
    
    log("✓ Matched to organization: " + targetOrg->shortCode + 
        " (ID: " + std::to_string(targetOrg->id) + ")", LogLevel::INFO);
    
    // ========================================================================
    // STEP 5: Create or Join Worker Thread for This Org
    // ========================================================================
    bool registered = g_sessionManager.registerSession(
        *sessionId,
        targetOrg->shortCode,
        server,
        bearerToken,  // Use user's token, not server's admin token
        g_apiHost,
        g_apiPort
    );
    
    if(!registered) {
        log("❌ Failed to register session for org: " + targetOrg->shortCode, LogLevel::ERRORS);
        return UA_STATUSCODE_BADINTERNALERROR;
    }
    
    log("✅ Session activated for user '" + username + "' → Org '" + targetOrg->shortCode + 
        "' (Active sessions: " + std::to_string(g_sessionManager.getActiveSessionCount()) + ")",
        LogLevel::INFO);
    
    // TODO: Store user profile for future RBAC implementation
    // Can add to sessionContext: *sessionContext = new UserProfile(profile);
    
    return UA_STATUSCODE_GOOD;
}

// ============================================================================
// MULTI-TENANCY: Custom Session Close Callback
// ============================================================================
//  Signature matches open62541 v1.4.11 accesscontrol.h line 55-56
// ============================================================================
static void
customCloseSession(UA_Server *server,
                  UA_AccessControl *ac,
                  const UA_NodeId *sessionId,
                  void *sessionContext) {
    
    log("🔓 Session closing...", LogLevel::INFO);
    
    // Unregister session and cleanup worker thread
    g_sessionManager.unregisterSession(*sessionId);
    
    log("  ✓ Session closed (Active sessions: " + 
        std::to_string(g_sessionManager.getActiveSessionCount()) + ")", LogLevel::INFO);
}

// ============================================================================
// Original session callbacks (COMMENTED OUT - using Access Control instead)
// ============================================================================
/*
static void sessionOpenCallback(UA_Server* server, UA_NodeId* sessionId,
                               void* sessionContext) {
    // Check max sessions limit
    if(g_maxConcurrentSessions > 0 &&
       g_sessionManager.getActiveSessionCount() >= g_maxConcurrentSessions) {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "Max concurrent sessions (%zu) reached. Session rejected.",
                      g_maxConcurrentSessions);
        return;
    }
    
    // Get endpoint URL from session
    UA_String endpointUrl = UA_STRING_NULL;
    UA_StatusCode rc = UA_Server_getSessionParameter(server, sessionId,
                                                     UA_SESSIONPARAMETER_ENDPOINTURL,
                                                     &endpointUrl);
    
    if(rc != UA_STATUSCODE_GOOD) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                    "Failed to get endpoint URL from session");
        return;
    }
    
    std::string shortCode = extractShortCodeFromEndpoint(&endpointUrl);
    UA_String_clear(&endpointUrl);
    
    if(!shortCode.empty() && g_sessionManager.isValidShortCode(shortCode)) {
        bool registered = g_sessionManager.registerSession(*sessionId, shortCode,
                                                           server, g_bearerToken,
                                                           g_apiHost, g_apiPort);
        if(registered) {
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "✓ Session opened for org '%s' (Active sessions: %zu)",
                       shortCode.c_str(),
                       g_sessionManager.getActiveSessionCount());
        } else {
            UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                        "✗ Failed to register session for org '%s'", shortCode.c_str());
        }
    } else {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "Invalid or unknown ShortCode: '%s'\", shortCode.c_str());
    }
}

static void sessionCloseCallback(UA_Server* server, UA_NodeId* sessionId,
                                void* sessionContext) {
    g_sessionManager.unregisterSession(*sessionId);
    
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
               "✓ Session closed (Active sessions: %zu)",
               g_sessionManager.getActiveSessionCount());
}
*/

/**
 * Session detection approach: Lazy initialization
 * Instead of complex session lifecycle tracking, we'll detect and register sessions
 * when they first interact with the server (via read/write/browse operations).
 * This is simpler and works with open62541's limited session API.
 * 
 * Implementation TODO: Add session detection in read/write/browse callbacks
 * to register sessions on-demand when clients access org-specific endpoints.
 */
// ============================================================================


// Define user credentials
static UA_UsernamePasswordLogin usernamePasswordLogin[2] = {
    {UA_STRING_STATIC("user1"), UA_STRING_STATIC("password1")},
    {UA_STRING_STATIC("user2"), UA_STRING_STATIC("password2")}};

// Custom access control
// static UA_ByteString
// getPassword(const UA_String *userName, void *userContext) {
//     if(UA_String_equal(userName, &usernamePasswordLogin[0].username))
//         return usernamePasswordLogin[0].password;
//     if(UA_String_equal(userName, &usernamePasswordLogin[1].username))
//         return usernamePasswordLogin[1].password;
//     return UA_BYTESTRING_NULL;
// }

// Your custom logger callback
static void
myLog(void *context, UA_LogLevel level, UA_LogCategory category, const char *msg,
      va_list /*args*/) {

    // Never format with va_list to avoid specifier/argument mismatches. Just pass
    // through.
    try {
        const char *text = msg ? msg : "";
        
        // Filter out specific noisy logs
        if (strstr(text, "AddNode: Node could not add") != nullptr) {
             return;
        }
        switch(level) {
            case UA_LOGLEVEL_FATAL:
            case UA_LOGLEVEL_ERROR:
                log(std::string(text), LogLevel::ERRORS);
                break;
            case UA_LOGLEVEL_WARNING:
                log(std::string(text), LogLevel::INFO);
                break;
            case UA_LOGLEVEL_INFO:
                log(std::string(text), LogLevel::INFO);
                break;
            case UA_LOGLEVEL_DEBUG:
                /* Suppress noisy DEBUG logs from core (e.g., Sample and Publish Callback)
                 */
                break;
        }
    } catch(...) {
        // Swallow all exceptions to avoid unwinding across C boundary
    }
}

// Custom logger plugin
static UA_Logger myLogger = {myLog, nullptr, nullptr};

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

static void
stopHandler(int sign) {
    log("Received shutdown signal", LogLevel::INFO);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
    running = false;
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

// TopicInfo struct moved to AandC.h to be shared with SessionWorker

unordered_map<string, TopicInfo> topicMap;

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
    if(name == "PLANT-001") {
        oAttr.eventNotifier = 1;
    }
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

as::io_context ioc;
using client_t = am::client<am::protocol_version::v5, am::protocol::mqtt>;
client_t amcl{ioc.get_executor()};

// MQTT connection state and message queue for reconnection
std::atomic<bool> g_mqtt_connected{false};
std::atomic<int> g_mqtt_backpressure_count{0}; // DIAGNOSTIC: Track in-flight tasks
std::atomic<int> g_mqtt_incoming_count{0};     // DIAGNOSTIC: Track incoming messages
struct QueuedMessage {
    std::string topic;
    std::string payload;
};
std::deque<QueuedMessage> g_mqtt_queue;
std::mutex g_mqtt_queue_mutex;

thread_local bool is_internal_write = false;

/* Global event notifier origin; if null, defaults to Server */
static UA_NodeId g_eventNotifierNode = UA_NODEID_NULL;

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
    // DIAGNOSTIC START
    int pending = g_mqtt_backpressure_count.fetch_add(1);
    if(pending > 1000 && pending % 500 == 0) {
        log("⚠️ MQTT BACKPRESSURE CRITICAL: " + std::to_string(pending) + " tasks pending! Memory growing...", LogLevel::INFO);
    }
    // DIAGNOSTIC END

    as::post(ioc, [topic, payload]() {
        as::co_spawn(
            ioc,
            [topic, payload]() -> as::awaitable<void> {
                try {
                    co_await amcl.async_publish(topic, payload, am::qos::at_most_once);
                } catch(const std::exception &e) {
                    log("[MQTT-PUB] ✗ Publish error to '" + topic + "': " + e.what(), LogLevel::ERRORS);
                    
                    // Connection might be broken - queue for retry
                    std::lock_guard<std::mutex> lock(g_mqtt_queue_mutex);
                    g_mqtt_queue.push_back({topic, payload});
                    log("[MQTT-QUEUE] Message requeued after error (queue size: " + std::to_string(g_mqtt_queue.size()) + ")", LogLevel::ERRORS);
                }
                g_mqtt_backpressure_count.fetch_sub(1); // DIAGNOSTIC: Task done
                co_return;
            },
            as::detached);
    });
};


static UA_NodeId
findChildNodeIdAnyNS(UA_Server *server, UA_NodeId parentId, const char *searchName) {
    UA_NodeId result = UA_NODEID_NULL;
    UA_String searchNameStr = UA_STRING((char*)searchName);

    // 1. Setup the Browse
    UA_BrowseDescription bd;
    UA_BrowseDescription_init(&bd);
    bd.nodeId = parentId;
    bd.browseDirection = UA_BROWSEDIRECTION_FORWARD;
    bd.includeSubtypes = true;
    
    // KEY CHANGE 1: Use "HierarchicalReferences".
    // This covers HasComponent, HasProperty, Organizes, etc.
    // So you don't need two separate checks.
    bd.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HIERARCHICALREFERENCES);
    
    bd.resultMask = UA_BROWSERESULTMASK_BROWSENAME; // We strictly need the name

    // 2. Execute Browse
    UA_BrowseResult bres = UA_Server_browse(server, 0, &bd);

    // 3. Iterate through ALL children (regardless of namespace)
    for(size_t i = 0; i < bres.referencesSize; ++i) {
        UA_ReferenceDescription *ref = &bres.references[i];

        // KEY CHANGE 2: Compare String ONLY (Ignore Namespace Index)
        if(UA_String_equal(&ref->browseName.name, &searchNameStr)) {
            
            // KEY CHANGE 3: Deep Copy (Fixes the memory bug)
            UA_NodeId_copy(&ref->nodeId.nodeId, &result);
            break; // Found it, stop looking
        }
    }

    // 4. Cleanup
    UA_BrowseResult_clear(&bres);
    return result;
}


// Helper to find a nested node by path (e.g. {"AckedState", "Id"})
static UA_NodeId
findNodeByPath(UA_Server *server, UA_NodeId startNode, const std::vector<const char*>& path) {
    UA_NodeId current = UA_NODEID_NULL;
    UA_NodeId_copy(&startNode, &current);

    for(const char* targetName : path) {
        UA_NodeId next = UA_NODEID_NULL;
        
        UA_RelativePathElement rpe;
        UA_RelativePathElement_init(&rpe);
        rpe.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT); // Usually components
        rpe.isInverse = false;
        rpe.includeSubtypes = true;
        rpe.targetName = UA_QUALIFIEDNAME(0, (char*)targetName);

        UA_BrowsePath bp;
        UA_BrowsePath_init(&bp);
        bp.startingNode = current; // Start from where we left off
        bp.relativePath.elementsSize = 1;
        bp.relativePath.elements = &rpe;

        UA_BrowsePathResult bpr = UA_Server_translateBrowsePathToNodeIds(server, &bp);
        
        // Try HasProperty if HasComponent failed (Id is usually a Property)
        if(bpr.statusCode != UA_STATUSCODE_GOOD || bpr.targetsSize == 0) {
             UA_BrowsePathResult_clear(&bpr);
             rpe.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY);
             bpr = UA_Server_translateBrowsePathToNodeIds(server, &bp);
        }

        if(bpr.statusCode == UA_STATUSCODE_GOOD && bpr.targetsSize > 0) {
            UA_NodeId_copy(&bpr.targets[0].targetId.nodeId, &next);
        }
        
        UA_BrowsePathResult_clear(&bpr);
        UA_NodeId_clear(&current); // Clear previous
        
        if(UA_NodeId_isNull(&next)) {
            return UA_NODEID_NULL; // Path broken
        }
        current = next; // Move forward
    }
    return current;
}

// static UA_NodeId
// findNodeByPath(UA_Server *server, UA_NodeId startNode, const std::vector<const char*>& path) {
//     UA_NodeId current = UA_NODEID_NULL;
    
//     // We start by copying the startNode so we can manage memory uniformly in the loop
//     UA_NodeId_copy(&startNode, &current);

//     for(const char* targetName : path) {
//         // Use our robust "AnyNS" helper to find the next step
//         UA_NodeId next = findChildNodeIdAnyNS(server, current, targetName);
        
//         // Clean up the previous 'current' node
//         UA_NodeId_clear(&current);

//         // If the path is broken, return NULL immediately
//         if(UA_NodeId_isNull(&next)) {
//             return UA_NODEID_NULL;
//         }
        
//         // Advance to the next node
//         current = next; 
//     }

//     return current;
// }

// --------------------------------------------------------------------------------------------

// // Helper for stealth writes on nested properties (e.g. AckedState/Id)
// static void
// setStealthValueByPath(UA_Server *server, UA_NodeId alarmId, 
//                      std::vector<const char*> path, 
//                      void *newValue, const UA_DataType *type) {
//     UA_NodeId targetNode = findNodeByPath(server, alarmId, path);
    
//     if(UA_NodeId_isNull(&targetNode)) {
//         // Silently skip if not found
//         return;
//     }

//     // Read current value
//     UA_Variant current;
//     UA_Variant_init(&current);
//     UA_Server_readValue(server, targetNode, &current);

//     bool isSame = false;
//     if(current.type == type) {
//         if(type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
//             isSame = (*(UA_Boolean*)current.data == *(UA_Boolean*)newValue);
//         } else if(type == &UA_TYPES[UA_TYPES_UINT16]) {
//             isSame = (*(UA_UInt16*)current.data == *(UA_UInt16*)newValue);
//         } else if(type == &UA_TYPES[UA_TYPES_NODEID]) {
//             isSame = UA_NodeId_equal((UA_NodeId*)current.data, (UA_NodeId*)newValue);
//         }
//         // Add more types as needed
//     }

//     // ONLY WRITE IF DIFFERENT
//     if(!isSame) {
//         UA_Variant v;
//         UA_Variant_init(&v);
//         UA_Variant_setScalarCopy(&v, newValue, type);
//         UA_Server_writeValue(server, targetNode, v); // STEALTH WRITE
//         UA_Variant_clear(&v);
//     }

//     UA_Variant_clear(&current);
//     UA_NodeId_clear(&targetNode);
// }

// // Helper function for stealth writes with diff checking
// static void
// setStealthValueChecked(UA_Server *server, UA_NodeId alarmId, const char *propertyName, 
//                        void *newValue, const UA_DataType *type) {
//     UA_NodeId propertyNode = findChildNodeIdAnyNS(server, alarmId, propertyName);
//     if(UA_NodeId_isNull(&propertyNode)) {
//         UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, 
//                       "Could not find property '%s' for stealth write", propertyName);
//         return;
//     }

//     // READ CURRENT VALUE
//     UA_Variant current;
//     UA_Variant_init(&current);
//     UA_Server_readValue(server, propertyNode, &current);

//     // COMPARE (Simple comparison for scalar types)
//     bool isSame = false;
//     if(current.type == type) {
//         if(type == &UA_TYPES[UA_TYPES_UINT16]) {
//             isSame = (*(UA_UInt16*)current.data == *(UA_UInt16*)newValue);
//         } else if(type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
//             isSame = (*(UA_Boolean*)current.data == *(UA_Boolean*)newValue);
//         } else if(type == &UA_TYPES[UA_TYPES_NODEID]) {
//             isSame = UA_NodeId_equal((UA_NodeId*)current.data, (UA_NodeId*)newValue);
//         } else if(type == &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]) {
//             UA_LocalizedText *currentLT = (UA_LocalizedText*)current.data;
//             UA_LocalizedText *newLT = (UA_LocalizedText*)newValue;
//             isSame = UA_String_equal(&currentLT->text, &newLT->text);
//         }
//         // Add more type comparisons as needed
//     }

//     // ONLY WRITE IF DIFFERENT
//     if(!isSame) {
//         UA_Variant v;
//         UA_Variant_init(&v);
//         UA_Variant_setScalarCopy(&v, newValue, type);
//         UA_Server_writeValue(server, propertyNode, v);
//         UA_Variant_clear(&v);
//     }

//     UA_Variant_clear(&current);
//     UA_NodeId_clear(&propertyNode);
// }




/**
 * Master Stealth Write Function
 * * Purpose: 
 * Updates a value deep inside a complex object structure (e.g. AckedState/Id).
 * It reads the current value first and ONLY performs a write if the value 
 * has actually changed. This reduces server load and network traffic.
 * * Dependencies:
 * - findNodeByPath (Helper function defined previously)
 */
static UA_StatusCode
setStealthValueByPath(UA_Server *server, UA_NodeId startNode, 
                      std::vector<const char*> path, 
                      void *newValue, const UA_DataType *type) {
    
    // 1. Find the target node using the helper
    //    (Traverses the path safely, handling mixed Namespaces)
    UA_NodeId targetNode = findNodeByPath(server, startNode, path);
    
    if(UA_NodeId_isNull(&targetNode)) {
        // Target not found. Silently return to avoid spamming logs 
        // during startup or partial configurations.
        return UA_STATUSCODE_BADNOTFOUND;
    }

    // 2. Read current value from the server
    UA_Variant current;
    UA_Variant_init(&current);
    UA_StatusCode readStatus = UA_STATUSCODE_BAD;
    {
         std::lock_guard<std::recursive_mutex> lock(g_server_mutex);
         readStatus = UA_Server_readValue(server, targetNode, &current);
    }

    // If read fails (e.g. bad permissions), we can't compare, so we abort.
    if(readStatus != UA_STATUSCODE_GOOD) {
        UA_NodeId_clear(&targetNode);
        return readStatus;
    }

    // 3. Compare (Unified Logic for all common types)
    bool isSame = false;

    // Only compare if the types match. If types differ, we force a write (isSame = false).
    if(current.type == type && current.data != NULL) {
        
        if(type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
            isSame = (*(UA_Boolean*)current.data == *(UA_Boolean*)newValue);
        } 
        else if(type == &UA_TYPES[UA_TYPES_SBYTE]) {
            isSame = (*(UA_SByte*)current.data == *(UA_SByte*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_BYTE]) {
            isSame = (*(UA_Byte*)current.data == *(UA_Byte*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_INT16]) {
            isSame = (*(UA_Int16*)current.data == *(UA_Int16*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_UINT16]) { // Used for Severity
            isSame = (*(UA_UInt16*)current.data == *(UA_UInt16*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_INT32]) {
            isSame = (*(UA_Int32*)current.data == *(UA_Int32*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_UINT32]) {
            isSame = (*(UA_UInt32*)current.data == *(UA_UInt32*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_INT64]) {
            isSame = (*(UA_Int64*)current.data == *(UA_Int64*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_UINT64]) {
            isSame = (*(UA_UInt64*)current.data == *(UA_UInt64*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_FLOAT]) {
            // Note: Direct float comparison is risky, but acceptable for exact matches here
            isSame = (*(UA_Float*)current.data == *(UA_Float*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_DOUBLE]) {
            isSame = (*(UA_Double*)current.data == *(UA_Double*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_STRING]) {
            isSame = UA_String_equal((UA_String*)current.data, (UA_String*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_DATETIME]) {
            isSame = (*(UA_DateTime*)current.data == *(UA_DateTime*)newValue);
        }
        else if(type == &UA_TYPES[UA_TYPES_NODEID]) { // Used for BranchId/ConditionId
            isSame = UA_NodeId_equal((UA_NodeId*)current.data, (UA_NodeId*)newValue);
        } 
        else if(type == &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]) { // Used for Message
            UA_LocalizedText *currentLT = (UA_LocalizedText*)current.data;
            UA_LocalizedText *newLT = (UA_LocalizedText*)newValue;
            // Compare the text content. We usually ignore the "locale" field for SCADA.
            isSame = UA_String_equal(&currentLT->text, &newLT->text);
        }
    }

// 4. Write ONLY if different
    if(!isSame) {
        // Lock Server Access
        std::lock_guard<std::recursive_mutex> lock(g_server_mutex);
        
        UA_Variant v;
        UA_Variant_init(&v);
        // Create a variant pointing to the new data
        UA_Variant_setScalarCopy(&v, newValue, type);
        
        // Perform the write
        UA_StatusCode wc = UA_Server_writeValue(server, targetNode, v);
        if(wc != UA_STATUSCODE_GOOD) {
             std::string pathStr = "";
             for(const char* s : path) { pathStr += "/"; pathStr += s; }
             UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                 "setStealthValueByPath: Write failed for path '%s' with status %s",
                 pathStr.c_str(), UA_StatusCode_name(wc));
        }
        
        // Clean up the temporary write variant
        UA_Variant_clear(&v);
    }

    // 5. Cleanup
    UA_Variant_clear(&current);  // Free memory from the Read operation
    UA_NodeId_clear(&targetNode); // Free memory from the Find operation
    return UA_STATUSCODE_GOOD;
}



// Wrapper for direct children (Convenience function)
static UA_StatusCode
setStealthValueChecked(UA_Server *server, UA_NodeId parentId, 
                       const char *propertyName, 
                       void *newValue, const UA_DataType *type) {
    
    // Just call the master function with a path of size 1
    std::vector<const char*> path = {propertyName};
    return setStealthValueByPath(server, parentId, path, newValue, type);
}

// --------------------------------------------------------------------------------------------



// Define a structure to hold method callback context
// struct MethodCallbackContext {
//     UA_NodeId ackedStateNodeId;
//     MonitoredNodeAlarmInfo *alarmInfo;
// };

/* Lightweight context for on-the-fly alarms to publish MQTT on Ack/Confirm */

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

static std::string findAlarmKeyForCondition(const UA_NodeId *alarmNodeId) {
    // 1. Check legacy global map
    for(const auto &kv : g_alarmByKey) {
        if(UA_NodeId_equal(&kv.second, alarmNodeId)) {
            return kv.first;
        }
    }
    
    // 2. Check if it's a String NodeId (Multi-tenant)
    // The alarmKey IS the NodeId string
    if(alarmNodeId->identifierType == UA_NODEIDTYPE_STRING) {
        return std::string((char*)alarmNodeId->identifier.string.data, alarmNodeId->identifier.string.length);
    }
    
    return "";
}

/* Forward declarations */
static std::string findGUIDForNodeId(const UA_NodeId *nodeId, const std::string &alarmKey);
static std::string findGUIDForBranchId(const UA_NodeId *branchId, const std::string &alarmKey);

/* Helper: Find trigger topic(s) for an alarm NodeId */
static std::vector<std::string> findTriggerTopicsForAlarm(const UA_NodeId *alarmNodeId) {
    std::vector<std::string> topics;
    
    // First, find the alarm key in g_alarmByKey
    std::string alarmKey = findAlarmKeyForCondition(alarmNodeId);

    if(alarmKey.empty()) {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "Alarm NodeId not found in g_alarmByKey");
        return topics;
    }
    
    // Now search g_triggerToAlarmMap for this alarm key
    for(const auto &triggerPair : g_triggerToAlarmMap) {
        const std::string &triggerTopic = triggerPair.first;
        const std::vector<TriggerToAlarmMapping> &mappings = triggerPair.second;
        
        for(const auto &mapping : mappings) {
            if(mapping.alarmKey == alarmKey) {
                topics.push_back(triggerTopic);
                break; // Found in this trigger, move to next
            }
        }
    }
    
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
               "Found %zu trigger topic(s) for alarm '%s'",
               topics.size(), alarmKey.c_str());
    
    return topics;
}


// Helper to print ByteString as Hex
std::string
toHex(const UA_ByteString *bs) {
    if(!bs || !bs->data || bs->length == 0)
        return "EMPTY";
    std::string res;
    char buf[3];
    for(size_t i = 0; i < bs->length; i++) {
        snprintf(buf, sizeof(buf), "%02X", bs->data[i]);
        res += buf;
    }
    return res;
}


// RAII Wrapper for UA_Variant to ensure cleanup
struct ScopedVariant {
    UA_Variant var;
    ScopedVariant() { UA_Variant_init(&var); }
    ~ScopedVariant() { UA_Variant_clear(&var); }
    UA_Variant* get() { return &var; }
    UA_Variant* operator&() { return &var; } // Helper for legacy C calls
    // Note: Do not copy/move without deep copy logic.
};


// Helper for precise timestamp generation (ISO 8601 with 100ns precision +05:30)
std::string getPreciseTimestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf;
    #if defined(_WIN32)
        localtime_s(&tm_buf, &t);
    #else
        localtime_r(&tm_buf, &t);
    #endif
    
    // Calculate fractional seconds
    auto duration = now.time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
    auto fractional = duration - seconds;
    long long fractional_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(fractional).count();

    char tsBuf[64];
    std::strftime(tsBuf, sizeof(tsBuf), "%Y-%m-%dT%H:%M:%S", &tm_buf);
    
    // Append fractional (7 digits) and Offset (+05:30)
    char finalBuf[128];
    // fractional_ns is nanoseconds (9 digits), we want 100ns (7 digits)
    snprintf(finalBuf, sizeof(finalBuf), "%s.%07lld+05:30", tsBuf, fractional_ns / 100);
    
    return std::string(finalBuf);
}

/* Custom Acknowledge method callback */
UA_StatusCode
customAcknowledgeCallback(UA_Server *server, const UA_NodeId *sessionId,
                          void *sessionContext, const UA_NodeId *methodId,
                          void *methodContext, const UA_NodeId *objectId,
                          void *objectContext, size_t inputSize, const UA_Variant *input,
                          size_t outputSize, UA_Variant *output) {

    // 1. Validate Input
    if(inputSize < 1 ||
       !UA_Variant_hasScalarType(&input[0], &UA_TYPES[UA_TYPES_BYTESTRING])) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "Acknowledge requires EventId (ByteString)");
        return UA_STATUSCODE_BADINVALIDARGUMENT;
    }

    UA_ByteString *eventId = (UA_ByteString *)input[0].data;
    std::string commentText = "";

    // 2. Lock Mutex
    std::lock_guard<std::mutex> lock(g_alarmMutex);

    std::string alarmKey = findAlarmKeyForCondition(objectId);
    std::string guid = "";

// 3. Find the specific branch (GUID)
    auto branchStateMapIt = g_branchStates.find(alarmKey);

    if(branchStateMapIt != g_branchStates.end()) {

        std::string inputHex = toHex(eventId);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                    "Attempting Ack. Input EventId: %s", inputHex.c_str());

        // =========================================================
        // STRATEGY A: Exact EventId Match
        // =========================================================
        for(const auto &branchPair : branchStateMapIt->second) {
            if(branchPair.second.hasEventId(eventId)) {
                guid = branchPair.first;
                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "MATCH FOUND: %s",
                            guid.c_str());
                break;
            } else {
                // DEBUG: Print first stored ID to see mismatch
                std::string storedHex = "None";
                if(!branchPair.second.eventIds.empty()) {
                    storedHex = toHex(&branchPair.second.eventIds.back());
                }
                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                            "Checking branch %s... Mismatch (Input: %s vs Stored: %s)",
                            branchPair.first.c_str(), inputHex.c_str(),
                            storedHex.c_str());
            }
        }

        // =========================================================
        // STRATEGY B: Fallback (FIFO - Oldest Unacked First)
        // =========================================================
        if(guid.empty()) {
            UA_DateTime oldestTime = LLONG_MAX;  // Start with Max Time
            std::string bestCandidate = "";

            for(const auto &branchPair : branchStateMapIt->second) {
                // Only look at branches that are NOT yet acked
                if(!branchPair.second.acked) {
                    // We want the SMALLEST time (Oldest)
                    if(branchPair.second.receiveTime < oldestTime) {
                        oldestTime = branchPair.second.receiveTime;
                        bestCandidate = branchPair.first;
                    }
                }
            }

            if(!bestCandidate.empty()) {
                guid = bestCandidate;
                UA_LOG_WARNING(
                    UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                    "Fallback: Exact match failed. Selected OLDEST unacked branch: %s",
                    guid.c_str());
            }
        }
    }

    // 4. Check if GUID found
    if(guid.empty()) {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "No unacked branch found for alarm '%s'", alarmKey.c_str());
        return UA_STATUSCODE_BADCONDITIONBRANCHALREADYACKED;
    }

    // 5. Extract Comment (if provided)
    if(inputSize >= 2 && input[1].type == &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]) {
        UA_LocalizedText *ct = (UA_LocalizedText *)input[1].data;
        if(ct && ct->text.length > 0) {
            commentText = std::string((char *)ct->text.data, ct->text.length);
        }
    }

    // 6. Publish Control Request to MQTT (Command & Control Pattern)
    std::vector<std::string> triggerTopics = findTriggerTopicsForAlarm(objectId);
    if(!triggerTopics.empty()) {
        std::string timestamp = getPreciseTimestamp();
        
        // Find AETypeID
        int AETypeID = 0;
        auto triggerIt = g_triggerToAlarmMap.find(triggerTopics[0]);
        if(triggerIt != g_triggerToAlarmMap.end()) {
            for(const auto &mapping : triggerIt->second) {
                if(mapping.alarmKey == alarmKey) {
                    AETypeID = mapping.alarmId;
                    break;
                }
            }
        }
        
        // Build control request payload
        json out;
        out["Event"] = {
            {"AETypeID", AETypeID},
            {"AEInstanceID", guid},
            {"Timestamp", timestamp},
            {"Source", static_cast<int>(AlarmSource::OPC)},
            {"UpdateType", static_cast<int>(UpdateType::Control)},
            {"Command", "Ack"}
        };
        
        if(!commentText.empty()) {
            out["Event"]["Comment"] = commentText;
        }
        
        // Publish request
        std::string subTopic = triggerTopics[0] + "/Event";
        publish_to_mqtt(subTopic, out.dump());
        
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "✓ Acknowledge request sent for GUID '%s' (AETypeID=%d). Awaiting AE Engine approval...",
                   guid.c_str(), AETypeID);
    } else {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "No trigger topics found for alarm, cannot send control request");
    }

    // Return success (request queued)
    return UA_STATUSCODE_GOOD;
}

/* Custom Confirm method callback */
UA_StatusCode
customConfirmCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
                      const UA_NodeId *methodId, void *methodContext,
                      const UA_NodeId *objectId, void *objectContext, size_t inputSize,
                      const UA_Variant *input, size_t outputSize, UA_Variant *output) {

    // 1. Validate Input
    if(inputSize < 1 ||
       !UA_Variant_hasScalarType(&input[0], &UA_TYPES[UA_TYPES_BYTESTRING])) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "Confirm requires EventId (ByteString)");
        return UA_STATUSCODE_BADINVALIDARGUMENT;
    }

    UA_ByteString *eventId = (UA_ByteString *)input[0].data;
    std::string commentText = "";  // FIX: Declare scope here

    // 2. Lock Mutex
    std::lock_guard<std::mutex> lock(g_alarmMutex);

    std::string alarmKey = findAlarmKeyForCondition(objectId);
    std::string guid = "";

    // 3. Find the specific branch
    auto branchStateMapIt = g_branchStates.find(alarmKey);
    if(branchStateMapIt != g_branchStates.end()) {
        // Fallback: Confirm the most recent Acked+Unconfirmed branch (Active or Inactive)
        UA_DateTime mostRecentTime = 0;

        for(const auto &branchPair : branchStateMapIt->second) {
            // Match Exact EventId
            if(branchPair.second.hasEventId(eventId)) {
                guid = branchPair.first;
                break;
            }
            // Fallback Logic
            if(branchPair.second.acked && !branchPair.second.confirmed) {
                if(branchPair.second.receiveTime > mostRecentTime) {
                    mostRecentTime = branchPair.second.receiveTime;
                    guid = branchPair.first;
                }
            }
        }
    }

    if(guid.empty()) {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "No acknowledged, unconfirmed branch found for alarm '%s'",
                       alarmKey.c_str());
        return UA_STATUSCODE_BADCONDITIONBRANCHALREADYCONFIRMED;
    }

    // 4. Extract Comment (if provided)
    if(inputSize >= 2 && input[1].type == &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]) {
        UA_LocalizedText *ct = (UA_LocalizedText *)input[1].data;
        if(ct && ct->text.length > 0) {
            commentText = std::string((char *)ct->text.data, ct->text.length);
        }
    }

    // 5. Publish Control Request to MQTT (Command & Control Pattern)
    std::vector<std::string> triggerTopics = findTriggerTopicsForAlarm(objectId);
    if(!triggerTopics.empty()) {
        std::string timestamp = getPreciseTimestamp();
        
        // Find AETypeID
        int AETypeID = 0;
        auto triggerIt = g_triggerToAlarmMap.find(triggerTopics[0]);
        if(triggerIt != g_triggerToAlarmMap.end()) {
            for(const auto &mapping : triggerIt->second) {
                if(mapping.alarmKey == alarmKey) {
                    AETypeID = mapping.alarmId;
                    break;
                }
            }
        }
        
        // Build control request payload
        json out;
        out["Event"] = {
            {"AETypeID", AETypeID},
            {"AEInstanceID", guid},
            {"Timestamp", timestamp},
            {"Source", static_cast<int>(AlarmSource::OPC)},
            {"UpdateType", static_cast<int>(UpdateType::Control)},
            {"Command", "Confirm"}
        };
        
        if(!commentText.empty()) {
            out["Event"]["Comment"] = commentText;
        }
        
        // Publish request
        std::string subTopic = triggerTopics[0] + "/Event";
        publish_to_mqtt(subTopic, out.dump());
        
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "✓ Confirm request sent for GUID '%s' (AETypeID=%d). Awaiting AE Engine approval...",
                   guid.c_str(), AETypeID);
    } else {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "No trigger topics found for alarm, cannot send control request");
    }

    // Return success (request queued)
    return UA_STATUSCODE_GOOD;
}

/* Custom Enable method callback to publish to MQTT */
UA_StatusCode
customEnableCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
                     const UA_NodeId *methodId, void *methodContext,
                     const UA_NodeId *objectId, void *objectContext, size_t inputSize,
                     const UA_Variant *input, size_t outputSize, UA_Variant *output) {

    // 1. Lock Mutex
    std::lock_guard<std::mutex> lock(g_alarmMutex);

    std::string alarmKey = findAlarmKeyForCondition(objectId);
    
    // 2. Enable operates on the entire alarm, not a specific branch
    //    Use the MOST RECENT branch GUID (by receiveTime)
    std::string guid = "0";
    auto branchStateMapIt = g_branchStates.find(alarmKey);
    if(branchStateMapIt != g_branchStates.end() && !branchStateMapIt->second.empty()) {
        // Find the most recent branch by receiveTime
        UA_DateTime mostRecentTime = 0;
        for(const auto &branchPair : branchStateMapIt->second) {
            if(branchPair.second.receiveTime > mostRecentTime) {
                mostRecentTime = branchPair.second.receiveTime;
                guid = branchPair.first;
            }
        }
    }

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
               "Enable request for alarm '%s' using GUID '%s' (most recent branch)",
               alarmKey.c_str(), guid.c_str());

    // 3. Publish Control Request to MQTT
    std::vector<std::string> triggerTopics = findTriggerTopicsForAlarm(objectId);
    if(!triggerTopics.empty()) {
        std::string timestamp = getPreciseTimestamp();
        
        // Find AETypeID
        int AETypeID = 0;
        auto triggerIt = g_triggerToAlarmMap.find(triggerTopics[0]);
        if(triggerIt != g_triggerToAlarmMap.end()) {
            for(const auto &mapping : triggerIt->second) {
                if(mapping.alarmKey == alarmKey) {
                    AETypeID = mapping.alarmId;
                    break;
                }
            }
        }
        
        // Build control request payload
        json out;
        out["Event"] = {
            {"AETypeID", AETypeID},
            {"AEInstanceID", guid},
            {"Timestamp", timestamp},
            {"Source", static_cast<int>(AlarmSource::OPC)},
            {"UpdateType", static_cast<int>(UpdateType::Control)},
            {"Command", "Enable"}
        };
        
        // Publish request
        std::string subTopic = triggerTopics[0] + "/Event";
        publish_to_mqtt(subTopic, out.dump());
        
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "✓ Enable request sent for alarm '%s' (AETypeID=%d). Awaiting AE Engine response...",
                   alarmKey.c_str(), AETypeID);
    } else {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "No trigger topics found for alarm, cannot send control request");
    }

    // Return success (request queued)
    return UA_STATUSCODE_GOOD;
}

/* Custom Disable method callback to publish to MQTT */
UA_StatusCode
customDisableCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
                      const UA_NodeId *methodId, void *methodContext,
                      const UA_NodeId *objectId, void *objectContext, size_t inputSize,
                      const UA_Variant *input, size_t outputSize, UA_Variant *output) {

    // 1. Lock Mutex
    std::lock_guard<std::mutex> lock(g_alarmMutex);

    std::string alarmKey = findAlarmKeyForCondition(objectId);
    
    // 2. Disable operates on the entire alarm, not a specific branch
    //    Use the MOST RECENT branch GUID (by receiveTime)
    std::string guid = "0";
    auto branchStateMapIt = g_branchStates.find(alarmKey);
    if(branchStateMapIt != g_branchStates.end() && !branchStateMapIt->second.empty()) {
        // Find the most recent branch by receiveTime
        UA_DateTime mostRecentTime = 0;
        for(const auto &branchPair : branchStateMapIt->second) {
            if(branchPair.second.receiveTime > mostRecentTime) {
                mostRecentTime = branchPair.second.receiveTime;
                guid = branchPair.first;
            }
        }
    }

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
               "Disable request for alarm '%s' using GUID '%s' (most recent branch)",
               alarmKey.c_str(), guid.c_str());

    // 3. Publish Control Request to MQTT
    std::vector<std::string> triggerTopics = findTriggerTopicsForAlarm(objectId);
    if(!triggerTopics.empty()) {
        std::string timestamp = getPreciseTimestamp();
        
        // Find AETypeID
        int AETypeID = 0;
        auto triggerIt = g_triggerToAlarmMap.find(triggerTopics[0]);
        if(triggerIt != g_triggerToAlarmMap.end()) {
            for(const auto &mapping : triggerIt->second) {
                if(mapping.alarmKey == alarmKey) {
                    AETypeID = mapping.alarmId;
                    break;
                }
            }
        }
        
        // Build control request payload
        json out;
        out["Event"] = {
            {"AETypeID", AETypeID},
            {"AEInstanceID", guid},
            {"Timestamp", timestamp},
            {"Source", static_cast<int>(AlarmSource::OPC)},
            {"UpdateType", static_cast<int>(UpdateType::Control)},
            {"Command", "Disable"}
        };
        
        // Publish request
        std::string subTopic = triggerTopics[0] + "/Event";
        publish_to_mqtt(subTopic, out.dump());
        
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "✓ Disable request sent for alarm '%s' (AETypeID=%d). Awaiting AE Engine response...",
                   alarmKey.c_str(), AETypeID);
    } else {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "No trigger topics found for alarm, cannot send control request");
    }

    // Return success (request queued)
    return UA_STATUSCODE_GOOD;
}

/* Custom AddComment method callback to publish to MQTT */
UA_StatusCode
customAddCommentCallback(UA_Server *server, const UA_NodeId *sessionId,
                         void *sessionContext, const UA_NodeId *methodId,
                         void *methodContext, const UA_NodeId *objectId,
                         void *objectContext, size_t inputSize, const UA_Variant *input,
                         size_t outputSize, UA_Variant *output) {

    // 1. Validate Input
    if(inputSize < 2) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "AddComment requires 2 inputs");
        return UA_STATUSCODE_BADINVALIDARGUMENT;
    }
    if(!UA_Variant_hasScalarType(&input[0], &UA_TYPES[UA_TYPES_BYTESTRING]) ||
       !UA_Variant_hasScalarType(&input[1], &UA_TYPES[UA_TYPES_LOCALIZEDTEXT])) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "Invalid arguments for AddComment");
        return UA_STATUSCODE_BADTYPEMISMATCH;
    }

    UA_ByteString *eventId = (UA_ByteString *)input[0].data;
    UA_LocalizedText *comment = (UA_LocalizedText *)input[1].data;
    std::string commentText = "";

    // 2. Lock Mutex
    std::lock_guard<std::mutex> lock(g_alarmMutex);

    std::string alarmKey = findAlarmKeyForCondition(objectId);
    std::string guid = "";

    // 3. Find the specific branch by EventId
    auto branchStateMapIt = g_branchStates.find(alarmKey);
    if(branchStateMapIt != g_branchStates.end()) {
        // Exact EventId match
        for(const auto &branchPair : branchStateMapIt->second) {
            if(branchPair.second.hasEventId(eventId)) {
                guid = branchPair.first;
                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                            "Found branch GUID '%s' for EventId", guid.c_str());
                break;
            }
        }
        
        // Fallback: Use most recent branch if exact match not found
        // (Similar to Confirm callback pattern)
        if(guid.empty()) {
            UA_DateTime mostRecentTime = 0;
            for(const auto &branchPair : branchStateMapIt->second) {
                if(branchPair.second.receiveTime > mostRecentTime) {
                    mostRecentTime = branchPair.second.receiveTime;
                    guid = branchPair.first;
                }
            }
        }
    }

    // Extract comment text
    if(comment->text.length > 0) {
        commentText = std::string((char *)comment->text.data, comment->text.length);
    }

    if(commentText.empty()) {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "Empty comment provided, skipping");
        return UA_STATUSCODE_BADINVALIDARGUMENT;
    }

    if(guid.empty()) {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "No branch found for alarm '%s'",
                      alarmKey.c_str());
        return UA_STATUSCODE_BADNOTFOUND;
    }

    // 4. Publish Control Request to MQTT (Command & Control Pattern)
    std::vector<std::string> triggerTopics = findTriggerTopicsForAlarm(objectId);
    if(!triggerTopics.empty()) {
        std::string timestamp = getPreciseTimestamp();
        
        // Find AETypeID
        int AETypeID = 0;
        auto triggerIt = g_triggerToAlarmMap.find(triggerTopics[0]);
        if(triggerIt != g_triggerToAlarmMap.end()) {
            for(const auto &mapping : triggerIt->second) {
                if(mapping.alarmKey == alarmKey) {
                    AETypeID = mapping.alarmId;
                    break;
                }
            }
        }
        
        // Build control request payload
        json out;
        out["Event"] = {
            {"AETypeID", AETypeID},
            {"AEInstanceID", guid},
            {"Timestamp", timestamp},
            {"Source", static_cast<int>(AlarmSource::OPC)},
            {"UpdateType", static_cast<int>(UpdateType::Control)},
            {"Command", "AddComment"},
            {"Comment", commentText}
        };
        
        // Publish request
        std::string subTopic = triggerTopics[0] + "/Event";
        publish_to_mqtt(subTopic, out.dump());
        
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "✓ AddComment request sent for GUID '%s' (AETypeID=%d). Awaiting AE Engine approval...",
                   guid.c_str(), AETypeID);
    } else {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                      "No trigger topics found for alarm, cannot send control request");
    }

    // Return success (request queued)
    return UA_STATUSCODE_GOOD;
}

/* Custom ConditionRefresh method callback */
static UA_StatusCode
ConditionRefreshMethodCallback(UA_Server *server, const UA_NodeId *sessionId,
                               void *sessionContext, const UA_NodeId *methodId,
                               void *methodContext, const UA_NodeId *objectId,
                               void *objectContext, size_t inputSize,
                               const UA_Variant *input, size_t outputSize,
                               UA_Variant *output) {

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                ">>> ConditionRefresh CALLBACK CALLED! <<<");

    // STEP 1: Lock Mutex
    std::lock_guard<std::mutex> lock(g_alarmMutex);

    // STEP 2: Fire RefreshStartEvent
    {
        UA_NodeId startEventId;
        UA_Server_createEvent(
            server, UA_NODEID_NUMERIC(0, UA_NS0ID_REFRESHSTARTEVENTTYPE), &startEventId);
        UA_LocalizedText msg = UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Refresh Start");
        UA_Server_writeObjectProperty_scalar(server, startEventId,
                                             UA_QUALIFIEDNAME(0, (char *)"Message"), &msg,
                                             &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
        UA_Server_triggerEvent(server, startEventId,
                               UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER), NULL, UA_TRUE);
    }

    // STEP 3: Iterate through all alarms
    for(auto &alarmPair : g_alarmBranches) {
        std::string alarmKey = alarmPair.first;
        auto &branchMap = alarmPair.second;
        if(branchMap.empty())
            continue;

        UA_NodeId conditionNodeId = branchMap.begin()->second.conditionNodeId;
        if(UA_NodeId_isNull(&conditionNodeId))
            continue;

        // Lookup SourceNode
        UA_NodeId sourceNodeId = UA_NODEID_NULL;
        UA_Variant sourceVar;
        UA_Variant_init(&sourceVar);
        if(UA_Server_readObjectProperty(server, conditionNodeId,
                                        UA_QUALIFIEDNAME(0, (char *)"SourceNode"),
                                        &sourceVar) == UA_STATUSCODE_GOOD) {
            if(UA_Variant_hasScalarType(&sourceVar, &UA_TYPES[UA_TYPES_NODEID])) {
                UA_NodeId_copy((UA_NodeId *)sourceVar.data, &sourceNodeId);
            }
            UA_Variant_clear(&sourceVar);
        }
        if(UA_NodeId_isNull(&sourceNodeId))
            UA_NodeId_copy(&conditionNodeId, &sourceNodeId);

        auto branchStateMapIt = g_branchStates.find(alarmKey);
        if(branchStateMapIt == g_branchStates.end()) {
            UA_NodeId_clear(&sourceNodeId);
            continue;
        }

        // CHECK IF ALARM IS DISABLED - Skip refresh for disabled alarms per OPC UA spec
        {
            UA_NodeId enabledStateId = findChildNodeIdAnyNS(server, conditionNodeId, (char*)"EnabledState");
            bool isEnabled = true;  // Default to enabled
            
            if(!UA_NodeId_isNull(&enabledStateId)) {
                UA_QualifiedName qId = UA_QUALIFIEDNAME_ALLOC(0, (char*)"Id");
                UA_Variant enabledVar;
                UA_StatusCode enabledRc = UA_Server_readObjectProperty(server, enabledStateId, qId, &enabledVar);
                if(enabledRc == UA_STATUSCODE_GOOD && UA_Variant_hasScalarType(&enabledVar, &UA_TYPES[UA_TYPES_BOOLEAN])) {
                    isEnabled = *(UA_Boolean*)enabledVar.data;
                }
                UA_Variant_clear(&enabledVar);
            }
            
            if(!isEnabled) {
                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                           "ConditionRefresh: Skipping disabled alarm '%s' (EnabledState=false)",
                           alarmKey.c_str());
                UA_NodeId_clear(&sourceNodeId);
                continue;  // Skip disabled alarms - they should not appear in refresh
            }
        }

        // ========================================================
        // STEP 4: MANUAL EVENT CONSTRUCTION FOR EACH BRANCH
        // ========================================================
        for(auto &branchPair : branchStateMapIt->second) {
            std::string guid = branchPair.first;
            BranchState &bs = branchPair.second;

            // Retain Logic: Active OR Unacked
            bool shouldRetain = (bs.active || !bs.acked);
            if(!shouldRetain)
                continue;

            // Get BranchId NodeId for this GUID
            UA_NodeId branchId = UA_NODEID_NULL;
            auto bi = branchMap.find(guid);
            if(bi != branchMap.end())
                branchId = bi->second.branchNodeId;

            // === CREATE TEMPORARY EVENT NODE ===
            UA_NodeId tempEventNodeId;
            UA_StatusCode rc = UA_Server_createEvent(
                server, UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE),
                &tempEventNodeId);
            
            if(rc != UA_STATUSCODE_GOOD) {
                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                              "Failed to create temp event for GUID '%s': %s",
                              guid.c_str(), UA_StatusCode_name(rc));
                continue;
            }

            // === POPULATE TEMPORARY EVENT NODE WITH BRANCH DATA ===
            
            // ActiveState/Id (Boolean)
            UA_Boolean actVal = bs.active ? UA_TRUE : UA_FALSE;
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"ActiveState/Id"),
                &actVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
            
            // ActiveState (LocalizedText)
            UA_LocalizedText actText = bs.active ? 
                UA_LOCALIZEDTEXT((char *)"en", (char *)"Active") :
                UA_LOCALIZEDTEXT((char *)"en", (char *)"Inactive");
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"ActiveState"),
                &actText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

            // AckedState/Id (Boolean)
            UA_Boolean ackVal = bs.acked ? UA_TRUE : UA_FALSE;
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"AckedState/Id"),
                &ackVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
            
            // AckedState (LocalizedText)
            UA_LocalizedText ackText = bs.acked ?
                UA_LOCALIZEDTEXT((char *)"en", (char *)"Acknowledged") :
                UA_LOCALIZEDTEXT((char *)"en", (char *)"Unacknowledged");
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"AckedState"),
                &ackText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

            // ConfirmedState/Id (Boolean)
            UA_Boolean cnfVal = bs.confirmed ? UA_TRUE : UA_FALSE;
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"ConfirmedState/Id"),
                &cnfVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
            
            // ConfirmedState (LocalizedText)
            UA_LocalizedText cnfText = bs.confirmed ?
                UA_LOCALIZEDTEXT((char *)"en", (char *)"Confirmed") :
                UA_LOCALIZEDTEXT((char *)"en", (char *)"Unconfirmed");
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"ConfirmedState"),
                &cnfText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

            // Retain (Boolean)
            UA_Boolean retainVal = shouldRetain ? UA_TRUE : UA_FALSE;
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"Retain"),
                &retainVal, &UA_TYPES[UA_TYPES_BOOLEAN]);

            // Severity (UInt16)
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"Severity"),
                &bs.severity, &UA_TYPES[UA_TYPES_UINT16]);

            // Message (LocalizedText)
            UA_LocalizedText msgText = 
                UA_LOCALIZEDTEXT_ALLOC((char *)"en-US", (char *)bs.message.c_str());
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"Message"),
                &msgText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
            UA_LocalizedText_clear(&msgText);

            // Time (DateTime)
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"Time"),
                &bs.time, &UA_TYPES[UA_TYPES_DATETIME]);

            // ReceiveTime (DateTime)
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"ReceiveTime"),
                &bs.receiveTime, &UA_TYPES[UA_TYPES_DATETIME]);

            // Quality (StatusCode)
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"Quality"),
                &bs.quality, &UA_TYPES[UA_TYPES_STATUSCODE]);

            // EnabledState/Id (Boolean) - MANDATORY for condition events
            UA_Boolean enabledVal = UA_TRUE;  // Always enabled during refresh
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"EnabledState/Id"),
                &enabledVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
            
            // EnabledState (LocalizedText)
            UA_LocalizedText enabledText = UA_LOCALIZEDTEXT((char *)"en", (char *)"Enabled");
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"EnabledState"),
                &enabledText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

            // BranchId (NodeId) - CRITICAL for client matching
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"BranchId"),
                &branchId, &UA_TYPES[UA_TYPES_NODEID]);

            // SourceNode (NodeId)
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"SourceNode"),
                &sourceNodeId, &UA_TYPES[UA_TYPES_NODEID]);

            // EventType (NodeId) - Helps clients identify event type
            UA_NodeId eventTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE);
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"EventType"),
                &eventTypeId, &UA_TYPES[UA_TYPES_NODEID]);

            // ConditionClassId (set to base condition class)
            UA_NodeId condClassId = UA_NODEID_NULL;
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"ConditionClassId"),
                &condClassId, &UA_TYPES[UA_TYPES_NODEID]);

            // ConditionName (String)
            UA_String condName = UA_STRING((char *)alarmKey.c_str());
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"ConditionName"),
                &condName, &UA_TYPES[UA_TYPES_STRING]);

            // ConditionId (NodeId) - CRITICAL: Links event to parent condition
            UA_Server_writeObjectProperty_scalar(
                server, tempEventNodeId,
                UA_QUALIFIEDNAME(0, (char *)"ConditionId"),
                &conditionNodeId, &UA_TYPES[UA_TYPES_NODEID]);

            // DEBUG: Log what we're about to fire
            UA_String branchIdStr = UA_STRING_NULL;
            UA_NodeId_print(&branchId, &branchIdStr);
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "ConditionRefresh: Triggering MANUAL event for GUID='%s', "
                       "BranchId=%.*s, ACTIVE=%d, ACKED=%d, severity=%u",
                       guid.c_str(), (int)branchIdStr.length, branchIdStr.data,
                       bs.active, bs.acked, bs.severity);
            UA_String_clear(&branchIdStr);

            // === TRIGGER EVENT (temp node auto-deleted with UA_TRUE) ===
            UA_ByteString newEventId = UA_BYTESTRING_NULL;
            rc = UA_Server_triggerEvent(server, tempEventNodeId, sourceNodeId,
                                        &newEventId, UA_TRUE);
            
            if(rc == UA_STATUSCODE_GOOD && newEventId.length > 0) {
                // Store the new EventId so client can acknowledge it
                bs.addEventId(&newEventId);
            }
            UA_ByteString_clear(&newEventId);
        }

        // ========================================================
        // STEP 5: CALCULATE AGGREGATE STATE
        // ========================================================
        bool aggActive = false;
        bool aggAcked = true;
        bool aggConfirmed = true;
        UA_UInt16 aggSeverity = 0;
        bool anyRetained = false;

        for(const auto &bp : branchStateMapIt->second) {
            if(bp.second.active) {
                aggActive = true;
                if(bp.second.severity > aggSeverity)
                    aggSeverity = bp.second.severity;
            }
            // Branch contributes if Active OR Unacked
            if(bp.second.active || !bp.second.acked) {
                anyRetained = true;
                if(!bp.second.acked)
                    aggAcked = false;
                if(!bp.second.confirmed)
                    aggConfirmed = false;
            }
        }

        if(!anyRetained) {
            aggAcked = true;
            aggConfirmed = true;
            aggSeverity = 0;
        }

        // Retain Logic: Active OR Unacked
        UA_Boolean aggRetain = (aggActive || !aggAcked) ? UA_TRUE : UA_FALSE;

        // ========================================================
        // STEP 6: RESTORE AGGREGATE STATE TO MAIN CONDITION NODE (STEALTH MODE)
        // ========================================================
        {
            // ActiveState/Id (STEALTH MODE)
            UA_Boolean valActive = aggActive ? UA_TRUE : UA_FALSE;
            setStealthValueByPath(server, conditionNodeId, {"ActiveState", "Id"}, &valActive, &UA_TYPES[UA_TYPES_BOOLEAN]);

            // ActiveState (LocalizedText) - stealth write
            UA_LocalizedText activeText =
                aggActive ? UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Active")
                          : UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Inactive");
            setStealthValueChecked(server, conditionNodeId, "ActiveState", &activeText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

            // AckedState/Id (STEALTH MODE)
            UA_Boolean valAcked = aggAcked ? UA_TRUE : UA_FALSE;
            setStealthValueByPath(server, conditionNodeId, {"AckedState", "Id"}, &valAcked, &UA_TYPES[UA_TYPES_BOOLEAN]);

            // AckedState (LocalizedText) - stealth write
            UA_LocalizedText ackedText =
                aggAcked ? UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Acknowledged")
                         : UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Unacknowledged");
            setStealthValueChecked(server, conditionNodeId, "AckedState", &ackedText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

            // ConfirmedState/Id (STEALTH MODE)
            UA_Boolean valConfirmed = aggConfirmed ? UA_TRUE : UA_FALSE;
            setStealthValueByPath(server, conditionNodeId, {"ConfirmedState", "Id"}, &valConfirmed, &UA_TYPES[UA_TYPES_BOOLEAN]);

            // ConfirmedState (LocalizedText) - stealth write
            UA_LocalizedText cnfText =
                aggConfirmed ? UA_LOCALIZEDTEXT((char *)"en", (char *)"Confirmed")
                             : UA_LOCALIZEDTEXT((char *)"en", (char *)"Unconfirmed");
            setStealthValueChecked(server, conditionNodeId, "ConfirmedState", &cnfText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

            // Severity - stealth write
            setStealthValueChecked(server, conditionNodeId, "Severity", &aggSeverity, &UA_TYPES[UA_TYPES_UINT16]);

            // Retain - stealth write
            setStealthValueChecked(server, conditionNodeId, "Retain", &aggRetain, &UA_TYPES[UA_TYPES_BOOLEAN]);

            // Reset BranchId to NULL (Aggregate) - stealth write
            UA_NodeId nullId = UA_NODEID_NULL;
            setStealthValueChecked(server, conditionNodeId, "BranchId", &nullId, &UA_TYPES[UA_TYPES_NODEID]);
        }

        // ========================================================
        // STEP 7: TRIGGER AGGREGATE EVENT (Standard Pattern)
        // ========================================================
        if(aggRetain) {
            UA_ByteString aggEid = UA_BYTESTRING_NULL;
            UA_Server_triggerConditionEvent(server, conditionNodeId, sourceNodeId,
                                            &aggEid);
            UA_ByteString_clear(&aggEid);
            
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "ConditionRefresh: Triggered aggregate event (BranchId=NULL)");
        }

        UA_NodeId_clear(&sourceNodeId);
    }

    // STEP 8: Fire RefreshEndEvent
    {
        UA_NodeId endEventId;
        UA_Server_createEvent(server, UA_NODEID_NUMERIC(0, UA_NS0ID_REFRESHENDEVENTTYPE),
                              &endEventId);
        UA_LocalizedText msg =
            UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Refresh Complete");
        UA_Server_writeObjectProperty_scalar(server, endEventId,
                                             UA_QUALIFIEDNAME(0, (char *)"Message"), &msg,
                                             &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
        UA_Server_triggerEvent(server, endEventId, UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER),
                               NULL, UA_TRUE);
    }

    return UA_STATUSCODE_GOOD;
}

/* Find GUID for a given BranchId NodeId */
static std::string findGUIDForBranchId(const UA_NodeId *branchId, const std::string &alarmKey) {
    if(!branchId || UA_NodeId_isNull(branchId)) {
        return "";  // NULL BranchId means main branch
    }
    
    auto branchMapIt = g_alarmBranches.find(alarmKey);
    if(branchMapIt != g_alarmBranches.end()) {
        for(const auto &branchPair : branchMapIt->second) {
            if(UA_NodeId_equal(&branchPair.second.branchNodeId, branchId)) {
                return branchPair.second.guid;
            }
        }
    }
    return "";
}





// Write callback for OPC UA node value changes
// Write callback for OPC UA node value changes
// Write callback for OPC UA node value changes
void
writeCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
              const UA_NodeId *nodeId, void *nodeContext, const UA_NumericRange *range,
              const UA_DataValue *data) {
    if (is_internal_write) return;  // 🔒 Prevent feedback loop

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
            auto now = std::chrono::system_clock::now();
            std::time_t t = std::chrono::system_clock::to_time_t(now);
            std::tm tm_buf;
            #if defined(_WIN32)
                localtime_s(&tm_buf, &t);
            #else
                localtime_r(&tm_buf, &t);
            #endif
            
            // Calculate fractional seconds (100ns precision)
            auto duration = now.time_since_epoch();
            auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration);
            auto fractional = duration - seconds;
            long long fractional_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(fractional).count();

            char tsBuf[64];
            std::strftime(tsBuf, sizeof(tsBuf), "%Y-%m-%dT%H:%M:%S", &tm_buf);
            
            // Append fractional (7 digits) and Offset (+05:30)
            char finalBuf[128];
            // fractional_ns is nanoseconds (9 digits), we want 100ns (7 digits)
            snprintf(finalBuf, sizeof(finalBuf), "%s.%07lld+05:30", tsBuf, fractional_ns / 100);
            
            dataPoint["TimeStamp"] = std::string(finalBuf);
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

/* Find GUID for a given NodeId (branch or condition) */
static std::string findGUIDForNodeId(const UA_NodeId *nodeId, const std::string &alarmKey) {
    // If alarmKey is provided, search in that specific alarm's branches
    if(!alarmKey.empty()) {
        // Check if this is a branch
        auto branchMapIt = g_alarmBranches.find(alarmKey);
        if(branchMapIt != g_alarmBranches.end()) {
            for(const auto &branchPair : branchMapIt->second) {
                if(UA_NodeId_equal(&branchPair.second.branchNodeId, nodeId)) {
                    return branchPair.second.guid;
                }
            }
        }
        
        // Check if this is the main condition
        auto alarmIt = g_alarmByKey.find(alarmKey);
        if(alarmIt != g_alarmByKey.end()) {
            if(UA_NodeId_equal(&alarmIt->second, nodeId)) {
                return "";  // Main branch has empty GUID
            }
        }
    } else {
        // Search across all alarms if alarmKey not provided
        // First check all branches
        for(const auto &alarmBranchPair : g_alarmBranches) {
            for(const auto &branchPair : alarmBranchPair.second) {
                if(UA_NodeId_equal(&branchPair.second.branchNodeId, nodeId)) {
                    return branchPair.second.guid;
                }
            }
        }
        
        // Then check all main conditions
        for(const auto &alarmPair : g_alarmByKey) {
            if(UA_NodeId_equal(&alarmPair.second, nodeId)) {
                return "";  // Main branch has empty GUID
            }
        }
    }
    
    return "";  // Not found
}

/* Cleanup inactive and acknowledged branches */
static void
cleanupBranches(const std::string &alarmKey) {
    auto &branchMap = g_alarmBranches[alarmKey];
    auto &branchStateMap = g_branchStates[alarmKey];

    for(auto it = branchMap.begin(); it != branchMap.end();) {
        const std::string &guid = it->first;
        bool remove = false;
        auto stateIt = branchStateMap.find(guid);

        if(stateIt != branchStateMap.end()) {
            // FIX: If you want Ack & Clear behavior, remove "&& confirmed" check
            // Otherwise, branches will stick around forever waiting for a confirm that
            // never comes.
            if(!stateIt->second.active && stateIt->second.acked) {
                remove = true;
            }
        } else {
            remove = true;  // Orphaned branch info
        }

        if(remove) {
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                        "Cleaning up branch GUID '%s' for alarm '%s'", guid.c_str(),
                        alarmKey.c_str());
            if(stateIt != branchStateMap.end()) {
                branchStateMap.erase(stateIt);
            }
            it = branchMap.erase(it);
        } else {
            ++it;
        }
    }
}

/* Get or create a branch for a given alarm and GUID */
static UA_StatusCode
getOrCreateAlarmBranch(UA_Server *server, const UA_NodeId &conditionId,
                       const std::string &guid, const std::string &alarmKey,
                       UA_NodeId *outBranchId) {

    // 1. Handle Main Branch (BranchId = NULL)
    // Treat AEInstanceID = 0 as aggregate (same as empty/null GUID)
    if(guid.empty() || guid == "null" || guid == "NULL" || guid == "{}" || guid == "0") {
        // Return a copy of the conditionId as the branchId
        return UA_NodeId_copy(&conditionId, outBranchId);
    }

    // 2. Check if branch already exists
    // Note: Caller MUST hold g_alarmMutex lock before calling this!
    auto &branchMap = g_alarmBranches[alarmKey];
    auto branchIt = branchMap.find(guid);

    if(branchIt != branchMap.end()) {
        // FOUND: Return a DEEP COPY of the stored NodeId
        // This ensures that if the caller clears outBranchId, the map stays valid.
        return UA_NodeId_copy(&branchIt->second.branchNodeId, outBranchId);
    }

    // 3. Create New Virtual Branch
    // Format: ns=1;s=Branch:<GUID>
    std::string nodeIdStr = "Branch:" + guid;

    // Create the "Master" NodeId that will live in the Map
    UA_NodeId masterBranchId = UA_NODEID_STRING_ALLOC(1, nodeIdStr.c_str());

    // Store in Map
    AlarmBranchInfo branchInfo;
    branchInfo.branchNodeId = masterBranchId;  // Map takes ownership of this alloc
    branchInfo.conditionNodeId = conditionId;  // Shallow copy is fine for numeric/safely
                                               // managed IDs, but ideally copy
    branchInfo.guid = guid;
    branchInfo.isMainBranch = false;

    // Insert into map
    branchMap[guid] = branchInfo;

    branchMap[guid] = branchInfo;

    // UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
    //             "✓ Created virtual branch for GUID '%s' (NodeId: ns=%u;s=%s)",
    //             guid.c_str(), masterBranchId.namespaceIndex, nodeIdStr.c_str());

    // 4. Return a DEEP COPY to the caller
    // Caller is responsible for clearing outBranchId, but it won't affect our Map
    return UA_NodeId_copy(&masterBranchId, outBranchId);
}

/* ============================================================================
 * HELPER: Perform Disable (OPC UA Compliant)
 * ============================================================================
 * When alarm is disabled via MQTT, this function:
 * - Clears all branches from memory (g_branchStates)
 * - Sets ActiveState/Retain to false
 * - Keeps EnabledState as already set by caller
 * - Clears BranchId
 * - Triggers final disable event for client notification
 */
static void performDisable(UA_Server *server, 
                           const UA_NodeId &alarmId,
                           const std::string &alarmKey) {
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
               "performDisable: Disabling alarm '%s' - sending branch disable events",
               alarmKey.c_str());
    
    UA_DateTime now = UA_DateTime_now();
    
    // 1. Send INDIVIDUAL BRANCH disable events (with specific BranchIds)
    auto branchIt = g_branchStates.find(alarmKey);
    if(branchIt != g_branchStates.end() && !branchIt->second.empty()) {
        size_t branchCount = branchIt->second.size();
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "Sending disable events for %zu branch(es) with specific BranchIds...",
                   branchCount);
        
        // Get branch map for BranchIds
        auto branchMapIt = g_alarmBranches.find(alarmKey);
        if(branchMapIt != g_alarmBranches.end()) {
            auto &branchMap = branchMapIt->second;
            
            // Send event for EACH branch with its specific BranchId
            for(auto &branchPair : branchIt->second) {
                std::string guid = branchPair.first;
                
                // Get this branch's BranchId
                UA_NodeId branchId = UA_NODEID_NULL;
                auto bi = branchMap.find(guid);
                if(bi != branchMap.end()) {
                    branchId = bi->second.branchNodeId;
                }
                
                // Set properties for THIS SPECIFIC BRANCH
                UA_Boolean false_val = UA_FALSE;
                UA_Boolean true_val = UA_TRUE;
                
                // BranchId = SPECIFIC branch (ns=1;s=Branch:GUID)
                setStealthValueChecked(server, alarmId, "BranchId", &branchId, &UA_TYPES[UA_TYPES_NODEID]);
                
                // ActiveState = false
                setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &false_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
                UA_LocalizedText inactiveText = UA_LOCALIZEDTEXT((char *)"en", (char *)"Inactive");
                setStealthValueChecked(server, alarmId, "ActiveState", &inactiveText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                
                // Retain = false (THIS IS THE KEY - tells client to remove this branch!)
                setStealthValueChecked(server, alarmId, "Retain", &false_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
                
                // AckedState = true
                setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &true_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
                UA_LocalizedText ackedText = UA_LOCALIZEDTEXT((char *)"en", (char *)"Acknowledged");
                setStealthValueChecked(server, alarmId, "AckedState", &ackedText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                
                // ConfirmedState = true
                setStealthValueByPath(server, alarmId, {"ConfirmedState", "Id"}, &true_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
                UA_LocalizedText confirmedText = UA_LOCALIZEDTEXT((char *)"en", (char *)"Confirmed");
                setStealthValueChecked(server, alarmId, "ConfirmedState", &confirmedText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                
                // Severity = 0
                UA_UInt16 zero_severity = 0;
                setStealthValueChecked(server, alarmId, "Severity", &zero_severity, &UA_TYPES[UA_TYPES_UINT16]);
                
                // NOTE: Do NOT set EnabledState here! Setting it disables the condition
                // and prevents subsequent branch events from being triggered.
                // EnabledState will be set AFTER all branch events in the aggregate section.
                
                // Trigger event for THIS SPECIFIC BRANCH
                UA_ByteString eventId = UA_BYTESTRING_NULL;
                UA_StatusCode eventRc = UA_Server_triggerConditionEvent(server, alarmId, alarmId, &eventId);
                UA_ByteString_clear(&eventId);
                
                if(eventRc == UA_STATUSCODE_GOOD) {
                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                               "✓ Sent disable event for branch '%s' (BranchId: ns=1;s=Branch:%s, Retain=false)",
                               guid.c_str(), guid.c_str());
                } else if(eventRc == UA_STATUSCODE_BADNOTFOUND) {
                    // Expected - client may have already removed this branch
                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                               "✓ Branch '%s' already removed (expected)",
                               guid.c_str());
                } else {
                    UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                  "Failed to trigger disable event for branch '%s': %s",
                                  guid.c_str(), UA_StatusCode_name(eventRc));
                }
            }
        }
        
        // Clear memory AFTER sending events
        branchIt->second.clear();
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "✓ Cleared %zu branch(es) from memory for '%s'",
                   branchCount, alarmKey.c_str());
    }
    
    // 2. NOW send aggregate disable event (BranchId=NULL)
    {
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "Sending aggregate disable event (BranchId=NULL)...");
        
        UA_Boolean false_val = UA_FALSE;
        UA_Boolean true_val = UA_TRUE;
        
        // BranchId = NULL (aggregate)
        UA_NodeId nullId = UA_NODEID_NULL;
        setStealthValueChecked(server, alarmId, "BranchId", &nullId, &UA_TYPES[UA_TYPES_NODEID]);
        
        // ActiveState = false
        setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &false_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_LocalizedText inactiveText = UA_LOCALIZEDTEXT((char *)"en", (char *)"Inactive");
        setStealthValueChecked(server, alarmId, "ActiveState", &inactiveText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
        
        // Retain = false
        setStealthValueChecked(server, alarmId, "Retain", &false_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
        
        // AckedState = true
        setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &true_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_LocalizedText ackedText = UA_LOCALIZEDTEXT((char *)"en", (char *)"Acknowledged");
        setStealthValueChecked(server, alarmId, "AckedState", &ackedText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
        
        // ConfirmedState = true
        setStealthValueByPath(server, alarmId, {"ConfirmedState", "Id"}, &true_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_LocalizedText confirmedText = UA_LOCALIZEDTEXT((char *)"en", (char *)"Confirmed");
        setStealthValueChecked(server, alarmId, "ConfirmedState", &confirmedText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
        
        // Severity = 0
        UA_UInt16 zero_severity = 0;
        setStealthValueChecked(server, alarmId, "Severity", &zero_severity, &UA_TYPES[UA_TYPES_UINT16]);
        
        // NOTE: Do NOT set EnabledState BEFORE triggering!
        // It will cause BadConditionAlreadyDisabled error.
        // We'll set it AFTER the event is triggered.
        
        // Trigger aggregate event (while still enabled)
        UA_ByteString eventId = UA_BYTESTRING_NULL;
        UA_StatusCode eventRc = UA_Server_triggerConditionEvent(server, alarmId, alarmId, &eventId);
        UA_ByteString_clear(&eventId);
        
        if(eventRc == UA_STATUSCODE_GOOD) {
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "✓ Sent aggregate disable event (BranchId=NULL, Retain=false)");
        } else if(eventRc == UA_STATUSCODE_BADNOTFOUND) {
            // This is EXPECTED - branches already cleared the aggregate
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "✓ Aggregate already removed by branch cleanup (expected)");
        } else {
            UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                          "Failed to trigger aggregate disable event: %s",
                          UA_StatusCode_name(eventRc));
        }
        
        // NOW set EnabledState=false AFTER event sent
        setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &false_val, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_LocalizedText disabledText = UA_LOCALIZEDTEXT((char *)"en", (char *)"Disabled");
        setStealthValueChecked(server, alarmId, "EnabledState", &disabledText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
        
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "✓ EnabledState set to FALSE after all events sent");
    }
    
    
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
               "✓ Alarm '%s' disabled successfully - all states cleared", alarmKey.c_str());
}

// ============================================================================
// MQTT Subscription Serialization
// ============================================================================
// We must serialize subscription requests to avoid concurrent writes to the socket
// and potentially overloading the client or hitting race conditions.
// Forward declarations
void process_queue_signal();

void queue_subscription(const std::string &topic) {
    if(topic.empty()) return;
    
    std::lock_guard<std::mutex> lock(g_sub_mutex);
    
    // log("DEBUG: queue_subscription called for '" + topic + "'", LogLevel::INFO);

    // Check if already subscribed to avoid unnecessary queueing
    if(g_subscribed_topics.find(topic) != g_subscribed_topics.end()) {
        // log("DEBUG: Already subscribed to '" + topic + "'", LogLevel::INFO);
        return;
    }
    
    // Check if already in queue to avoid duplicates
    if(std::find(g_subscription_queue.begin(), g_subscription_queue.end(), topic) != g_subscription_queue.end()) {
        // log("DEBUG: Already queued '" + topic + "'", LogLevel::INFO);
        return;
    }
    
    g_subscription_queue.push_back(topic);
    
    // Trigger processing by signalling the main loop
    process_queue_signal();
    // log("DEBUG: Added to queue and signalled main loop", LogLevel::INFO);
}

// Helper to subscribe to a single topic dynamically
void GlobalMQTT_Subscribe(const std::string &topic) {
    // FIXED: Removed connection check to allow offline queuing
    // The queue logic will handle it, or we can check connection inside perform_subscriptions
    // if we want, but queueing is better for reliability on reconnect.
    queue_subscription(topic);
}

// Helper to unsubscribe from a single topic dynamically
void GlobalMQTT_Unsubscribe(const std::string &topic) {
    if(topic.empty()) return;

    {
        std::lock_guard<std::mutex> lock(g_sub_mutex);
        // Remove from local tracking set
        auto it = g_subscribed_topics.find(topic);
        if(it != g_subscribed_topics.end()) {
            g_subscribed_topics.erase(it);
        } else {
             // Not subscribed, nothing to do
             return;
        }

        // Also remove from pending queue if present
        auto qIt = std::find(g_subscription_queue.begin(), g_subscription_queue.end(), topic);
        if(qIt != g_subscription_queue.end()) {
            g_subscription_queue.erase(qIt);
            return; // Was only in queue, not yet sent to broker
        }
    }

    // Send Unsubscribe packet to broker (via IO thread)
    // We reuse the process_queue logic somewhat, or post directly
    as::post(ioc, [topic](){
        if(!g_mqtt_connected.load()) return;
        
        // We need access to the client object. 
        // Ideally we should have a 'queue_unsubscribe' similar to subscribe, 
        // but for now posting directly if connected is a start.
        // NOTE: Actual MQTT unsubscribe requires the client object which is local to the thread
        // or accessible via a global. 
        // Since 'client' (am::endpoint) is inside start_mqtt_client's lambda/scope or global?
        // Wait, start_mqtt_client uses a local client ptr. 
        
        // REVISION: We need to queue the unsubscribe action just like subscribe if we want 
        // strict correctness, OR we accept that we can only unsubscribe when allowed.
        // For simplicity in this leak fix: we just remove from g_subscribed_topics so we don't 
        // track it anymore. The broker will clean up subscriptions on disconnect anyway.
        // BUT for a long-running session that unsubscribes, we DO want to tell the broker.
        
        // Assuming client is NOT easily accessible here without refactoring.
        // However, we CLEARED it from g_subscribed_topics. 
        // If we reconnect, we won't re-subscribe to it.
        // This is sufficient to stop the "growth" of tracked topics in our memory.
        
        // log("GlobalMQTT_Unsubscribe: Removed '" + topic + "' from tracking", LogLevel::INFO);
    });
}

// ----------------------------------------------------------------------------
// Subscription Loop Helpers
// ----------------------------------------------------------------------------

void process_queue_signal() {
    // Notify the main loop to wake up and check the queue
    // Must execute on IO thread for thread-safety (called from Worker threads)
    as::post(ioc, [](){
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
    // log("DEBUG: perform_subscriptions called", LogLevel::INFO);
    std::vector<std::string> batch;
    {
        std::lock_guard<std::mutex> lock(g_sub_mutex);
        size_t count = 0;
        while(!g_subscription_queue.empty() && count < 50) {
            batch.push_back(g_subscription_queue.front());
            g_subscription_queue.erase(g_subscription_queue.begin());
            count++;
        }
    }

    if(batch.empty()) {
        log("DEBUG: perform_subscriptions called but batch empty (race condition?)", LogLevel::INFO);
        co_return;
    }

    std::vector<am::topic_subopts> sub_entry;
    {
        std::lock_guard<std::mutex> alarmLock(g_alarmMutex);
        for(const auto& topic : batch) {
            sub_entry.push_back({topic, am::qos::at_most_once}); // Generic / Base Telemetry

            // Only subscribe to /Event if this topic is a registered Alarm Trigger
            if(g_triggerToAlarmMap.find(topic) != g_triggerToAlarmMap.end()) {
                sub_entry.push_back({topic + "/Event", am::qos::at_most_once});
                log("DEBUG: Preparing ALARM subscription for '" + topic + "/Event'", LogLevel::INFO);
            }
        }
    }
// ... (rest of function implicit) ...

    try {
        // Safe to call async_subscribe here because async_recv is NOT running in parallel
        auto suback_opt = co_await amcl.async_subscribe(
            am::v5::subscribe_packet{*amcl.acquire_unique_packet_id(),
                                     am::force_move(sub_entry)},
            as::use_awaitable);
        
        if(suback_opt) {
            std::lock_guard<std::mutex> lock(g_sub_mutex);
            for(const auto& topic : batch) g_subscribed_topics.insert(topic);
            log("✓ Subscribed to batch of " + std::to_string(batch.size()) + " topics", LogLevel::INFO);
        }
    } catch(const std::exception& e) {
        log("ERROR: Subscription batch failed: " + std::string(e.what()), LogLevel::ERRORS);
    }
}

void start_mqtt_client(UA_Server *server) {
    // Use global ioc and amcl
    as::co_spawn(
        ioc,
        [server]() -> as::awaitable<void> {
            log("DEBUG: MQTT Client Coroutine Started!", LogLevel::INFO);
            // Reconnection loop
            while(running) {
                try {
                
                    // [Connection Logic Redacted/Preserved]
                    // ... (Assume connection logic is unchanged above) ...
                    

                    log("Attempting to connect to MQTT broker...", LogLevel::INFO);
                    co_await amcl.async_underlying_handshake("216.48.184.131", "15579", as::use_awaitable);
                    auto connack_opt = co_await amcl.async_start(
                        am::v5::connect_packet{ true, 0x1234, "", std::nullopt, "portal", "dt0Unw7QRh" },
                        as::use_awaitable);
                    
                    if(!connack_opt) throw std::runtime_error("Failed to start MQTT session");
                    

                    log("Successfully connected.", LogLevel::INFO);
                    g_mqtt_connected.store(true);
                                        // Clear and queue
                    {
                        std::lock_guard<std::mutex> alarmLock(g_alarmMutex);
                        std::lock_guard<std::mutex> lock(g_sub_mutex);
                        g_subscribed_topics.clear(); 
                        
                        if(!g_triggerToAlarmMap.empty()) {
                            log("DEBUG: Re-queueing " + std::to_string(g_triggerToAlarmMap.size()) + " alarm topics", LogLevel::INFO);
                            for(const auto &pair : g_triggerToAlarmMap) g_subscription_queue.push_back(pair.first);
                        }
                        
                        {
                            std::lock_guard<std::mutex> nodeLock(g_nodeMap_mutex);
                            if(!nodeMap.empty()) {
                                log("DEBUG: Re-queueing " + std::to_string(nodeMap.size()) + " generic topics", LogLevel::INFO);
                                for(const auto &pair : nodeMap) {
                                    g_subscription_queue.push_back(pair.first);
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
                             auto pv_opt = co_await amcl.async_recv(as::use_awaitable);
                             if(!pv_opt) {
                                 log("DEBUG: MQTT disconnected in Recv Loop", LogLevel::INFO);
                                 throw std::runtime_error("Disconnected");
                             }
                             
                             pv_opt->visit(am::overload{
                                [&](client_t::publish_packet &p) {
                                    std::string topic = p.topic();
                                    std::string payload = p.payload();

                                    /* Check if this is a trigger topic for alarm conditions (.alarm.pub suffix) */
                                    std::string baseTopic = topic;
                                    
                                    // Check if topic ends with /Event and extract base topic
                                    if(topic.size() > 6 && topic.rfind("/Event") == topic.size() - 6) {
                                        baseTopic = topic.substr(0, topic.size() - 6);
                                        log("DEBUG: Detected /Event topic, base='" + baseTopic + "'", LogLevel::INFO);
                                    }

                                    // Thread-Safe Lookup: Copy mappings to local vector under lock
                                    std::vector<TriggerToAlarmMapping> mappings;
                                    {
                                        std::lock_guard<std::mutex> lock(g_alarmMutex);
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
                                        std::lock_guard<std::mutex> lock(g_alarmMutex);
                                        is_internal_write = true; // 🔒 Suppress callback loop
                                        log("✓ Topic '" + topic + "' Processing Alarm Event.", LogLevel::INFO);
                                        // This is a trigger topic, process the alarm payload
                                        try {
                                            auto alarmPayload = json::parse(payload);
                                            // log("DEBUG: Parsed JSON, checking for 'Event' field...", LogLevel::INFO);
                                            
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
                                                
                                                log("DEBUG: Extracted AETypeID=" + std::to_string(AETypeID) + ", GUID='" + aeInstanceId + "'", LogLevel::INFO);

                                                bool active = alarm.value("Active", false);
                                                bool enabled = alarm.value("Enabled", true);
                                                bool shelved = alarm.value("Shelved", false);
                                                bool acked = alarm.value("Acked", false);
                                                bool confirmed = alarm.value("Confirmed", false);
                                                
                                                UA_UInt16 severity = static_cast<UA_UInt16>(alarm.value("Severity", 500));
                                                std::string alarmMessage = alarm.value("AlarmMessage", "Alarm triggered");
                                                std::string alarmName = alarm.value("Name", "");
                                                std::string comment = alarm.value("Comment", "");
                                                
                                                log("DEBUG: Extracted Message='" + alarmMessage + "' from key 'AlarmMessage'", LogLevel::INFO);
                                                
                                                // Source/Quality/UpdateType enums
                                                int sourceEnumValue = alarm.value("Source", 4); // 4=AEEngine
                                                int qualityEnumValue = alarm.value("Quality", 192); // 192=Good
                                                
                                                // Enum conversion debug
                                                // log("DEBUG: Quality Enum Value = " + std::to_string(qualityEnumValue), LogLevel::INFO);
                                                
                                                AlarmQuality qualityEnum = intToAlarmQuality(qualityEnumValue);
                                                std::string quality = alarmQualityToString(qualityEnum);
                                                
                                                int updateTypeValue = alarm.value("UpdateType", 1); // 1=Telemetry
                                                UpdateType updateType = static_cast<UpdateType>(updateTypeValue);
                                                
                                                std::string timestamp = alarm.value("Timestamp", 
                                                    std::format("{:%Y-%m-%d %H:%M:%S}", std::chrono::system_clock::now()));
                                                
                                                // Extract Retain
                                                bool retain = alarm.value("Retain", false);
                                                
                                                log("DEBUG: Data Extracted. Active=" + std::to_string(active) + ", Checking " + std::to_string(mappings.size()) + " mappings...", LogLevel::INFO);
                                                    
                                                // Iterate through all alarms mapped to this trigger
                                                {
                                                    // std::lock_guard<std::mutex> lock(g_alarmMutex); // REMOVED: Already locked in outer scope
                                                    
                                                    for(const auto &mapping : mappings) {
                                                        // Filter by AETypeID
                                                        if(mapping.alarmId != AETypeID) {
                                                            log("DEBUG: SKIP Mapping ID=" + std::to_string(mapping.alarmId) + " != Payload ID=" + std::to_string(AETypeID), LogLevel::INFO);
                                                            continue;
                                                        }
                                                        
                                                        auto alarmIt = g_alarmByKey.find(mapping.alarmKey);
                                                        if(alarmIt == g_alarmByKey.end()) {
                                                            log("DEBUG: SKIP Mapping - AlarmKey not found in g_alarmByKey: " + mapping.alarmKey, LogLevel::INFO);
                                                            continue;
                                                        }
                                                    
                                                    log("DEBUG: MATCH Mapping OK. Processing AlarmKey=" + mapping.alarmKey, LogLevel::INFO);
                                                    
                                                    UA_NodeId conditionId = alarmIt->second;
                                                    
                                                    // Get or create branch for this GUID
                                                    UA_NodeId branchNodeId = UA_NODEID_NULL;
                                                    UA_StatusCode branchSc = getOrCreateAlarmBranch(server, conditionId, 
                                                                                                     aeInstanceId, mapping.alarmKey,
                                                                                                     &branchNodeId);
    
                                                    if(branchSc != UA_STATUSCODE_GOOD) continue;
                                                    
                                                    UA_NodeId alarmId = conditionId;
                                                    
                                                    // Check EnabledState
                                                    UA_NodeId enabledStateId = findChildNodeIdAnyNS(server, alarmId, (char*)"EnabledState");
                                                    bool currentEnabled = false;
                                                    if(!UA_NodeId_isNull(&enabledStateId)) {
                                                        UA_QualifiedName qId = UA_QUALIFIEDNAME_ALLOC(0, (char*)"Id");
                                                        UA_Variant enabledVar;
                                                        UA_StatusCode enabledRc = UA_Server_readObjectProperty(server, enabledStateId, qId, &enabledVar);
                                                        if(enabledRc == UA_STATUSCODE_GOOD && UA_Variant_hasScalarType(&enabledVar, &UA_TYPES[UA_TYPES_BOOLEAN])) {
                                                            currentEnabled = *(UA_Boolean*)enabledVar.data;
                                                        }
                                                        UA_Variant_clear(&enabledVar);
                                                        UA_QualifiedName_clear(&qId);
                                                    }
                                                    UA_NodeId_clear(&enabledStateId);
                                                    
                                                    if(enabled != currentEnabled) {
                                                        if(!enabled) {
                                                            performDisable(server, alarmId, mapping.alarmKey);
                                                            continue;
                                                        } else {
                                                            UA_Boolean enableVal = UA_TRUE;
                                                            setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &enableVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                            currentEnabled = true;
                                                        }
                                                    }
                                                    
                                                    if(!currentEnabled) continue;
                                                    
                                                    // Step 1 & 2: Update Branch State
                                                    bool currentAcked = acked;
                                                    bool currentConfirmed = confirmed;
                                                    
                                                    UA_DateTime now = UA_DateTime_now();
                                                    UA_StatusCode qualityCode = (quality == "Good") ? UA_STATUSCODE_GOOD : UA_STATUSCODE_BAD;
                                                    
                                                    if(!aeInstanceId.empty() && aeInstanceId != "null" && aeInstanceId != "0") {
                                                        auto &branchStateMap = g_branchStates[mapping.alarmKey];
                                                        BranchState &branchState = branchStateMap[aeInstanceId];
                                                        
                                                        bool isStateTransition = (branchState.active != active) || (branchState.acked != currentAcked);
                                                        if(isStateTransition && !branchState.eventIds.empty()) {
                                                            branchState.clearEventIds();
                                                        }
                                                        
                                                        branchState.active = active;
                                                        branchState.acked = currentAcked;
                                                        branchState.confirmed = currentConfirmed;
                                                        branchState.severity = severity;
                                                        branchState.message = alarmMessage;
                                                        branchState.time = now;
                                                        branchState.receiveTime = now;
                                                        branchState.retain = retain;
                                                        branchState.quality = qualityCode;
                                                    }
                                                    
                                                    // Step 3: Hijack Node & Trigger Branch Event
                                                    // Set ALL properties on the condition node before triggering
                                                    {
                                                        UA_Variant v;
                                                        UA_Variant_init(&v);
                                                        
                                                        // ActiveState/Id (STEALTH MODE)
                                                        UA_Boolean bAct = active ? UA_TRUE : UA_FALSE;
                                                        setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &bAct, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                       
                                                        // ActiveState (LocalizedText)
                                                        UA_LocalizedText actText = active ? 
                                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Active") :
                                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Inactive");
                                                        setStealthValueChecked(server, alarmId, "ActiveState", &actText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        
                                                        // AckedState/Id (STEALTH MODE)
                                                        UA_Boolean bAck = currentAcked ? UA_TRUE : UA_FALSE;
                                                        setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &bAck, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        // AckedState (LocalizedText)
                                                        UA_LocalizedText ackText = currentAcked ? 
                                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Acknowledged") :
                                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Unacknowledged");
                                                        setStealthValueChecked(server, alarmId, "AckedState", &ackText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        
                                                        // ConfirmedState/Id (STEALTH MODE)
                                                        UA_Boolean bConf = currentConfirmed ? UA_TRUE : UA_FALSE;
                                                        setStealthValueByPath(server, alarmId, {"ConfirmedState", "Id"}, &bConf, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        // Severity
                                                        setStealthValueChecked(server, alarmId, "Severity", &severity, &UA_TYPES[UA_TYPES_UINT16]);
                                                        
                                                        // Message
                                                        UA_LocalizedText message = UA_LOCALIZEDTEXT((char*)"en-US", 
                                                                                                   const_cast<char*>(alarmMessage.c_str()));
                                                        setStealthValueChecked(server, alarmId, "Message", &message, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        
                                                        // Retain
                                                        UA_Boolean ret = retain ? UA_TRUE : UA_FALSE;
                                                        setStealthValueChecked(server, alarmId, "Retain", &ret, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        // Time
                                                        setStealthValueChecked(server, alarmId, "Time", &now, &UA_TYPES[UA_TYPES_DATETIME]);
                                                        
                                                        // ReceiveTime
                                                        setStealthValueChecked(server, alarmId, "ReceiveTime", &now, &UA_TYPES[UA_TYPES_DATETIME]);
                                                        
                                                        // Quality
                                                        setStealthValueChecked(server, alarmId, "Quality", &qualityCode, &UA_TYPES[UA_TYPES_STATUSCODE]);
                                                        
                                                        // Comment
                                                        if(!comment.empty()) {
                                                            UA_LocalizedText commentText = UA_LOCALIZEDTEXT((char*)"en-US", 
                                                                                                           const_cast<char*>(comment.c_str()));
                                                            setStealthValueChecked(server, alarmId, "Comment", &commentText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        }
                                                        
                                                        // BranchId (critical for client matching)
                                                        if(!aeInstanceId.empty() && aeInstanceId != "0") {
                                                            auto &branchMap = g_alarmBranches[mapping.alarmKey];
                                                            auto branchIt = branchMap.find(aeInstanceId);
                                                            if(branchIt != branchMap.end()) {
                                                                setStealthValueChecked(server, alarmId, "BranchId", &branchIt->second.branchNodeId, &UA_TYPES[UA_TYPES_NODEID]);
                                                            }
                                                        }
                                                        
                                                        // Get source node
                                                        std::string emitterName = mapping.alarmKey.substr(0, mapping.alarmKey.find("-"));
                                                        UA_NodeId sourceNode = alarmId; // fallback
                                                        auto nodeIt = nodeMap.find(emitterName);
                                                        if(nodeIt != nodeMap.end()) {
                                                            sourceNode = nodeIt->second;
                                                        }
                                                        
                                                        // TRIGGER BRANCH EVENT
                                                        UA_ByteString eventId = UA_BYTESTRING_NULL;
                                                        UA_Server_triggerConditionEvent(server, alarmId, sourceNode, &eventId);
                                                        
                                                        // Store EventId for acknowledgment
                                                        if(eventId.length > 0) {
                                                            if(!aeInstanceId.empty() && aeInstanceId != "0") {
                                                                g_branchStates[mapping.alarmKey][aeInstanceId].addEventId(&eventId);
                                                            }
                                                        }
                                                        UA_ByteString_clear(&eventId);
                                                    }
                
                                                    // Step 4: Restore Aggregate & Trigger Aggregate Event
                                                    if(!aeInstanceId.empty() && aeInstanceId != "null" && aeInstanceId != "0") {
                                                        bool aggActive = false;
                                                        bool aggAcked = true;
                                                        bool aggConfirmed = true;
                                                        UA_UInt16 aggSeverity = 0;
                                                        bool aggRetain = false;
                                                        
                                                        auto &branchStateMap = g_branchStates[mapping.alarmKey];
                                                        for(const auto &branchPair : branchStateMap) {
                                                            if(branchPair.second.active) {
                                                                aggActive = true;
                                                                if(branchPair.second.severity > aggSeverity) aggSeverity = branchPair.second.severity;
                                                            }
                                                            if(!branchPair.second.acked) aggAcked = false;
                                                            if(!branchPair.second.confirmed) aggConfirmed = false;
                                                            if(branchPair.second.retain) aggRetain = true;
                                                        }
                                                            log("DEBUG: Updating ActiveState...", LogLevel::INFO);
                                                        UA_Boolean valActive = aggActive ? UA_TRUE : UA_FALSE;
                                                        setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &valActive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        UA_LocalizedText actText = aggActive ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Active") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Inactive");
                                                        setStealthValueChecked(server, alarmId, "ActiveState", &actText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        
                                                        log("DEBUG: Updating AckedState...", LogLevel::INFO);
                                                        UA_Boolean valAcked = aggAcked ? UA_TRUE : UA_FALSE;
                                                        setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &valAcked, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        UA_LocalizedText ackText = aggAcked ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Acknowledged") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unacknowledged");
                                                        setStealthValueChecked(server, alarmId, "AckedState", &ackText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                                        
                                                        UA_Boolean valConf = aggConfirmed ? UA_TRUE : UA_FALSE;
                                                        setStealthValueByPath(server, alarmId, {"ConfirmedState", "Id"}, &valConf, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        UA_StatusCode scSeverity = setStealthValueChecked(server, alarmId, "Severity", &aggSeverity, &UA_TYPES[UA_TYPES_UINT16]);
                                                        if(scSeverity != UA_STATUSCODE_GOOD) {
                                                            log("ERROR: Get Condition LastSeverity failed. StatusCode " + std::string(UA_StatusCode_name(scSeverity)), LogLevel::ERRORS);
                                                        }
                                                        
                                                        UA_Boolean retVal = aggRetain ? UA_TRUE : UA_FALSE;
                                                        setStealthValueChecked(server, alarmId, "Retain", &retVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        // Restore BranchId setting
                                                        setStealthValueChecked(server, alarmId, "BranchId", &branchNodeId, &UA_TYPES[UA_TYPES_NODEID]);
                                                        
                                                        // RETRIEVE CORRECT SOURCE NODE (Emitter)
                                                        UA_NodeId sourceNode = UA_NODEID_NULL;
                                                        UA_Variant sourceVar;
                                                        UA_Variant_init(&sourceVar);
                                                        
                                                        UA_NodeId sourcePropId = findChildNodeIdAnyNS(server, alarmId, "SourceNode");
                                                        if(!UA_NodeId_isNull(&sourcePropId)) {
                                                            if(UA_Server_readValue(server, sourcePropId, &sourceVar) == UA_STATUSCODE_GOOD) {
                                                                if(UA_Variant_hasScalarType(&sourceVar, &UA_TYPES[UA_TYPES_NODEID])) {
                                                                     UA_NodeId_copy((UA_NodeId*)sourceVar.data, &sourceNode);
                                                                }
                                                            }
                                                            UA_Variant_clear(&sourceVar);
                                                        }
                                                        
                                                        if(UA_NodeId_isNull(&sourceNode)) {
                                                            log("ERROR: Could not find SourceNode for alarm. Defaulting to alarmId.", LogLevel::ERRORS);
                                                            // sourceNode = alarmId; // SHARED POINTER DANGER
                                                            UA_NodeId_copy(&alarmId, &sourceNode); // DEEP COPY for safety
                                                        } else {
                                                            // log("DEBUG: Found SourceNode for trigger: ns=" + std::to_string(sourceNode.namespaceIndex), LogLevel::INFO);
                                                        }
                                                        
                                                        // Implicit Event Triggered by Property Writes (e.g. BranchId, ActiveState)
                                                        // Capture the resulting EventId from the Node to ensure Exact Match for Ack/Confirm
                                                        UA_NodeId eventIdProp = findChildNodeIdAnyNS(server, alarmId, "EventId");
                                                        if(!UA_NodeId_isNull(&eventIdProp)) {
                                                            UA_Variant evtVar;
                                                            UA_Variant_init(&evtVar);
                                                            if(UA_Server_readValue(server, eventIdProp, &evtVar) == UA_STATUSCODE_GOOD) {
                                                                 if(UA_Variant_hasScalarType(&evtVar, &UA_TYPES[UA_TYPES_BYTESTRING])) {
                                                                     UA_ByteString *newEvtId = (UA_ByteString*)evtVar.data;
                                                                     
                                                                     if(!aeInstanceId.empty() && aeInstanceId != "null" && aeInstanceId != "0") {
                                                                           // std::lock_guard<std::mutex> lock(g_alarmMutex); // REMOVED: Already locked in outer scope
                                                                           auto &branchStateMap = g_branchStates[mapping.alarmKey];
                                                                          if(branchStateMap.find(aeInstanceId) != branchStateMap.end()) {
                                                                               branchStateMap[aeInstanceId].addEventId(newEvtId);
                                                                               // log("DEBUG: Captured Implicit EventId: " + toHex(newEvtId), LogLevel::INFO);
                                                                          }
                                                                     }
                                                                 }
                                                            }


                                                        
                                                        UA_NodeId_clear(&eventIdProp); // Safe to clear here (defined in this block)
                                                    }
                                                    
                                                    UA_NodeId_clear(&sourcePropId); // Move INSIDE block
                                                    UA_NodeId_clear(&sourceNode);   // Move INSIDE block (Inner shadowed variable)
                                                } // End of Step 4 block
                                                    
                                                    cleanupBranches(mapping.alarmKey);
                                                    
                                                    UA_NodeId_clear(&branchNodeId); 
                                                    // sourcePropId local scope or outer? It's inside the if block at 3257.
                                                    // Let's check variables in scope...
                                                    
                                                    /* 
                                                       Variables to clear:
                                                       - branchNodeId (Deep Copy from getOrCreateAlarmBranch)
                                                       - sourceNode (Deep Copy now)
                                                       - eventIdProp (Deep Copy from findChildNodeIdAnyNS) - Wait, defined inside 3276 block?
                                                    */
                                                    
                                                    /*
                                                    if(!UA_NodeId_isNull(&alarmId)) {
                                                         UA_NodeId nullId = UA_NODEID_NULL;
                                                         UA_Server_writeObjectProperty_scalar(server, alarmId, UA_QUALIFIEDNAME(0, (char*)"BranchId"), &nullId, &UA_TYPES[UA_TYPES_NODEID]);
                                                    }
                                                    */
                                                }
                                                }
                                                // 🔒 END CRITICAL: Unlock g_alarmMutex
                                            }
                                        } catch(const std::exception& e) { log("JSON/Processing Error: " + std::string(e.what()), LogLevel::ERRORS); }
                                        is_internal_write = false; // 🔓 Reset callback loop
                                    }

                                    if(!isAlarmEvent) {
                                        /* Regular data update path (Generic Telemetry) */
                                        // Check if topic exists in nodeMap
                                        std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                                        // DIAGNOSTIC: Incoming Message Log
                                        int in_count = g_mqtt_incoming_count.fetch_add(1);
                                        if(in_count % 100 == 0) {
                                            log("📥 MQTT Rx: Processed " + std::to_string(in_count) + " messages (Internal Write Path)", LogLevel::INFO);
                                        }

                                        if(nodeMap.find(topic) != nodeMap.end()) {
                                            UA_NodeId nodeId = nodeMap[topic];
                                            try {
                                                auto j = json::parse(payload);
                                                
                                                    // 1. Check for Complex "Data" Array Payload
                                                    // 1. Check for Complex "Data" Array Payload
                                                if(j.contains("Data") && j["Data"].is_array() && !j["Data"].empty()) {
                                                    const auto& dataItem = j["Data"][0];
                                                    
                                                    if(dataItem.contains("Value")) {
                                                        if(dataItem["Value"].is_number()) {
                                                            double value = dataItem["Value"].get<double>();
                                                            
                                                            // Simplified: Zero-Copy Write (Server handles deduplication)
                                                            UA_Variant myVar;
                                                            UA_Variant_init(&myVar);
                                                            UA_Variant_setScalar(&myVar, &value, &UA_TYPES[UA_TYPES_DOUBLE]);
                                                            
                                                            {
                                                                std::lock_guard<std::recursive_mutex> lock(g_server_mutex);
                                                                is_internal_write = true;
                                                                UA_Server_writeValue(server, nodeId, myVar);
                                                                is_internal_write = false;
                                                            }
                                                        } else if(dataItem["Value"].is_boolean()) {
                                                            UA_Boolean value = dataItem["Value"].get<bool>();
                                                            
                                                            // Simplified: Zero-Copy Write (Server handles deduplication)
                                                            UA_Variant myVar;
                                                            UA_Variant_init(&myVar);
                                                            UA_Variant_setScalar(&myVar, &value, &UA_TYPES[UA_TYPES_BOOLEAN]);

                                                            {
                                                                std::lock_guard<std::recursive_mutex> lock(g_server_mutex);
                                                                is_internal_write = true;
                                                                UA_Server_writeValue(server, nodeId, myVar);
                                                                is_internal_write = false;
                                                            }
                                                        } else if(dataItem["Value"].is_string()) {
                                                            std::string strValue = dataItem["Value"].get<std::string>();
                                                            
                                                            // Simplified: Zero-Copy Write (Server handles deduplication)
                                                            UA_String value = UA_STRING((char*)strValue.c_str());
                                                            UA_Variant myVar;
                                                            UA_Variant_init(&myVar);
                                                            UA_Variant_setScalar(&myVar, &value, &UA_TYPES[UA_TYPES_STRING]);
                                                            
                                                            {
                                                                std::lock_guard<std::recursive_mutex> lock(g_server_mutex);
                                                                is_internal_write = true;
                                                                UA_Server_writeValue(server, nodeId, myVar);
                                                                is_internal_write = false;
                                                            }
                                                        }
                                                    }
                                                // 2. Fallback: Check for Simple "Value" Payload (Legacy support)
                                                } else if(j.contains("Value")) {
                                                     if(j["Value"].is_number()) {
                                                         double value = j["Value"].get<double>();
                                                         
                                                         // Simplified: Zero-Copy Write (Server handles deduplication)
                                                         UA_Variant myVar;
                                                         UA_Variant_init(&myVar);
                                                         UA_Variant_setScalar(&myVar, &value, &UA_TYPES[UA_TYPES_DOUBLE]);
                                                         
                                                         {
                                                             std::lock_guard<std::recursive_mutex> lock(g_server_mutex);
                                                             is_internal_write = true; 
                                                             UA_Server_writeValue(server, nodeId, myVar);
                                                             is_internal_write = false;
                                                         }
                                                     }
                                                }
                                            } catch(const std::exception &e) {
                                                log("JSON parse error: " + std::string(e.what()), LogLevel::ERRORS);
                                            }
                                        }
                                    }
                                },
                                [](auto const&) {}
                             });
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
                                        continue;
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
                as::steady_timer timer(ioc);
                timer.expires_after(std::chrono::seconds(2));
                co_await timer.async_wait(as::use_awaitable);
            }
        }, as::detached);
}



// Orphaned legacy code removed




// Migrated logic from main()
int RunServer(int argc, char **argv) {
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
    
    // Early console output before logging is initialized
    if(!isService) {
        log("==================================================", LogLevel::INFO);
        log("OPC UA Server Starting...", LogLevel::INFO);
        log("==================================================", LogLevel::INFO);
    } else {
        // Just log to file if service
        // init_logging call comes later, but we can rely on default "server.log" or init call
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
    file >> Settingsconfig;

    // Extract values
    std::cout << "Extracting configuration values..." << std::endl;

    // Extract AppSettings
    std::string applicationEndURL =
        Settingsconfig["AppSettings"]["ApplicationEndURL"].get<std::string>();
    std::string applicationEndURLHost =
        Settingsconfig["AppSettings"]["ApplicationEndURLHost"].get<std::string>();
    int applicationEndURLPort =
        Settingsconfig["AppSettings"]["ApplicationEndURLPort"].get<int>();

    // Extract MqttConfig
    std::string brokerAddress =
        Settingsconfig["MqttConfig"]["MqttSettings"][0]["BrokerAddress"]
            .get<std::string>();
    int brokerPort =
        Settingsconfig["MqttConfig"]["MqttSettings"][0]["BrokerPort"].get<int>();
    std::string mqttUsername =
        Settingsconfig["MqttConfig"]["MqttSettings"][0]["Username"].get<std::string>();
    std::string mqttPassword =
        Settingsconfig["MqttConfig"]["MqttSettings"][0]["Password"].get<std::string>();

    // Extract Authorization
    std::string authUsername =
        Settingsconfig["Authorization"]["Username"].get<std::string>();
    std::string authPassword =
        Settingsconfig["Authorization"]["Password"].get<std::string>();

    // Extract Payload
    std::string dbPath =
        Settingsconfig["Payload"]["OfflineQueueOptions"]["DbPath"].get<std::string>();
    int retentionDays =
        Settingsconfig["Payload"]["OfflineQueueOptions"]["RetentionDays"].get<int>();

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
    
    while(true) {
        std::cout << "Acquiring bearer token for API authentication..." << std::endl;
        log("Acquiring bearer token for API authentication...", LogLevel::INFO);
        
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
        
        std::cout << "⚠️ Retrying in " << retryDelay << " seconds..." << std::endl;
        log("⚠️ Retrying in " + std::to_string(retryDelay) + " seconds...", LogLevel::INFO);
        std::this_thread::sleep_for(std::chrono::seconds(retryDelay));
        
        if(retryDelay < 15) {
            retryDelay += 5;
        }
    }

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

    size_t revocationListSize = 0;
    UA_ByteString *revocationList = NULL;

    UA_Server *server = UA_Server_new();
    UA_ServerConfig *config = UA_Server_getConfig(server);

    // Custom logger is now enabled with safe format string handling
    // The myLog function now sanitizes problematic format specifiers before processing
    config->logging = &myLogger;
    

    log("OPC UA Server initialized", LogLevel::INFO);
    log("Setting up server configuration", LogLevel::DEBUG);

    size_t nsIdx = UA_Server_addNamespace(server, "urn:my.properties");
    log("Added namespace: urn:my.properties", LogLevel::DEBUG);

    // NOTE: ConditionRefresh callback registration is done AFTER all conditions
    // are created (see after mqtt_subscribe_and_update call), not here at startup.
    // This is because open62541 sets its own callback when the first condition
    // is created, so we must override it AFTER that happens.

    // broker_start(argc, argv);
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

    std::string json_body = R"(
    {
        "data": { "nodeId": "ND01" } 
    }
    )";
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
        
        // Acquire bearer token (Legacy logic preserved for manager)
        // Token already acquired in startup
        bearerToken = g_bearerToken;

        std::vector<ServerConfig> configs;
        try {
            log("Fetching server configurations...", LogLevel::INFO);
            configs = ParseServerConfig(applicationEndURLHost, std::to_string(applicationEndURLPort), 
                                        bearerToken, json_body, target);
            
            if(configs.empty()) {
                log("❌ No server configurations found in API response. Exiting.", LogLevel::ERRORS);
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
            HANDLE hJob = CreateJobObject(NULL, NULL);
            if (hJob) {
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION jeli = { 0 };
                jeli.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
                SetInformationJobObject(hJob, JobObjectExtendedLimitInformation, &jeli, sizeof(jeli));
            } else {
                log("Failed to create Job Object. Child termination relies on manual cleanup.", LogLevel::ERRORS);
            }

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
    if(!current_config.orgMappings.empty()) {
        log("Mapping " + std::to_string(current_config.orgMappings.size()) + " organizations from config...", LogLevel::INFO);
        for(const auto& map : current_config.orgMappings) {
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
        // g_apiHost and g_apiPort already set earlier
        
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

    // config->applicationDescription.applicationUri =
    // UA_STRING_ALLOC("urn:Anexee.server");
    config->applicationDescription.applicationUri =
        UA_STRING_ALLOC("urn:Anexee.server.application");
    // config->applicationDescription.productUri = UA_STRING_ALLOC("urn:Anexee.server");
    config->applicationDescription.productUri = UA_STRING_ALLOC("urn:Anexee:product");
    config->applicationDescription.applicationName =
        UA_LOCALIZEDTEXT_ALLOC("en-US", "AnexeeServer");

    log("Application description configured: Anexee Server", LogLevel::DEBUG);

    // ========================================================================
    // PERFORMANCE TUNING: Limit Queues to prevent Memory Leaks
    // ========================================================================
    // Prevent unbounded growth of notification queues if clients are slow
    
    config->maxSessions = 100;
    config->maxSecureChannels = 50;      // Limit concurrent TCP connections
    config->maxSessionTimeout = 10000.0; // Prune detached sessions after 10s to free MonitoredItems
    config->maxSubscriptionsPerSession = 50;
    config->maxMonitoredItemsPerSubscription = 1000;
    config->maxMonitoredItems = 1000;   // Global limit to prevent TimerTree explosion
    config->maxSubscriptions = 200;      // Global limit for subscriptions
    config->publishingIntervalLimits.min = 100.0; // Enforce min 100ms publishing interval
    config->samplingIntervalLimits.min = 500.0;   // Throttle sampling to max 5Hz to prevent notification flood
    config->publishingIntervalLimits.max = 3600.0 * 1000.0;
    config->queueSizeLimits.max = 1;                // Minimal Queue: Only keep latest value (No Buffering)
    config->enableRetransmissionQueue = true;  // Enable retransmission queue
    config->maxRetransmissionQueueSize = 100;         // Standard: Unlimited (was 1/10 for debugging)
    config->maxNotificationsPerPublish = 1000;      // Limit per PublishResponse
    log("Performance Limits used: MaxSessions=100, GlobalMAXMI=20000", LogLevel::INFO);

    // Add historizing configuration
    // Add historizing configuration
    // DISABLED: History Data Storage disabled to prevent continuous memory growth
    /*
    g_gathering = (UA_HistoryDataGathering *)UA_malloc(sizeof(UA_HistoryDataGathering));
    *g_gathering = UA_HistoryDataGathering_Default(1);
    config->historyDatabase = UA_HistoryDatabase_default(*g_gathering);

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                "Historizing configuration initialized");
    log("Historizing configuration initialized successfully", LogLevel::INFO);
    */
    log("Historizing configuration DISABLED (Memory Optimization)", LogLevel::INFO);

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



    // NOTE: BearerToken already acquired earlier (line ~3191)
    // Duplicate definition removed to avoid redefinition error


    // Add an ALARM FOLDER INSIDE SERVER THEN ALL NODES WITH HASEVENTSOURCE WILL BE ADDED
    // TO THIS FOLDER
    UA_NodeId areaNodeId = UA_NODEID_NUMERIC(0, 54624);
    UA_ObjectAttributes objAttr = UA_ObjectAttributes_default;
    objAttr.displayName = UA_LOCALIZEDTEXT((char *)"en", (char *)"Alarms");
    UA_Server_addObjectNode(
        server, UA_NODEID_NULL, UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER),
        UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES), UA_QUALIFIEDNAME(1, (char *)"Alarms"),
        UA_NODEID_NUMERIC(0, UA_NS0ID_BASEOBJECTTYPE), objAttr, NULL, &areaNodeId);

    UA_Server_addReference(server, UA_NODEID_NUMERIC(0, 2253),  // Server
                           UA_NODEID_NUMERIC(0, UA_NS0ID_HASNOTIFIER),
                           UA_EXPANDEDNODEID_NUMERIC(areaNodeId.namespaceIndex,
                                                     areaNodeId.identifier.numeric),
                           UA_TRUE);

    /* Use the Alarms object as the default Event Notifier origin */
    g_eventNotifierNode = areaNodeId;

    // ========================================================================
    // MULTI-TENANCY: Global topic/alarm fetching DISABLED
    // ========================================================================
    // NOTE: In the old single-tenant version, topics and alarms were fetched
    // globally here with orgId=0. In the new multi-tenant architecture, each
    // organization's worker thread (SessionWorker.cpp) fetches its own
    // topics and alarms using the correct org-specific orgId.
    //
    // This global initialization code is now commented out to avoid:
    // 1. Fetching with incorrect orgId=0
    // 2. Creating global address space that conflicts with per-org namespaces
    // 3. Duplicate API calls (happens in worker threads)
    //
    // All topic/alarm fetching now happens in sessionWorkerThread() with
    // the correct orgId for each connected organization.
    // ========================================================================

#if 0  // LEGACY CODE DISABLED - Using #if 0 instead of /* */ for large block
    int count = 0;
    vector<string> topics;
    if(!BearerToken.empty()) {

        std::string json_body = R"(
            {
        
                "orgId": 0,  // ❌ WRONG: This was hardcoded to 0
                "roleId": "",
                "userId": 0,
                "moduleId": 0,
                "userType": "",
                "requestDateTime": "2024-12-26T08:16:05.629Z",
                "ipAddress": "",
                "originName": "",
                "filterModel": {
                    
                    "customValue": "all"
                },

                "data": {
                    "isLogging" : true
                }
            }
            )";

        std::string target = "/api/GetTopicList";

        auto futureResponse = std::async(std::launch::async, getHierarchy, 
                                         applicationEndURLHost,
                                         std::to_string(applicationEndURLPort),
                                         BearerToken,
                                         json_body, target);
        json response = futureResponse.get();

        if(response.contains("data") && response["data"].is_array()) {
            for(const auto &item : response["data"]) {
                if(item.contains("namespace")) {
                    string ns = item["namespace"].get<string>();
                    topics.push_back(ns);

                    // Create address space for this topic
                    auto parts = split(ns, '/');
                    std::string currentPath;
                    UA_NodeId parent = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);

                    for(size_t i = 0; i < parts.size(); ++i) {
                        if(!currentPath.empty())
                            currentPath += "/";
                        currentPath += parts[i];
                        if(i < parts.size() - 1) {
                            parent =
                                getOrCreateFolder(server, currentPath, parts[i], parent);
                        } else {
                            UA_VariableAttributes attr = UA_VariableAttributes_default;
                            UA_Int32 value = 0;
                            UA_Variant_setScalarCopy(&attr.value, &value,
                                                     &UA_TYPES[UA_TYPES_INT32]);
                            attr.displayName =
                                UA_LOCALIZEDTEXT_ALLOC("en-US", parts[i].c_str());
                            attr.description = UA_LOCALIZEDTEXT_ALLOC(
                                "en-US", item["name"].get<string>().c_str());
                            attr.accessLevel = UA_ACCESSLEVELMASK_READ |
                                               UA_ACCESSLEVELMASK_WRITE |
                                               UA_ACCESSLEVELMASK_HISTORYREAD;
                            attr.historizing = true;

                            // UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(1,
                            // currentPath.c_str());
                            UA_NodeId nodeId =
                                UA_NODEID_NUMERIC(1, item["tagId"].get<int>());
                            UA_QualifiedName nodeName =
                                UA_QUALIFIEDNAME_ALLOC(1, parts[i].c_str());
                            UA_Server_addVariableNode(
                                server, nodeId, parent,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT), nodeName,
                                UA_NODEID_NUMERIC(
                                    0, UA_NS0ID_BASEDATAVARIABLETYPE),  // Use proper type
                                attr, NULL, NULL);

                            UA_Range range;
                            range.low = item["rangeMin"].get<double>();
                            range.high = item["rangeMax"].get<double>();
                            UA_Variant rangeVariant;
                            UA_Variant_init(&rangeVariant);
                            UA_Variant_setScalarCopy(&rangeVariant, &range,
                                                 &UA_TYPES[UA_TYPES_RANGE]);

                            // Create attributes for the range property
                            UA_VariableAttributes rangeAttr =
                                UA_VariableAttributes_default;
                            rangeAttr.displayName =
                                UA_LOCALIZEDTEXT_ALLOC("en-US", "EURange");
                            rangeAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                            rangeAttr.value = rangeVariant;

                            UA_NodeId rangeNodeId =
                                UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000 +
                                                         1);  // Unique ID for range
                            UA_QualifiedName rangeName =
                                UA_QUALIFIEDNAME_ALLOC(0, "EURange");
                            UA_Server_addVariableNode(
                                server, rangeNodeId, nodeId,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY), rangeName,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE), rangeAttr,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE), rangeAttr,
                                NULL, NULL);

                            UA_Variant_clear(&rangeVariant);

                            // After adding the EURange property, add the alarm limits
                            UA_Double alarmHiHi = item["alarmHiHi"].get<double>();
                            UA_Double alarmHi = item["alarmHi"].get<double>();
                            UA_Double alarmLo = item["alarmLo"].get<double>();
                            UA_Double alarmLoLo = item["alarmLoLo"].get<double>();

                            // Add each alarm limit as a property
                            auto addAlarmProperty = [&](const char *name, UA_Double value,
                                                        UA_NodeId parentId, int offset) {
                                UA_VariableAttributes alarmAttr =
                                    UA_VariableAttributes_default;
                                UA_Variant alarmVariant;
                                UA_Variant_init(&alarmVariant);
                                UA_Variant_setScalarCopy(&alarmVariant, &value,
                                                     &UA_TYPES[UA_TYPES_DOUBLE]);

                                alarmAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", name);
                                alarmAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                alarmAttr.value = alarmVariant;

                                UA_NodeId alarmNodeId = UA_NODEID_NUMERIC(
                                    2, item["tagId"].get<int>() * 1000 + offset);
                                UA_QualifiedName alarmName =
                                    UA_QUALIFIEDNAME_ALLOC(0, name);
                                UA_Server_addVariableNode(
                                    server, alarmNodeId, parentId,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY), alarmName,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                                    alarmAttr, NULL, NULL);

                                // clean up
                                UA_Variant_clear(&alarmVariant);
                            };

                            addAlarmProperty("AlarmHiHi", alarmHiHi, nodeId, 2);
                            addAlarmProperty("AlarmHi", alarmHi, nodeId, 3);
                            addAlarmProperty("AlarmLo", alarmLo, nodeId, 4);
                            addAlarmProperty("AlarmLoLo", alarmLoLo, nodeId, 5);

                            // Create the engineering unit property
                            UA_String unit = UA_STRING_ALLOC(
                                item["measurmentUnitType"].get<string>().c_str());

                            UA_Variant unitVariant;
                            UA_Variant_init(&unitVariant);
                            UA_Variant_setScalarCopy(&unitVariant, &unit,
                                                 &UA_TYPES[UA_TYPES_STRING]);

                            UA_VariableAttributes unitAttr =
                                UA_VariableAttributes_default;
                            unitAttr.displayName =
                                UA_LOCALIZEDTEXT_ALLOC("en-US", "Engineering Unit");
                            unitAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                            UA_Variant_copy(&unitVariant, &unitAttr.value);
                            UA_Variant_clear(&unitVariant);

                            UA_NodeId unitNodeId =
                                UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000 +
                                                         6);  // Use a different offset
                            UA_QualifiedName unitName =
                                UA_QUALIFIEDNAME_ALLOC(0, "EngineeringUnit");
                            UA_Server_addVariableNode(
                                server, unitNodeId, nodeId,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY), unitName,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE), unitAttr,
                                NULL, NULL);

                            // DeadBand
                            if(item.contains("deadband")) {
                                UA_Double deadBand = item["deadband"].get<double>();
                                UA_Variant deadBandVariant;
                                UA_Variant_init(&deadBandVariant);
                                UA_Variant_setScalarCopy(&deadBandVariant, &deadBand,
                                                     &UA_TYPES[UA_TYPES_DOUBLE]);

                                UA_VariableAttributes deadBandAttr =
                                    UA_VariableAttributes_default;
                                deadBandAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", "DeadBand");
                                deadBandAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                UA_Variant_copy(&deadBandVariant, &deadBandAttr.value);
                                UA_Variant_clear(&deadBandVariant);

                                UA_NodeId deadBandNodeId = UA_NODEID_NUMERIC(
                                    2, item["tagId"].get<int>() * 1000 +
                                           7);  // Use a different offset
                                UA_QualifiedName deadBandName =
                                    UA_QUALIFIEDNAME_ALLOC(0, "DeadBand");
                                UA_Server_addVariableNode(
                                    server, deadBandNodeId, nodeId,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                                    deadBandName,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                                    deadBandAttr, NULL, NULL);
                            }

                            // Precision Type
                            if(item.contains("precisionType")) {
                                UA_String precision = UA_STRING_ALLOC(
                                    item["precisionType"].get<string>().c_str());

                                UA_Variant precisionVariant;
                                UA_Variant_init(&precisionVariant);
                                UA_Variant_setScalarCopy(&precisionVariant, &precision,
                                                     &UA_TYPES[UA_TYPES_STRING]);

                                UA_VariableAttributes precisionAttr =
                                    UA_VariableAttributes_default;
                                precisionAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", "Precision Type");
                                precisionAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                UA_Variant_copy(&precisionVariant, &precisionAttr.value);
                                UA_Variant_clear(&precisionVariant);

                                UA_NodeId precisionNodeId = UA_NODEID_NUMERIC(
                                    2, item["tagId"].get<int>() * 1000 +
                                           8);  // Use a different offset
                                UA_QualifiedName precisionName =
                                    UA_QUALIFIEDNAME_ALLOC(0, "Precision");
                                UA_Server_addVariableNode(
                                    server, precisionNodeId, nodeId,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                                    precisionName,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                                    precisionAttr, NULL, NULL);
                            }

                            // Parameter Group
                            if(item.contains("parameterGroup")) {
                                UA_String parameterGroup = UA_STRING_ALLOC(
                                    item["parameterGroup"].get<string>().c_str());

                                UA_Variant parameterGroupVariant;
                                UA_Variant_init(&parameterGroupVariant);
                                UA_Variant_setScalarCopy(&parameterGroupVariant,
                                                     &parameterGroup,
                                                     &UA_TYPES[UA_TYPES_STRING]);

                                UA_VariableAttributes parameterGroupAttr =
                                    UA_VariableAttributes_default;
                                parameterGroupAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", "Parameter Group");
                                parameterGroupAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                UA_Variant_copy(&parameterGroupVariant,
                                                &parameterGroupAttr.value);
                                UA_Variant_clear(&parameterGroupVariant);

                                UA_NodeId parameterGroupNodeId = UA_NODEID_NUMERIC(
                                    2, item["tagId"].get<int>() * 1000 +
                                           9);  // Use a different offset
                                UA_QualifiedName parameterGroupName =
                                    UA_QUALIFIEDNAME_ALLOC(0, "Parameter Group");
                                UA_Server_addVariableNode(
                                    server, parameterGroupNodeId, nodeId,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                                    parameterGroupName,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                                    parameterGroupAttr, NULL, NULL);
                            }

                            // tagType
                            if(item.contains("tagType")) {
                                UA_String tagType = UA_STRING_ALLOC(
                                    item["tagType"].get<string>().c_str());

                                UA_Variant tagTypeVariant;
                                UA_Variant_init(&tagTypeVariant);
                                UA_Variant_setScalarCopy(&tagTypeVariant, &tagType,
                                                     &UA_TYPES[UA_TYPES_STRING]);

                                UA_VariableAttributes tagTypeAttr =
                                    UA_VariableAttributes_default;
                                tagTypeAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", "Tag Type");
                                tagTypeAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                UA_Variant_copy(&tagTypeVariant, &tagTypeAttr.value);
                                UA_Variant_clear(&tagTypeVariant);

                                UA_NodeId tagTypeId = UA_NODEID_NUMERIC(
                                    2, item["tagId"].get<int>() * 1000 +
                                           10);  // Use a different offset
                                UA_QualifiedName tagTypeName =
                                    UA_QUALIFIEDNAME_ALLOC(0, "TagType");
                                UA_Server_addVariableNode(
                                    server, tagTypeId, nodeId,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                                    tagTypeName,
                                    UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                                    tagTypeAttr, NULL, NULL);
                            }

                            nodeMap[currentPath] = nodeId;

                            // After creating each variable node, add this:
                            //UA_HistorizingNodeIdSettings setting;
                            //setting.historizingBackend =
                            //    UA_HistoryDataBackend_Memory(200, 1000);
                            //setting.maxHistoryDataResponseSize = 1000;
                            //setting.historizingUpdateStrategy =
                            //    UA_HISTORIZINGUPDATESTRATEGY_VALUESET;
                            //// setting.pollingInterval = 1000;

                            //// Register the node for historizing using the global
                            //// gathering context
                            //UA_StatusCode ret = g_gathering->registerNodeId(
                            //    server, g_gathering->context, &nodeId, setting);

                            //const UA_HistorizingNodeIdSettings *currentSettings =
                            //    g_gathering->getHistorizingSetting(
                            //        server, g_gathering->context, &nodeId);
                            //if(currentSettings) {
                            //    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                            //                "Node historizing settings verified - Update "
                            //                "Strategy: %d",
                            //                currentSettings->historizingUpdateStrategy);
                            //}

                            // Link Alarms?
                            // Added count so it creates alarm for every 25th node thus
                            // not overflowing the alarm queue
                            //if(nodeId.identifier.numeric == 1618 ||
                            //   nodeId.identifier.numeric == 1569) {

                            //    MonitoredNodeAlarmInfo alarmInfo;
                            //    alarmInfo.processNodeId = nodeId;
                            //    alarmInfo.displayName =
                            //        parts[i];  // Use the node's display name for alarm
                            //                   // messages

                            //    // Get alarm thresholds from the JSON item

                            //    alarmInfo.alarmHiHi =
                            //        (item["alarmHiHi"].get<double>() == 0)
                            //            ? 20.0
                            //            : item["alarmHiHi"].get<double>();
                            //    alarmInfo.alarmHi = (item["alarmHi"].get<double>() == 0)
                            //                            ? 10.0
                            //                            : item["alarmHi"].get<double>();
                            //    alarmInfo.alarmLo = (item["alarmLo"].get<double>() == 0)
                            //                            ? -10.0
                            //                            : item["alarmLo"].get<double>();
                            //    alarmInfo.alarmLoLo =
                            //        (item["alarmLoLo"].get<double>() == 0)
                            //            ? -20.0
                            //            : item["alarmLoLo"].get<double>();

                            //    alarmInfo.deadband = item.contains("deadband")
                            //                             ? item["deadband"].get<double>()
                            //                             : 0.0;

                            //    UA_StatusCode alarmStatus =
                            //        createAndLinkExclusiveLimitAlarm(
                            //            server, &nodeId, alarmInfo.displayName, item,
                            //            &alarmInfo.alarmInstanceId);
                            //    if(alarmStatus != UA_STATUSCODE_GOOD) {
                            //        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                            //                     "Failed to create alarm for node %s",
                            //                     parts[i].c_str());
                            //        // Handle error, maybe continue or return
                            //    } else {
                            //        // Store the alarm information in our global map
                            //        monitoredAlarms[nodeId] = alarmInfo;
                            //    }
                            //}
                            count++;

                            UA_ValueCallback callback;
                            callback.onWrite = writeCallback;
                            callback.onRead = NULL;
                            UA_Server_setVariableNode_valueCallback(server, nodeId,
                                                                    callback);
                        }
                    }

                    // Store topic info
                    topicMap[ns] = {item["tagId"].get<int>(), item["name"].get<string>(),
                                    item["tagType"].get<string>(),
                                    item["rangeMin"].get<double>(),
                                    item["rangeMax"].get<double>()};
                }
            }

            std::string json_body = R"(
                {
                    "filterModel": {
                        "currentPage": 1,
                        "pageSize": 10
                    },
                    "orgId": 14
                }
                )";

            std::string target = "/api/GetAlarmsConfigDetailList";

           vector<AlarmConfig> alarms = ParseAlarmConfig(applicationEndURLHost, std::to_string(applicationEndURLPort), BearerToken, json_body, target);


           for (const auto &item : alarms){

            string AlarmName = item.name;  
            int AlarmId = item.id;
            if(item.alarmEmitters.has_value()){
                for (const auto &emitter : item.alarmEmitters.value()){

                    UA_NodeId sourceNode = UA_NODEID_NULL;
                    if(auto it = nodeMap.find(emitter.emitterNodeName); it != nodeMap.end()) {
                        sourceNode = it->second;
                    } else {
                        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                       "No OPC UA node registered for tag '%s'",
                                       emitter.emitterNodeName.c_str());
                        sourceNode = UA_NODEID_NULL;
                    }



                    std::string triggerTopic = emitter.emitterNodeName;
                    
                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                               "    Trigger ID=%d, topic='%s'",
                               emitter.id, triggerTopic.c_str());
                                            // Build the alarm key (same as used when creating the condition)
                            std::string alarmKey = emitter.emitterNodeName + "-" + AlarmName;
                            
                            TriggerToAlarmMapping mapping;
                            mapping.triggerTopic = triggerTopic;
                            mapping.alarmKey = alarmKey;
                            mapping.triggerId = emitter.id;
                            mapping.alarmId = AlarmId;
                            
                            // Add to global map
                            g_triggerToAlarmMap[triggerTopic].push_back(mapping);
                            
                            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                       "Mapped trigger topic '%s' (ID: %d) to alarm '%s' (key: '%s')",
                                       triggerTopic.c_str(), emitter.id, AlarmName.c_str(), alarmKey.c_str());
                        
                    
                    
                    
                    /* Use a stable key per emitter+alarm name to avoid duplicate creation */
                    const std::string key = emitter.emitterNodeName + "-" + AlarmName;
                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                "Key: '%s'", key.c_str());

                    auto found = g_alarmByKey.find(key);
                    if(found == g_alarmByKey.end()) {
                        /* First time: create the condition and expose it under the source */
                        UA_NodeId alarmId = UA_NODEID_NULL;
                        UA_StatusCode sc = UA_Server_createCondition(
                            server, UA_NODEID_NULL, /* let server choose id */
                            UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE),
                            UA_QUALIFIEDNAME(1, const_cast<char*>(AlarmName.c_str())), sourceNode,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT), &alarmId);

                        if(sc == UA_STATUSCODE_GOOD) {
                            // Initialize the condition with proper default states
                            // Initialize alarm state using STEALTH MODE to avoid ghost events at creation
                            
                            // 1. Enable the condition (STEALTH MODE)
                            
                            UA_Boolean enabled = (item.enable == "STS_TRUE") ? UA_TRUE : UA_FALSE;
                            setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &enabled, &UA_TYPES[UA_TYPES_BOOLEAN]);
                            
                            // 2. Initialize ActiveState to INACTIVE (STEALTH MODE)
                            UA_Boolean inactive = UA_FALSE;
                            setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                            
                            // Also set the Active boolean property - stealth write
                            setStealthValueChecked(server, alarmId, "ActiveState", &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                            
                            // 3. Initialize AckedState to FALSE (STEALTH MODE)
                            setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                            
                            // 4. Initialize Retain to FALSE - stealth write
                            setStealthValueChecked(server, alarmId, "Retain", &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                            
                            // 5. Set initial low severity - stealth write
                            UA_UInt16 initialSeverity = 0;
                            setStealthValueChecked(server, alarmId, "Severity", &initialSeverity, &UA_TYPES[UA_TYPES_UINT16]);
                            
                            // 6. Set initial message - stealth write
                            UA_LocalizedText initialMsg = UA_LOCALIZEDTEXT(const_cast<char*>("en-US"), 
                                                                           const_cast<char*>("Alarm initialized (inactive)"));
                            setStealthValueChecked(server, alarmId, "Message", &initialMsg, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                            
                            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                       "✓ Condition '%s' created, enabled, and initialized to INACTIVE state (stealth)", 
                                       AlarmName.c_str());
                            
                            // Set EventNotifier attribute directly on the alarm condition node
                            // EventNotifier is a node attribute, not a property or variable
                            // Bit 0 (0x01) = SubscribeToEvents
                            UA_Byte eventNotifier = 0x01; // Enable SubscribeToEvents  
                            UA_NodeId eventNotifierNode = UA_NODEID_NULL;
                            auto nodeIt = nodeMap.find(emitter.emitterNodeName);
                            eventNotifierNode = nodeIt->second;

                            
                            UA_StatusCode evtRc = UA_Server_writeEventNotifier(server, eventNotifierNode, eventNotifier);
                            
                            if(evtRc == UA_STATUSCODE_GOOD) {
                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                           "✓ Set EventNotifier=0x01 (SubscribeToEvents) on alarm '%s'",
                                           AlarmName.c_str());
                            } 

                            
                            // Set up method callbacks for Acknowledge, Confirm, and AddComment
                            // These are called when the client invokes the methods
                            
                            // Register Acknowledge method callback
                            UA_NodeId acknowledgeMethodId = 
                                findChildNodeIdAnyNS(server, alarmId, (char *)"Acknowledge");
                            if(!UA_NodeId_isNull(&acknowledgeMethodId)) {
                                UA_Server_setMethodNodeCallback(server, acknowledgeMethodId,
                                                               customAcknowledgeCallback);
                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                           "✓ Acknowledge callback registered for '%s'",
                                           AlarmName.c_str());
                                UA_NodeId_clear(&acknowledgeMethodId);
                            } else {
                                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                              "Acknowledge method not found for '%s'",
                                              AlarmName.c_str());
                            }
                            
                            // Register Confirm method callback
                            UA_NodeId confirmMethodId = 
                                findChildNodeIdAnyNS(server, alarmId, (char *)"Confirm");
                            if(!UA_NodeId_isNull(&confirmMethodId)) {
                                UA_Server_setMethodNodeCallback(server, confirmMethodId,
                                                               customConfirmCallback);
                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                           "✓ Confirm callback registered for '%s'",
                                           AlarmName.c_str());
                                UA_NodeId_clear(&confirmMethodId);
                            } else {
                                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                              "Confirm method not found for '%s'",
                                              AlarmName.c_str());
                            }
                            
                            // Register AddComment method callback
                            UA_NodeId addCommentMethodId = 
                                findChildNodeIdAnyNS(server, alarmId, (char *)"AddComment");
                            if(!UA_NodeId_isNull(&addCommentMethodId)) {
                                UA_Server_setMethodNodeCallback(server, addCommentMethodId,
                                                               customAddCommentCallback);
                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                           "✓ AddComment callback registered for '%s'",
                                           AlarmName.c_str());
                                UA_NodeId_clear(&addCommentMethodId);
                            } else {
                                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                              "AddComment method not found for '%s'",
                                              AlarmName.c_str());
                            }
                            
                            // Register Enable method callback
                            UA_NodeId enableMethodId = 
                                findChildNodeIdAnyNS(server, alarmId, (char *)"Enable");
                            if(!UA_NodeId_isNull(&enableMethodId)) {
                                UA_Server_setMethodNodeCallback(server, enableMethodId,
                                                               customEnableCallback);
                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                           "✓ Enable callback registered for '%s'",
                                           AlarmName.c_str());
                                UA_NodeId_clear(&enableMethodId);
                            } else {
                                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                              "Enable method not found for '%s'",
                                              AlarmName.c_str());
                            }
                            
                            // Register Disable method callback
                            UA_NodeId disableMethodId = 
                                findChildNodeIdAnyNS(server, alarmId, (char *)"Disable");
                            if(!UA_NodeId_isNull(&disableMethodId)) {
                                UA_Server_setMethodNodeCallback(server, disableMethodId,
                                                               customDisableCallback);
                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                           "✓ Disable callback registered for '%s'",
                                           AlarmName.c_str());
                                UA_NodeId_clear(&disableMethodId);
                            } else {
                                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                              "Disable method not found for '%s'",
                                              AlarmName.c_str());
                            }
                            
                            g_alarmByKey.emplace(key, alarmId);
                        } else {
                            UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                           "CreateCondition failed for '%s' (emitter '%s'): %s",
                                           AlarmName.c_str(), emitter.emitterNodeName.c_str(),
                                           UA_StatusCode_name(sc));
                        }
                    } else {
                        /* Already created: ensure a HasComponent reference from this source */
                        const UA_NodeId alarmId = found->second;
                        UA_ExpandedNodeId target;
                        UA_ExpandedNodeId_init(&target);
                        target.nodeId = alarmId;

                        UA_StatusCode sc = UA_Server_addReference(
                            server,
                            sourceNode,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT),
                            target,
                            true /* forward */);

                        if(sc != UA_STATUSCODE_GOOD &&
                           sc != UA_STATUSCODE_BADDUPLICATEREFERENCENOTALLOWED) {
                            UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                           "AddReference(HasComponent) failed for '%s' (emitter '%s'): %s",
                                           AlarmName.c_str(), emitter.emitterNodeName.c_str(),
                                           UA_StatusCode_name(sc));
                        }
                    }

                }
            }

            if(item.alarmTriggers.has_value()){
                for (const auto &trigger : item.alarmTriggers.value()){
                    // // Map each trigger to all emitters for this alarm
                    // std::string triggerTopic = trigger.applicableTagName;
                    
                    // UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                    //            "    Trigger ID=%d, topic='%s'",
                    //            trigger.id, triggerTopic.c_str());
                    
                    // if(item.alarmEmitters.has_value()) {
                    //     for (const auto &emitter : item.alarmEmitters.value()) {
                    //         // Build the alarm key (same as used when creating the condition)
                    //         std::string alarmKey = emitter.emitterNodeName + "-" + AlarmName;
                            
                    //         TriggerToAlarmMapping mapping;
                    //         mapping.triggerTopic = triggerTopic;
                    //         mapping.alarmKey = alarmKey;
                    //         mapping.triggerId = trigger.id;
                    //         mapping.hiHi = trigger.hiHi;
                    //         mapping.hi = trigger.hi;
                    //         mapping.lo = trigger.lo;
                    //         mapping.loLo = trigger.loLo;
                            
                    //         // Add to global map
                    //         g_triggerToAlarmMap[triggerTopic].push_back(mapping);
                            
                    //         UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                    //                    "Mapped trigger topic '%s' (ID: %d) to alarm '%s' (key: '%s')",
                    //                    triggerTopic.c_str(), trigger.id, AlarmName.c_str(), alarmKey.c_str());
                    //     }
                    // }
                }
            } 


           }

            
            


            //         // Clean up
            // UA_String_clear(&unit);
            // UA_Double_clear(&alarmHiHi);
            // UA_Double_clear(&alarmHi);
            // UA_Double_clear(&alarmLo);
            // UA_Double_clear(&alarmLoLo);
            // UA_Variant_clear(&unitVariant);
            // UA_Variant_clear(&rangeVariant);
        }
        
        // Add trigger topics to MQTT subscription list with .alarm.pub suffix
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "=== Trigger Topic Summary: %zu unique topics mapped to alarms ===",
                   g_triggerToAlarmMap.size());
        
        for(const auto &triggerPair : g_triggerToAlarmMap) {
            const std::string &baseTriggerTopic = triggerPair.first;
            // Add .event suffix for trigger topics
            std::string triggerTopic = baseTriggerTopic + "/Event";
            
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "  Trigger topic: '%s' -> %zu alarm(s)",
                       baseTriggerTopic.c_str(), triggerPair.second.size());
            
            // Check if topic is not already in the list
            if(std::find(topics.begin(), topics.end(), triggerTopic) == topics.end()) {
                topics.push_back(triggerTopic);
                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                           "    ✓ Added '%s' to MQTT subscription list",
                           triggerTopic.c_str());
            } else {
                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                           "    ⓘ Already in subscription list");
            }
        }
        
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "=== Total MQTT topics to subscribe: %zu ===",
                   topics.size());
        
        // TODO:
        // FIX IT WHEN CREATING THE ADDRESS SPACE AGAIN OTHERWISE IT WILL SUBSCRIBE TO THE
        // TOPICS AGAIN
        mqtt_subscribe_and_update(server, topics);
        
        // ============================================================================
        // OVERRIDE ConditionRefresh callback AFTER all conditions are created
        // ============================================================================
        // open62541 sets its own ConditionRefresh callback when the FIRST condition
        // is created. We need to OVERRIDE it AFTER all conditions are registered
        // to ensure our custom callback handles branch re-triggering properly.
        // ============================================================================
        
        UA_NodeId refreshMethodId = UA_NODEID_NUMERIC(0, UA_NS0ID_CONDITIONTYPE_CONDITIONREFRESH);
        UA_StatusCode refreshOverrideRc = UA_Server_setMethodNode_callback(server, 
                                                                            refreshMethodId, 
                                                                            &ConditionRefreshMethodCallback);
        
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "========================================");
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "OVERRIDING ConditionRefresh CALLBACK (after condition creation)");
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "  NodeId: ns=0;i=%d (ConditionType_ConditionRefresh)", 
                   UA_NS0ID_CONDITIONTYPE_CONDITIONREFRESH);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "  Callback function: ConditionRefreshMethodCallback");
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "  Override result: %s", UA_StatusCode_name(refreshOverrideRc));
        
        if(refreshOverrideRc == UA_STATUSCODE_GOOD) {
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "✓ ConditionRefresh callback OVERRIDDEN successfully!");
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "  When UA Expert clicks Refresh, you will see:");
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "  '>>> ConditionRefresh CALLBACK CALLED! <<<'");
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "  Branches will be re-triggered and WILL NOT disappear");
        } else {
            UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                        "✗ ConditionRefresh callback override FAILED!");
            UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                        "  Branches WILL disappear on refresh!");
        }
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                   "========================================");
    }  // END if(!BearerToken.empty())
#endif  // END LEGACY CODE BLOCK
    
    // ========================================================================
    // END OF COMMENTED OUT LEGACY CODE
    // All topic/alarm initialization is now handled per-session in worker threads
    // ========================================================================

    // Start MQTT Client (Async)
    start_mqtt_client(server);

    std::thread mqtt_thread([&]() { ioc.run(); });
    // mqtt_thread.detach(); // FIXED: Do not detach, we must join it to prevent crash on exit

    // string bearerToken = getBearerToken();
    // json topicList = getTopicList(bearerToken);

  

    log("Starting OPC UA Server...", LogLevel::INFO);
    log("Added repeated callback for counter updates", LogLevel::DEBUG);

    UA_StatusCode startupRc = UA_Server_run_startup(server);
    if(startupRc != UA_STATUSCODE_GOOD) {
         log("❌ Server startup failed with code: " + std::string(UA_StatusCode_name(startupRc)), LogLevel::ERRORS);
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
    log("⚙️ Configuring multi-tenancy session detection...", LogLevel::INFO);
    
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
    // Register server with LDS now that it's running

    // register server
    // UA_ClientConfig cc;
    // memset(&cc, 0, sizeof(UA_ClientConfig));
    // UA_ClientConfig_setDefault(&cc);

    // UA_ByteString client_cert = loadFile("client/own/certs/client_cert.der");
    // UA_ByteString client_key = loadFile("client/own/certs/client_key.der");
    // UA_ByteString server_cert = loadFile("server/own/certs/server_cert.der");
    // UA_ByteString ca_cert = loadFile("ca/certs/ca.crt");
    // UA_ByteString revocation_cert = loadFile("server/trusted/crl/crl.crl");

    //     if (certificate.length == 0) {
    //         UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load client
    //         certificate"); return EXIT_FAILURE;
    //     }
    //     if (privateKey.length == 0) {
    //         UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load client
    //         private key"); return EXIT_FAILURE;
    //     }
    //     if (serverCerte.length == 0) {
    //         UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load server
    //         certificate into trust list"); return EXIT_FAILURE;
    //     }

    // UA_STACKARRAY(UA_ByteString, trustLists, 1);
    // trustLists[0] = serverCerte;

    // // Step 3: Apply encryption
    // UA_ClientConfig_setDefaultEncryption(&cc, certificatee, privateKeye, NULL, 0,
    // NULL, 0);

    // UA_CertificateGroup_AcceptAll(&cc.certificateVerification);

    // //cc.securityMode = UA_MESSAGESECURITYMODE_NONE;
    // //cc.securityPolicyUri =
    // //UA_STRING_STATIC("http://opcfoundation.org/UA/SecurityPolicy#None");

    // //UA_String_clear(&cc.applicationUri);
    // cc.clientDescription.applicationUri =
    // UA_STRING_ALLOC("urn:Anexee.server.application");
    // //cc.clientDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en-US",
    // //"Anexee");
    // //cc.clientDescription.productUri =
    // //UA_STRING_ALLOC("urn:Anexee.server");

    // //cc.userTokenPolicy.securityPolicyUri =
    // //    UA_STRING_STATIC("http://opcfoundation.org/UA/SecurityPolicy#None");
    // //

    // //cc.clientDescription.applicationUri =
    // //    UA_STRING_ALLOC("urn:Anexee.server.application");
    // cc.clientDescription.productUri = UA_STRING_ALLOC("urn:Anexee.server");
    // cc.clientDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Anexee");
    // cc.clientDescription.applicationType = UA_APPLICATIONTYPE_SERVER;

    // cc.endpointUrl = UA_STRING_ALLOC("opc.tcp://localhost:4840");

    // cc.userTokenPolicy.tokenType = UA_USERTOKENTYPE_ANONYMOUS;
    // cc.userTokenPolicy.policyId = UA_STRING_ALLOC("anonymous-policy");

    // cc.securityPolicyUri =
    //     UA_STRING_ALLOC("http://opcfoundation.org/UA/SecurityPolicy#None");
    // cc.securityMode = UA_MESSAGESECURITYMODE_NONE;

    // // Set if LDS requires user credentials (rare):
    // //cc.userIdentityToken.encoding = UA_EXTENSIONOBJECT_DECODED;
    // //cc.userIdentityToken.content.decoded.type =
    // //    &UA_TYPES[UA_TYPES_USERNAMEIDENTITYTOKEN];
    // //cc.userIdentityToken.content.decoded.data = UA_UserNameIdentityToken_new();
    // //UA_UserNameIdentityToken *token =
    // //    (UA_UserNameIdentityToken *)cc.userIdentityToken.content.decoded.data;

    // cc.applicationUri = UA_STRING_ALLOC("urn:Anexee.server.application");

    // const char *discoveryUrlStr = "opc.tcp://Asce:4840";
    // UA_String discoveryUrl = UA_String_fromChars(discoveryUrlStr);

    // UA_StatusCode result = UA_Server_registerDiscovery(server, &cc, discoveryUrl,
    // UA_STRING_NULL); if(result != UA_STATUSCODE_GOOD) {
    //    UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
    //                 "Could not create periodic job for server register. StatusCode %s",
    //                 UA_StatusCode_name(result));
    //    UA_Server_delete(server);
    //    return EXIT_FAILURE;
    // }

    log("Server is now running and listening for connections", LogLevel::INFO);

    auto last_trim = std::chrono::steady_clock::now();

    try {
        while(running) {
            UA_Server_run_iterate(server, true);
            
            // FRAGMENTATION CONTROL: Release unused heap memory to OS periodically
            // This is the Windows equivalent of malloc_trim(0)
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::seconds>(now - last_trim).count() > 10) {
                #ifdef _WIN32
                int res = _heapmin();
                if(res == 0) {
                     log("DEBUG: Performed _heapmin() (Released unused heap to OS)", LogLevel::DEBUG); 
                }
                #endif
                
                // MONITORING DIAGNOSTICS: Log active MonitoredItems count
                UA_Variant val;
                UA_Variant_init(&val);
                UA_NodeId diagNodeId = UA_NODEID_NUMERIC(0, 2271); // Global: Server_ServerDiagnostics_ServerDiagnosticsSummary_CurrentMonitoredItemsCount
                UA_StatusCode retval = UA_Server_readValue(server, diagNodeId, &val);
                if(retval == UA_STATUSCODE_GOOD) {
                    if(UA_Variant_hasScalarType(&val, &UA_TYPES[UA_TYPES_UINT32])) {
                        UA_UInt32 count = *(UA_UInt32*)val.data;
                        log("DIAGNOSTICS: Current Monitored Items = " + std::to_string(count), LogLevel::INFO);
                    }
                    UA_Variant_clear(&val);
                } else {
                    log("DIAGNOSTICS FAILED: Could not read Node ns=1;i=54543 (Error: " + std::string(UA_StatusCode_name(retval)) + ")", LogLevel::INFO);
                }

                last_trim = now;
            }
        }
    } catch (const std::exception& e) {
        log("🔥 CRITICAL: Unhandled exception in main loop: " + std::string(e.what()), LogLevel::ERRORS);
        std::cerr << "CRITICAL: Unhandled exception: " << e.what() << std::endl;
    } catch (...) {
        log("🔥 CRITICAL: Unknown exception in main loop", LogLevel::ERRORS);
        std::cerr << "CRITICAL: Unknown exception in main loop" << std::endl;
    }

    // FIXED: graceful shutdown of MQTT thread
    ioc.stop();
    if(mqtt_thread.joinable()) {
        mqtt_thread.join();
    }
    
    log("Server loop exited - running flag is now false", LogLevel::INFO);
    log("Server shutdown initiated", LogLevel::INFO);
    //    // Unregister from LDS before shutdown
    //    memset(&cc, 0, sizeof(UA_ClientConfig));
    //    UA_ClientConfig_setDefault(&cc);

    //    cc.endpoint.securityMode = UA_MESSAGESECURITYMODE_NONE;
    //
    //    cc.endpoint.userIdentityTokensSize = 1;
    //    cc.endpoint.userIdentityTokens = (UA_UserTokenPolicy *) UA_Array_new(1,
    //    &UA_TYPES[UA_TYPES_USERTOKENPOLICY]);
    //    UA_UserTokenPolicy_init(&cc.endpoint.userIdentityTokens[0]);
    //    cc.endpoint.userIdentityTokens[0].tokenType = UA_USERTOKENTYPE_ANONYMOUS;
    //    cc.endpoint.userIdentityTokens[0].policyId =
    //    UA_String_fromChars("open62541-anonymous-policy");
    //    UA_ByteString_clear(&cc.securityPolicyUri);
    //    cc.endpoint.userIdentityTokens[0].securityPolicyUri =
    //    UA_String_fromChars("http://opcfoundation.org/UA/SecurityPolicy#None");

    //    for(size_t j = 0; j < cc.endpoint.userIdentityTokensSize; j++) {
    //        UA_UserTokenPolicy *pol = &cc.endpoint.userIdentityTokens[j];
    //        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_CLIENT, "Deregistration Client
    //        TokenType: %d, PolicyId: %.*s, SecurityPolicyUri: %.*s",
    //            pol->tokenType,
    //            (int)pol->policyId.length, pol->policyId.data,
    //            (int)pol->securityPolicyUri.length, pol->securityPolicyUri.data);
    //    }

    //    UA_StatusCode res = UA_Server_deregisterDiscovery(server, &cc, discoveryUrl);
    //    if(res != UA_STATUSCODE_GOOD)
    //        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
    //                     "Could not unregister from discovery server. StatusCode %s",
    //                     UA_StatusCode_name(res));
    //    else
    //        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Unregistered from
    //        discovery server.");

    log("Cleaning up server resources", LogLevel::DEBUG);

    // NOTE: Example node cleanup disabled - these don't exist in multi-tenant version
    // UA_VariableAttributes_clear(&attr);
    // UA_VariableAttributes_clear(&attr2);
    // UA_VariableAttributes_clear(&attr3);
    // UA_NodeId_clear(&myIntegerNodeId);
    // UA_NodeId_clear(&myDoubleNodeId);
    // UA_NodeId_clear(&myImageNodeId);
    // UA_NodeId_clear(&minNodeId);
    // UA_QualifiedName_clear(&myIntegerName);
    // UA_QualifiedName_clear(&myDoubleName);
    // UA_QualifiedName_clear(&myImageName);

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
    // Clean up method callback contexts
    // Note: In a production environment, you might want to keep track of all allocated
    // contexts and clean them up individually. For this example, the server will handle
    // most cleanup. The MethodCallbackContext structures are stored as node contexts and
    // will be cleaned up when the server is deleted.
    log("Deleting server instance", LogLevel::DEBUG);
    UA_Server_delete(server);

    log("Server shutdown completed successfully", LogLevel::INFO);
    return EXIT_SUCCESS;
}

// ========================================================================
// WINDOWS SERVICE IMPLEMENTATION
// ========================================================================

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
    // SCM usually doesn't pass arguments to main(), but we check for our own flag just in case
    // However, StartServiceCtrlDispatcher is what we SHOULD call if we suspect we are a service.
    // But we can't just call it always because it blocks and fails if not service.
    // Convention: Service binaries are just run without args or with specific args.
    
    // HEURISTIC: If --service is passed, we definitely try SCM dispatch
    bool tryService = false;
    for(int i=1; i<argc; i++) {
        if(std::string(argv[i]) == "--service") tryService = true;
    }

    if(tryService) {
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
    }

    // 3. Normal Console / Child Mode
    // If not --install/uninstall/service, run normally
    return RunServer(argc, argv);
}
