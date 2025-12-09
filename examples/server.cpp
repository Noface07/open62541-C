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
#include <open62541/client.h>
#include <open62541/client_config_default.h>
#include <open62541/plugin/historydata/history_data_backend.h>
#include <open62541/plugin/historydata/history_data_backend_memory.h>
#include <open62541/plugin/historydata/history_data_gathering_default.h>
#include <open62541/plugin/historydata/history_database_default.h>

#include <chrono>
#include <future>
#include <thread>
#include <mutex>

#include "AandC.h"
#include "AlarmConfig.h"
#include "Logger.h"
#include "fetchAPI.h"
#include "SessionManager.h"
#include <async_mqtt/all.hpp>
#include <async_mqtt/asio_bind/predefined_layer/mqtts.hpp>
#include <async_mqtt/asio_bind/predefined_layer/ws.hpp>
#include <async_mqtt/asio_bind/predefined_layer/wss.hpp>
#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <nlohmann/json.hpp>
#include "alarm_enums.h"
#include "AccessControl.h"

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

static UA_HistoryDataGathering *g_gathering = NULL;

/* Cache created alarm Condition nodes keyed by emitter + alarm name */
static std::unordered_map<std::string, UA_NodeId> g_alarmByKey;


// ---------------------------------------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------------------
/* Map trigger topic (applicableTagName) to list of alarm keys (emitter+alarmName) */
// TriggerToAlarmMapping struct moved to AandC.h

std::unordered_map<std::string, std::vector<TriggerToAlarmMapping>> g_triggerToAlarmMap;

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
    
    // Handle anonymous login - use appsettings credentials
    if(isAnonymous) {
        log("  Anonymous login detected - using default credentials from appsettings", LogLevel::INFO);
        username = g_authUsername;  // From appsettings.json
        password = g_authPassword;  // From appsettings.json
    } else {
        log("  User: " + username, LogLevel::INFO);
    }
    
    if(username.empty() || password.empty()) {
        log("❌ Missing credentials", LogLevel::ERRORS);
        return UA_STATUSCODE_BADUSERACCESSDENIED;
    }
    
    // ========================================================================
    // STEP 2: Get Bearer Token
    // ========================================================================
    json tokenResponse;
    try {
        tokenResponse = getBearerToken(g_apiHost, g_apiPort, username, password);
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
        // currentOrgId is string, org.id is int - convert for comparison
        if(std::to_string(org.id) == profile.currentOrgId) {
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

map<string, UA_NodeId> nodeMap;
struct TopicInfo {
    int tagId;
    string name;
    string tagType;
    double rangeMin;
    double rangeMax;
    // int source;
    // int infoId;
    // int quality;
    // int updateType;
};

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
    nodeMap[path] = nodeId;
    return nodeId;
}

as::io_context ioc;
using client_t = am::client<am::protocol_version::v5, am::protocol::mqtt>;
client_t amcl{ioc.get_executor()};

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
                    co_await amcl.async_publish(topic, payload, am::qos::at_most_once);
                } catch(const std::exception &e) {
                    UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                "[MQTT-PUB] ✗ Publish error to '%s': %s", 
                                topic.c_str(), e.what());
                    
                    // Connection might be broken - queue for retry
                    std::lock_guard<std::mutex> lock(g_mqtt_queue_mutex);
                    g_mqtt_queue.push_back({topic, payload});
                    UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                  "[MQTT-QUEUE] Message requeued after error (queue size: %zu)",
                                  g_mqtt_queue.size());
                }
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
static void
setStealthValueByPath(UA_Server *server, UA_NodeId startNode, 
                      std::vector<const char*> path, 
                      void *newValue, const UA_DataType *type) {
    
    // 1. Find the target node using the helper
    //    (Traverses the path safely, handling mixed Namespaces)
    UA_NodeId targetNode = findNodeByPath(server, startNode, path);
    
    if(UA_NodeId_isNull(&targetNode)) {
        // Target not found. Silently return to avoid spamming logs 
        // during startup or partial configurations.
        return;
    }

    // 2. Read current value from the server
    UA_Variant current;
    UA_Variant_init(&current);
    UA_StatusCode readStatus = UA_Server_readValue(server, targetNode, &current);

    // If read fails (e.g. bad permissions), we can't compare, so we abort.
    if(readStatus != UA_STATUSCODE_GOOD) {
        UA_NodeId_clear(&targetNode);
        return;
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
        UA_Variant v;
        UA_Variant_init(&v);
        // Create a variant pointing to the new data
        UA_Variant_setScalarCopy(&v, newValue, type);
        
        // Perform the write
        UA_Server_writeValue(server, targetNode, v);
        
        // Clean up the temporary write variant
        UA_Variant_clear(&v);
    }

    // 5. Cleanup
    UA_Variant_clear(&current);  // Free memory from the Read operation
    UA_NodeId_clear(&targetNode); // Free memory from the Find operation
}



// Wrapper for direct children (Convenience function)
static void
setStealthValueChecked(UA_Server *server, UA_NodeId parentId, 
                       const char *propertyName, 
                       void *newValue, const UA_DataType *type) {
    
    // Just call the master function with a path of size 1
    std::vector<const char*> path = {propertyName};
    setStealthValueByPath(server, parentId, path, newValue, type);
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
        std::string timestamp = std::format("{:%Y-%m-%d %H:%M:%S}", std::chrono::system_clock::now());
        
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
            {"CommandName", "Acknowledge"}
        };
        
        if(!commentText.empty()) {
            out["Event"]["Comment"] = commentText;
        }
        
        // Publish request
        std::string subTopic = triggerTopics[0] + ".event";
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
        std::string timestamp = std::format("{:%Y-%m-%d %H:%M:%S}", std::chrono::system_clock::now());
        
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
            {"CommandName", "Confirm"}
        };
        
        if(!commentText.empty()) {
            out["Event"]["Comment"] = commentText;
        }
        
        // Publish request
        std::string subTopic = triggerTopics[0] + ".event";
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
        std::string timestamp = std::format("{:%Y-%m-%d %H:%M:%S}", std::chrono::system_clock::now());
        
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
            {"CommandName", "Enable"}
        };
        
        // Publish request
        std::string subTopic = triggerTopics[0] + ".event";
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
        std::string timestamp = std::format("{:%Y-%m-%d %H:%M:%S}", std::chrono::system_clock::now());
        
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
            {"CommandName", "Disable"}
        };
        
        // Publish request
        std::string subTopic = triggerTopics[0] + ".event";
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
        std::string timestamp = std::format("{:%Y-%m-%d %H:%M:%S}", std::chrono::system_clock::now());
        
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
            {"CommandName", "AddComment"},
            {"Comment", commentText}
        };
        
        // Publish request
        std::string subTopic = triggerTopics[0] + ".event";
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
static void
writeCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
              const UA_NodeId *nodeId, void *nodeContext, const UA_NumericRange *range,
              const UA_DataValue *data) {
    // if (is_internal_write) return;  // 🔒 Prevent feedback loop

    // Find the topic for this node
    std::string topic;

    //if(is_internal_write)
    //    goto Alarms;

    for(const auto &pair : nodeMap) {
        if(UA_NodeId_equal(&pair.second, nodeId)) {
            topic = pair.first;
            break;
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

        // Add metadata to the same dataPoint
        dataPoint["TagId"] = topicMap[topic].tagId;
        dataPoint["TagType"] = topicMap[topic].tagType;
        dataPoint["DatapointId"] = topicMap[topic].tagId;

        dataPoint["TimeStamp"] = UA_DateTime_now();

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

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                "✓ Created virtual branch for GUID '%s' (NodeId: ns=%u;s=%s)",
                guid.c_str(), masterBranchId.namespaceIndex, nodeIdStr.c_str());

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

void
mqtt_subscribe_and_update(UA_Server *server, const std::vector<std::string> &topics) {
    // Use global ioc and amcl
    as::co_spawn(
        ioc,
        [&topics, server]() -> as::awaitable<void> {
            // Reconnection loop
            while(running) {
                try {
                    log("Attempting to connect to MQTT broker...", LogLevel::INFO);
                    // Connect to broker
                    co_await amcl.async_underlying_handshake("216.48.184.131", "15579",
                                                             as::use_awaitable);




                    // Start MQTT session with username/password
                    auto connack_opt = co_await amcl.async_start(
                        am::v5::connect_packet{
                            true,          // clean_start
                            0x1234,        // keep_alive
                            "",            // Client Identifier
                            std::nullopt,  // no will
                            "portal",      // username
                            "dt0Unw7QRh"   // password
                        },
                        as::use_awaitable);
                    if(!connack_opt) {
                        throw std::runtime_error(
                            "Failed to start MQTT session (CONNACK not received).");
                    }
                    log("Successfully connected to MQTT broker.", LogLevel::INFO);
                    g_mqtt_connected.store(true);





                    // Subscribe to all topics and their corresponding ".event" channels
                    std::vector<am::topic_subopts> sub_entry;
                    for(const auto &topic : topics) {
                        sub_entry.push_back({topic, am::qos::at_most_once});
                        sub_entry.push_back(
                            {topic + ".event", am::qos::at_most_once});
                    }


                    auto suback_opt = co_await amcl.async_subscribe(
                        am::v5::subscribe_packet{*amcl.acquire_unique_packet_id(),
                                                 am::force_move(sub_entry)},
                        as::use_awaitable);
                    if(!suback_opt) {
                        throw std::runtime_error("Failed to subscribe to topics.");
                    }
                    log("Successfully subscribed to " + std::to_string(topics.size()) +
                            " topics.",
                        LogLevel::INFO);
                    
                    // Flush queued messages after successful reconnect (SEQUENTIAL)
                    {
                        std::lock_guard<std::mutex> lock(g_mqtt_queue_mutex);
                        size_t queueSize = g_mqtt_queue.size();
                        if(queueSize > 0) {
                            log("Flushing " + std::to_string(queueSize) + " queued messages sequentially...",
                                LogLevel::INFO);
                        }
                    }
                    
                    // Flush outside the lock, one message at a time
                    while(g_mqtt_connected.load()) {
                        QueuedMessage msg;
                        {
                            std::lock_guard<std::mutex> lock(g_mqtt_queue_mutex);
                            if(g_mqtt_queue.empty()) {
                                log("Queue flush complete.", LogLevel::INFO);
                                break;
                            }
                            msg = g_mqtt_queue.front();
                            g_mqtt_queue.pop_front();
                        }
                        
                        // Publish one message
                        try {
                            co_await amcl.async_publish(msg.topic, msg.payload, am::qos::at_most_once);
                        } catch(const std::exception &e) {
                            UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                        "[MQTT-FLUSH] Failed to send queued message: %s", e.what());
                            // Requeue at front and stop flushing
                            {
                                std::lock_guard<std::mutex> lock(g_mqtt_queue_mutex);
                                g_mqtt_queue.push_front(msg);
                            }
                            log("Queue flush aborted due to connection error. Messages will retry on next reconnect.",
                                LogLevel::INFO);
                            break;
                        }
                    }



                    // Receive loop
                    while(running) {
                        
                        
                        auto pv_opt = co_await amcl.async_recv(as::use_awaitable);
                        if(!pv_opt) {
                            // This indicates a graceful disconnect or an issue.
                            log("MQTT connection closed by broker or network issue.",
                                LogLevel::ERRORS);
                            g_mqtt_connected.store(false);
                            break;  // Exit receive loop to trigger reconnection
                        }


                        pv_opt->visit(am::overload{
                            [&](client_t::publish_packet &p) {
                                std::string topic = p.topic();
                                std::string payload = p.payload();

                                /* Check if this is a trigger topic for alarm conditions (.alarm.pub suffix) */
                                std::string baseTopic = topic;
                                bool isAlarmPubTopic = false;
                                
                                // Check if topic ends with .event and extract base topic
                                if(topic.size() > 6 && topic.rfind(".event") == topic.size() - 6) {
                                    baseTopic = topic.substr(0, topic.size() - 6);
                                    isAlarmPubTopic = true;
                                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                               "Detected .event topic, base='%s'",
                                               baseTopic.c_str());
                                }

                                auto triggerIt = g_triggerToAlarmMap.find(baseTopic);
                                if(triggerIt != g_triggerToAlarmMap.end()) {
                                    std::lock_guard<std::mutex> lock(g_alarmMutex);
                                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                               "✓ Topic '%s' FOUND in trigger map with %zu alarm mappings",
                                               topic.c_str(), triggerIt->second.size());
                                    // This is a trigger topic, process the alarm payload
                                    try {
                                        auto alarmPayload = json::parse(payload);
                                        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                   "Parsed JSON, checking for 'Event' field...");
                                        
                                        if(alarmPayload.contains("Event")) {
                                            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                       "✓ 'Event' field found in payload");
                                            auto &alarm = alarmPayload["Event"];
                                            
                                            // =================================================================
                                            // Process Telemetry Update (UpdateType always = 1 from AE Engine)
                                            // =================================================================
                                            // Extract AeInstanceID GUID (required for branch management)
                                            std::string aeInstanceId = "";
                                            if(alarm.contains("AeInstanceID")) {
                                                    if(alarm["AeInstanceID"].is_string()) {
                                                        aeInstanceId = alarm["AeInstanceID"].get<std::string>();
                                                    } else if(alarm["AeInstanceID"].is_number()) {
                                                        // Convert number to string
                                                        aeInstanceId = std::to_string(alarm["AeInstanceID"].get<int>());
                                                    } else if(!alarm["AeInstanceID"].is_null()) {
                                                        // Fallback: use dump() for other types
                                                        aeInstanceId = alarm["AeInstanceID"].dump();
                                                    }
                                                }
                                                
                                                // Extract alarm data from payload (NEW CAPITALIZED FORMAT)
                                                int AETypeID = 0;
                                                if(alarm.contains("AeTypeID")) {
                                                    if(alarm["AeTypeID"].is_string()) {
                                                        AETypeID = std::stoi(alarm["AeTypeID"].get<std::string>());
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
                                                std::string alarmMessage = alarm.value("Alarm_message", "Alarm triggered");
                                                std::string alarmName = alarm.value("Name", "");
                                                std::string comment = alarm.value("Comment", "");
                                                
                                                // Source enum: 1=OPC, 2=Simulator, 3=Expr, 4=AEEngine
                                                int sourceEnumValue = alarm.value("Source", static_cast<int>(AlarmSource::AEEngine));
                                                AlarmSource sourceEnum = intToAlarmSource(sourceEnumValue);
                                                std::string source = alarmSourceToString(sourceEnum);
                                                
                                                // Quality enum: 1=Good, 2=Bad, 3=Unknown
                                                int qualityEnumValue = alarm.value("Quality", static_cast<int>(AlarmQuality::Good));
                                                AlarmQuality qualityEnum = intToAlarmQuality(qualityEnumValue);
                                                std::string quality = alarmQualityToString(qualityEnum);
                                                
                                                // UpdateType enum: 1=Telemetry, 2=Command, 3=BulkData
                                                int updateTypeValue = alarm.value("UpdateType", static_cast<int>(UpdateType::Telemetry));
                                                UpdateType updateType = static_cast<UpdateType>(updateTypeValue);
                                                
                                                std::string timestamp = alarm.value("Timestamp", 
                                                    std::format("{:%Y-%m-%d %H:%M:%S}", std::chrono::system_clock::now()));
                                                
                                                // Extract Retain directly from payload
                                                bool retain = alarm.value("Retain", false);
                                                
                                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                           "✓ Telemetry received: AeTypeID=%d, AeInstanceID='%s', Active=%d, Enabled=%d, Severity=%d, Shelved=%d, Acked=%d, Confirmed=%d, Quality=%s, UpdateType=%d, Name='%s', msg='%s'",
                                                    AETypeID, aeInstanceId.c_str(), active, enabled,
                                                    severity, shelved, acked, confirmed, quality.c_str(), updateType, 
                                                    alarmName.c_str(), alarmMessage.c_str());
                                                    
                                                // Iterate through all alarms mapped to this trigger
                                                for(const auto &mapping : triggerIt->second) {
                                                // Filter by AETypeID - only trigger the matching alarm
                                                if(mapping.alarmId != AETypeID) {
                                                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                               "Skipping alarm '%s' (ID: %d) - does not match AETypeID %d",
                                                        mapping.alarmKey.c_str(),
                                                        mapping.alarmId, AETypeID);
                                                    continue;
                                                }
                                                
                                                // Find the alarm condition in g_alarmByKey
                                                auto alarmIt = g_alarmByKey.find(mapping.alarmKey);
                                                if(alarmIt == g_alarmByKey.end()) {
                                                    UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                                 "Alarm key '%s' not found in g_alarmByKey",
                                                                 mapping.alarmKey.c_str());
                                                    continue;
                                                }
                                                
                                                UA_NodeId conditionId = alarmIt->second;
                                                



                                                // Get or create branch for this GUID
                                                UA_NodeId branchNodeId = UA_NODEID_NULL;
                                                UA_StatusCode branchSc = getOrCreateAlarmBranch(server, conditionId, 
                                                                                                 aeInstanceId, mapping.alarmKey,
                                                                                                 &branchNodeId);


                                                if(branchSc != UA_STATUSCODE_GOOD) {
                                                    UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                                  "Failed to get/create branch for GUID '%s': %s",
                                                                  aeInstanceId.c_str(), UA_StatusCode_name(branchSc));
                                                    continue;
                                                }
                                                
                                                // Log with proper NodeId format
                                                UA_String branchNodeStr = UA_STRING_NULL;
                                                UA_NodeId_print(&branchNodeId, &branchNodeStr);
                                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                           "Processing alarm '%s' with GUID '%s' on branch NodeId: %.*s",
                                                           mapping.alarmKey.c_str(), aeInstanceId.c_str(),
                                                           (int)branchNodeStr.length, branchNodeStr.data);
                                                UA_String_clear(&branchNodeStr);


                                                
                                                // Use conditionId for all condition field operations (branch nodes don't have condition properties)
                                                // The branchNodeId is only used for the BranchId field in events to identify the branch
                                                UA_NodeId alarmId = conditionId;  // Use condition for all field operations
                                                
                                                // Update the alarm condition based on MQTT payload
                                                UA_Variant val;
                                                UA_Variant_init(&val);
                                                
                                                // Check current EnabledState to avoid unnecessary event triggers
                                                UA_NodeId enabledStateId = findChildNodeIdAnyNS(server, alarmId, (char*)"EnabledState");
                                                bool currentEnabled = false;
                                                if(!UA_NodeId_isNull(&enabledStateId)) {
                                                    UA_QualifiedName qId = UA_QUALIFIEDNAME_ALLOC(0, (char*)"Id");
                                                    UA_Variant enabledVar;
                                                    UA_StatusCode enabledRc = UA_Server_readObjectProperty(server, enabledStateId, qId, &enabledVar);
                                                    if(enabledRc == UA_STATUSCODE_GOOD && UA_Variant_hasScalarType(&enabledVar, &UA_TYPES[UA_TYPES_BOOLEAN])) {
                                                        currentEnabled = *(UA_Boolean*)enabledVar.data;
                                                    }
                                                }
                                                
                                                // Only set EnabledState if it's actually changing
                                                if(enabled != currentEnabled) {
                                                    if(!enabled) {
                                                        // DISABLE - perform full cleanup per OPC UA spec
                                                        // IMPORTANT: performDisable sends events FIRST (while enabled)
                                                        // THEN sets EnabledState=false internally
                                                        
                                                        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                                   "Disabling alarm '%s'...",
                                                                   mapping.alarmKey.c_str());
                                                        
                                                        // performDisable handles everything: branch events, aggregate event, EnabledState
                                                        performDisable(server, alarmId, mapping.alarmKey);
                                                        
                                                        // Skip rest of alarm processing - disabled alarms don't process active/severity
                                                        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                                   "Skipping alarm processing for disabled alarm '%s'",
                                                                   mapping.alarmKey.c_str());
                                                        continue;  // Move to next alarm
                                                    } else {
                                                        // ENABLE - just set EnabledState to true
                                                        UA_Boolean enableVal = UA_TRUE;
                                                        setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &enableVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                        
                                                        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                                   "✓ EnabledState set to TRUE for '%s' (stealth) - will process this message",
                                                                   mapping.alarmKey.c_str());
                                                        
                                                        // Update currentEnabled so we don't skip processing this message
                                                        currentEnabled = true;
                                                    }
                                                }
                                                
                                                // Skip processing if alarm is already disabled (no state change occurred)
                                                if(!currentEnabled) {
                                                    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                               "Skipping alarm '%s' - already disabled (no state change)",
                                                               mapping.alarmKey.c_str());
                                                    continue;
                                                }
                                                
                                    // ============================================================
                                    // STEP 1: Get previous state & use payload acked/confirmed
                                    // ============================================================
                                    bool previousActive = false;
                                    // NEW: Use acked/confirmed directly from MQTT payload
                                    bool currentAcked = acked;
                                    bool currentConfirmed = confirmed;
                                    
                                    // Treat AEInstanceID = 0 as aggregate (skip branch logic)
                                    if(!aeInstanceId.empty() && aeInstanceId != "null" && aeInstanceId != "NULL" && aeInstanceId != "{}" && aeInstanceId != "0") {
                                        // Check branch state map for previous active state only
                                        auto &branchStateMap = g_branchStates[mapping.alarmKey];
                                        auto branchStateIt = branchStateMap.find(aeInstanceId);
                                        if(branchStateIt != branchStateMap.end()) {
                                            // Branch exists - get previous active state
                                            previousActive = branchStateIt->second.active;
                                            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                       "Branch '%s' previousActive=%d, currentAcked=%d (from MQTT payload)", 
                                                       aeInstanceId.c_str(), previousActive, currentAcked);
                                        } else {
                                            // New branch
                                            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                       "Branch '%s' is NEW, previousActive=0, Acked=%d (from MQTT payload)", 
                                                       aeInstanceId.c_str(), currentAcked);
                                        }
                                    }
                                    
                                    // ============================================================
                                    // STEP 2: Update g_branchStates memory
                                    // ============================================================
                                    UA_DateTime now = UA_DateTime_now();
                                    UA_StatusCode qualityCode = (quality == "Good") ? UA_STATUSCODE_GOOD : UA_STATUSCODE_BAD;
                                    
                                    // Treat AEInstanceID = 0 as aggregate (skip branch state update)
                                    if(!aeInstanceId.empty() && aeInstanceId != "null" && aeInstanceId != "NULL" && aeInstanceId != "{}" && aeInstanceId != "0") {
                                        auto &branchStateMap = g_branchStates[mapping.alarmKey];
                                        BranchState &branchState = branchStateMap[aeInstanceId];
                                        
                                        // Clear EventIds on state transition
                                        bool isStateTransition = (branchState.active != active) || (branchState.acked != currentAcked);
                                        if(isStateTransition && !branchState.eventIds.empty()) {
                                            branchState.clearEventIds();
                                        }
                                        
                                        // Update all branch state fields
                                        branchState.active = active;
                                        branchState.acked = currentAcked;
                                        branchState.confirmed = currentConfirmed;
                                        branchState.severity = severity;
                                        branchState.message = alarmMessage;
                                        branchState.time = now;
                                        branchState.receiveTime = now;
                                        branchState.retain = retain;  // Use retain directly from payload
                                        branchState.quality = qualityCode;
                                        
                                        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                   "Updated g_branchStates['%s']: active=%d, acked=%d, retain=%d", 
                                                   aeInstanceId.c_str(), active, currentAcked, retain);
                                    }

                                    
                                    // Variables for MQTT echo-back (declared here for wider scope)
                                    std::string branchEventIdHex = "";
                                    UA_StatusCode branchTriggerStatus = UA_STATUSCODE_GOOD;

                                    // ============================================================
                                    // STEP 3: HIJACK NODE & TRIGGER BRANCH EVENT
                                    // ============================================================
                                    // Set ALL properties on the condition node before triggering
                                    {
                                        UA_Variant v;
                                        UA_Variant_init(&v);
                                        
                                        // ActiveState/Id (STEALTH MODE)
                                        UA_Boolean bAct = active ? UA_TRUE : UA_FALSE;
                                        setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &bAct, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                       
                                        // ActiveState (LocalizedText) - use stealth write
                                        UA_LocalizedText actText = active ? 
                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Active") :
                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Inactive");
                                        setStealthValueChecked(server, alarmId, "ActiveState", &actText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                        
                                        // AckedState/Id (STEALTH MODE)
                                        UA_Boolean bAck = currentAcked ? UA_TRUE : UA_FALSE;
                                        setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &bAck, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                        
                                        // AckedState (LocalizedText) - use stealth write
                                        UA_LocalizedText ackText = currentAcked ? 
                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Acknowledged") :
                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Unacknowledged");
                                        setStealthValueChecked(server, alarmId, "AckedState", &ackText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                        
                                        // ConfirmedState/Id (STEALTH MODE - default confirmed for MQTT alarms)
                                        UA_Boolean bConf = currentConfirmed ? UA_TRUE : UA_FALSE;
                                        setStealthValueByPath(server, alarmId, {"ConfirmedState", "Id"}, &bConf, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                        
                                        // Severity - use stealth write
                                        setStealthValueChecked(server, alarmId, "Severity", &severity, &UA_TYPES[UA_TYPES_UINT16]);
                                        
                                        // Message - use stealth write
                                        UA_LocalizedText message = UA_LOCALIZEDTEXT((char*)"en-US", 
                                                                                   const_cast<char*>(alarmMessage.c_str()));
                                        setStealthValueChecked(server, alarmId, "Message", &message, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                        
                                        // Retain - use stealth write
                                        UA_Boolean ret = retain ? UA_TRUE : UA_FALSE;
                                        setStealthValueChecked(server, alarmId, "Retain", &ret, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                        
                                        // Time - use stealth write
                                        setStealthValueChecked(server, alarmId, "Time", &now, &UA_TYPES[UA_TYPES_DATETIME]);
                                        
                                        // ReceiveTime - use stealth write
                                        setStealthValueChecked(server, alarmId, "ReceiveTime", &now, &UA_TYPES[UA_TYPES_DATETIME]);
                                        
                                        // Quality - use stealth write
                                        setStealthValueChecked(server, alarmId, "Quality", &qualityCode, &UA_TYPES[UA_TYPES_STATUSCODE]);
                                        
                                        // Comment - use stealth write (if provided in payload)
                                        if(!comment.empty()) {
                                            UA_LocalizedText commentText = UA_LOCALIZEDTEXT((char*)"en-US", 
                                                                                           const_cast<char*>(comment.c_str()));
                                            setStealthValueChecked(server, alarmId, "Comment", &commentText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                       "✓ Comment set: '%s'", comment.c_str());
                                        }
                                        
                                        // BranchId (critical for client matching) - use stealth write
                                        // Only set BranchId if not aggregate (0 = aggregate)
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
                                        
                                        // DEBUG LOG: Show what we're about to trigger
                                        if(!aeInstanceId.empty() && aeInstanceId != "0") {
                                            auto &branchMap = g_alarmBranches[mapping.alarmKey];
                                            auto branchIt = branchMap.find(aeInstanceId);
                                            if(branchIt != branchMap.end()) {
                                                UA_String branchIdStr = UA_STRING_NULL;
                                                UA_NodeId_print(&branchIt->second.branchNodeId, &branchIdStr);
                                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                           "MQTT: Triggering branch event - GUID='%s', BranchId=%.*s, ACTIVE=%d, ACKED=%d, severity=%u",
                                                           aeInstanceId.c_str(), (int)branchIdStr.length, branchIdStr.data, 
                                                           active, currentAcked, severity);
                                                UA_String_clear(&branchIdStr);
                                            }
                                        }
                                        
                                        // TRIGGER BRANCH EVENT
                                        UA_ByteString eventId = UA_BYTESTRING_NULL;
                                        UA_StatusCode triggerStatus = UA_Server_triggerConditionEvent(
                                            server, alarmId, sourceNode, &eventId);
                                        
                                        // Store EventId for acknowledgment
                                        if(triggerStatus == UA_STATUSCODE_GOOD && eventId.length > 0) {
                                            // Only store for branch events (not aggregate with AEInstanceID = 0)
                                            if(!aeInstanceId.empty() && aeInstanceId != "0") {
                                                g_branchStates[mapping.alarmKey][aeInstanceId].addEventId(&eventId);
                                                
                                                std::string hex = "";
                                                for(size_t i = 0; i < eventId.length; i++) {
                                                    char buf[4];
                                                    sprintf(buf, "%02X", eventId.data[i]);
                                                    hex += buf;
                                                }
                                                UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                           "✓ Stored EventId for GUID '%s': %s", 
                                                           aeInstanceId.c_str(), hex.c_str());
                                            }
                                        }
                                        
                                        // Store eventId hex string and triggerStatus for MQTT echo
                                        branchTriggerStatus = triggerStatus;
                                        if(eventId.length > 0 && eventId.data != nullptr) {
                                            char hexBuf[3];
                                            for(size_t i = 0; i < eventId.length; i++) {
                                                sprintf(hexBuf, "%02X", eventId.data[i]);
                                                branchEventIdHex += hexBuf;
                                            }
                                        }
                                        
                                        UA_ByteString_clear(&eventId);
                                    }

                                    // ============================================================
                                    // STEP 4: RESTORE AGGREGATE & TRIGGER AGGREGATE EVENT
                                    // ============================================================
                                    // Only calculate aggregate if this was a branch event (not aggregate with AEInstanceID = 0)
                                    if(!aeInstanceId.empty() && aeInstanceId != "null" && aeInstanceId != "NULL" && aeInstanceId != "{}" && aeInstanceId != "0") {
                                        // Calculate aggregate state from ALL branches
                                        bool aggActive = false;
                                        bool aggAcked = true;
                                        bool aggConfirmed = true;
                                        UA_UInt16 aggSeverity = 0;
                                        bool aggRetain = false;
                                        
                                        auto &branchStateMap = g_branchStates[mapping.alarmKey];
                                        for(const auto &branchPair : branchStateMap) {
                                            // Active: OR logic
                                            if(branchPair.second.active) {
                                                aggActive = true;
                                                if(branchPair.second.severity > aggSeverity) {
                                                    aggSeverity = branchPair.second.severity;
                                                }
                                            }
                                            
                                            // Acked/Confirmed: AND logic (only for retained branches)
                                            if(branchPair.second.retain) {
                                                if(!branchPair.second.acked) aggAcked = false;
                                                if(!branchPair.second.confirmed) aggConfirmed = false;
                                                aggRetain = true;  // At least one branch is retained
                                            }
                                        }
                                        
                                        // Update aggregate on condition node
                                        UA_Variant v;
                                        UA_Variant_init(&v);
                                        
                                        // ActiveState (STEALTH MODE)
                                        UA_Boolean valActive = aggActive ? UA_TRUE : UA_FALSE;
                                        setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &valActive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                        
                                        // ActiveState (LocalizedText) - use stealth write
                                        UA_LocalizedText actText = aggActive ? 
                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Active") :
                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Inactive");
                                        setStealthValueChecked(server, alarmId, "ActiveState", &actText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                        
                                        // AckedState (STEALTH MODE)
                                        UA_Boolean valAcked = aggAcked ? UA_TRUE : UA_FALSE;
                                        setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &valAcked, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                        
                                        // AckedState (LocalizedText) - use stealth write
                                        UA_LocalizedText ackText = aggAcked ? 
                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Acknowledged") :
                                            UA_LOCALIZEDTEXT((char*)"en", (char*)"Unacknowledged");
                                        setStealthValueChecked(server, alarmId, "AckedState", &ackText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                                        
                                        // ConfirmedState (STEALTH MODE)
                                        UA_Boolean valConf = aggConfirmed ? UA_TRUE : UA_FALSE;
                                        setStealthValueByPath(server, alarmId, {"ConfirmedState", "Id"}, &valConf, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                        
                                        // Severity (max from all active branches) - use stealth write
                                        setStealthValueChecked(server, alarmId, "Severity", &aggSeverity, &UA_TYPES[UA_TYPES_UINT16]);
                                        
                                        // Retain - use stealth write
                                        UA_Boolean retVal = aggRetain ? UA_TRUE : UA_FALSE;
                                        setStealthValueChecked(server, alarmId, "Retain", &retVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                        
                                        // BranchId = NULL for aggregate - use stealth write
                                        UA_NodeId nullId = UA_NODEID_NULL;
                                        setStealthValueChecked(server, alarmId, "BranchId", &nullId, &UA_TYPES[UA_TYPES_NODEID]);
                                        
                                        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                   "✓ Updated main condition aggregate state: Active=%d, Acked=%d, Confirmed=%d, Severity=%u, Retain=%d",
                                                   aggActive, aggAcked, aggConfirmed, aggSeverity, aggRetain);
                                        
                                        // TRIGGER AGGREGATE EVENT
                                        std::string emitterName = mapping.alarmKey.substr(0, mapping.alarmKey.find("-"));
                                        UA_NodeId sourceNode = alarmId;
                                        auto nodeIt = nodeMap.find(emitterName);
                                        if(nodeIt != nodeMap.end()) {
                                            sourceNode = nodeIt->second;
                                        }
                                        
                                        UA_ByteString aggEventId = UA_BYTESTRING_NULL;
                                        UA_StatusCode aggTrigger = UA_Server_triggerConditionEvent(
                                            server, alarmId, sourceNode, &aggEventId);
                                        
                                        if(aggTrigger == UA_STATUSCODE_GOOD) {
                                            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                       "✓ Triggered AGGREGATE update event (BranchId=NULL) from MQTT update");
                                        }
                                        
                                        UA_ByteString_clear(&aggEventId);
                                    }
                                    
                                    // // ============================================================
                                    // // STEP 5: MQTT ECHO-BACK
                                    // // ============================================================
                                    // // Extract alarm name from alarm key (everything after the last '-')
                                    // std::string AlarmName;
                                    // size_t dashPos = mapping.alarmKey.find_last_of('-');
                                    // if(dashPos != std::string::npos && dashPos + 1 < mapping.alarmKey.length()) {
                                    //     AlarmName = mapping.alarmKey.substr(dashPos + 1);
                                    // } else {
                                    //     AlarmName = mapping.alarmKey;
                                    // }
                                    
                                    // if(branchTriggerStatus == UA_STATUSCODE_GOOD) {
                                    //     UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                    //                "✓ Successfully triggered alarm condition for '%s' (active=%d, enabled=%d, acked=%d)",
                                    //                mapping.alarmKey.c_str(), active, enabled, currentAcked);
                                        
                                    //     // Echo back to .alarm.sub topic
                                    //     std::string subTopic = baseTopic + ".alarm.sub";
                                        
                                    //     try {
                                    //         // Get the actual OPC UA NodeId string
                                    //         std::string alarmNodeIdStr = formatNodeId(&alarmId);
                                            
                                    //         json echoPayload;
                                    //         echoPayload["Event"] = {
                                    //             {"Id", alarmNodeIdStr},  // OPC UA Condition/Branch NodeId
                                    //             {"Name", AlarmName},
                                    //             {"EventId", branchEventIdHex},
                                    //             {"AETypeID", mapping.alarmId},
                                    //             {"AEInstanceID", aeInstanceId},  // GUID for branch identification
                                    //             {"timestamp", timestamp},
                                    //             {"severity", severity},
                                    //             {"active", active},
                                    //             {"enabled", enabled},
                                    //             {"retain", finalRetain},
                                    //             {"quality", quality},
                                    //             {"shelved", shelved},
                                    //             {"alarm_message", alarmMessage},
                                    //             {"status", "triggered"},
                                    //             {"acked", currentAcked}
                                    //         };
                                            
                                    //         std::string echoMsg = echoPayload.dump();
                                    //         UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                    //                    "  → Publishing to '%s': %s",
                                    //                    subTopic.c_str(), echoMsg.c_str());
                                            
                                    //         publish_to_mqtt(subTopic, echoMsg);
                                            
                                    //         UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                    //                    "  → Echo publish call completed for '%s'", 
                                    //                    subTopic.c_str());
                                    //     } catch(const std::exception &e) {
                                    //         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                    //                     "Failed to echo to .alarm.sub: %s", e.what());
                                    //     }
                                    // } else {
                                    //     UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                    //                  "✗ Failed to trigger alarm condition for '%s': %s (active=%d, enabled=%d, acked=%d)",
                                    //                  mapping.alarmKey.c_str(),
                                    //                  UA_StatusCode_name(branchTriggerStatus),
                                    //                  active, enabled, currentAcked);
                                    // }
                                    
                                    // Cleanup branches after processing
                                    cleanupBranches(mapping.alarmKey);


                                                // CRITICAL: Reset BranchId to NULL on the Condition Node
                                                if(!UA_NodeId_isNull(&alarmId)) {
                                                     UA_NodeId nullId = UA_NODEID_NULL;
                                                     UA_Server_writeObjectProperty_scalar(server, alarmId, UA_QUALIFIEDNAME(0, (char*)"BranchId"), &nullId, &UA_TYPES[UA_TYPES_NODEID]);
                                                }
                                            }  // End of for loop
                                    } else {
                                        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                      "✗ 'Alarm' field NOT found in payload! Payload: '%s'",
                                                      payload.c_str());
                                    }
                                    } catch(const json::exception &e) {
                                        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                                                    "JSON parse error for trigger topic '%s': %s",
                                                    topic.c_str(), e.what());
                                    }
                                    
                                    // Skip the normal alarm processing for trigger topics
                                    return;
                                }
                                
                                /* Regular data update path (non-alarm topics) */
                                auto it = nodeMap.find(topic);
                                if(it != nodeMap.end()) {
                                    try {
                                        auto j = json::parse(payload);
                                        if(j.contains("Data") && j["Data"].is_array() &&
                                           !j["Data"].empty()) {
                                            if(j["Data"][0]["Value"].is_number()) {
                                                double value =
                                                    j["Data"][0]["Value"].get<double>();
                                                UA_Variant var;
                                                UA_Variant_setScalar(
                                                    &var, &value,
                                                    &UA_TYPES[UA_TYPES_DOUBLE]);
                                                is_internal_write = true;
                                                UA_Server_writeValue(server, it->second,
                                                                     var);
                                                is_internal_write = false;
                                            } else if(j["Data"][0]["Value"]
                                                          .is_boolean()) {
                                                UA_Boolean value =
                                                    j["Data"][0]["Value"].get<bool>();
                                                UA_Variant var;
                                                UA_Variant_setScalar(
                                                    &var, &value,
                                                    &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                is_internal_write = true;
                                                UA_Server_writeValue(server, it->second,
                                                                     var);
                                                is_internal_write = false;
                                            } else if(j["Data"][0]["Value"].is_string()) {
                                                std::string strValue =
                                                    j["Data"][0]["Value"]
                                                        .get<std::string>();
                                                UA_String value =
                                                    UA_STRING_ALLOC(strValue.c_str());
                                                UA_Variant var;
                                                UA_Variant_setScalar(
                                                    &var, &value,
                                                    &UA_TYPES[UA_TYPES_STRING]);
                                                is_internal_write = true;
                                                UA_Server_writeValue(server, it->second,
                                                                     var);
                                                is_internal_write = false;
                                                UA_String_clear(&value);
                                            }
                                        }
                                    } catch(const std::exception &e) {
                                        log("JSON parse error: " + std::string(e.what()),
                                            LogLevel::ERRORS);
                                    }
                                }
                            },
                            [](auto &) {}  // Ignore other packet types
                        });
                    }
                } catch(const std::exception &e) {
                    log("MQTT error: " + std::string(e.what()), LogLevel::ERRORS);
                    g_mqtt_connected.store(false);
                }
                
                // Recreate client before reconnecting
                log("Recreating MQTT client...", LogLevel::INFO);
                amcl = client_t{ioc.get_executor()};
                
                // Wait 2 seconds before reconnecting
                log("Waiting 2 seconds before reconnect...", LogLevel::INFO);
                as::steady_timer timer(ioc);
                timer.expires_after(std::chrono::seconds(2));
                co_await timer.async_wait(as::use_awaitable);
                // Reconnection will happen at the start of the while loop
            }
            log("MQTT reconnection loop stopped due to server shutdown.", LogLevel::INFO);
            co_return;
        },
        as::detached);
}



int
main(int argc, char **argv) {
    // Early console output before logging is initialized
    std::cout << "==================================================" << std::endl;
    std::cout << "OPC UA Server Starting..." << std::endl;
    std::cout << "==================================================" << std::endl;

    // Load configuration from appsettings.json
    std::cout << "Loading configuration from appsettings.json..." << std::endl;
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

    // Acquire bearer token for API authentication
    std::string BearerToken;
    try {
        std::cout << "Acquiring bearer token for API authentication..." << std::endl;
        log("Acquiring bearer token for API authentication...", LogLevel::INFO);
        json authResponse = getBearerToken(applicationEndURLHost, 
                                           std::to_string(applicationEndURLPort),
                                           authUsername, authPassword);
        
        if(authResponse.contains("access_token")) {
            BearerToken = authResponse["access_token"].get<std::string>();
            std::cout << "✓ Bearer token acquired successfully" << std::endl;
            log("Bearer token acquired successfully", LogLevel::INFO);
        } else {
            std::cerr << "ERROR: Bearer token response missing 'access_token' field" << std::endl;
            log("Bearer token response missing 'access_token' field", LogLevel::ERRORS);
            log("Response: " + authResponse.dump(), LogLevel::DEBUG);
            std::cerr << "Press Enter to exit..." << std::endl;
            std::cin.get();
            return EXIT_FAILURE;
        }
    } catch(const std::exception& e) {
        std::cerr << "ERROR: Failed to acquire bearer token: " << e.what() << std::endl;
        log("Failed to acquire bearer token: " + std::string(e.what()), LogLevel::ERRORS);
        std::cerr << "Press Enter to exit..." << std::endl;
        std::cin.get();
        return EXIT_FAILURE;
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
        "orgId": 0,
        "roleId": "",
        "userId": 0,
        "moduleId": 0,
        "userType": "",
        "requestDateTime": "2024-12-26T08:16:05.629Z",
        "ipAddress": "",
        "originName": "",
        "filterModel": {
        "pageSize": 10,
        "totalRows": 0,
        "currentPage": 1,
        "searchText": "",
        "filterRowsCount": 0,
        "orderType": "A",
        "orderBy": "id"
        }
    }
    )";

    std::string target = "/api/GetAllOrganizationList";

    vector<OrgConfig> orgs;
    try {
        log("Fetching organization list from API...", LogLevel::INFO);
        orgs = ParseOrgConfig(applicationEndURLHost, std::to_string(applicationEndURLPort), 
                             BearerToken, json_body, target);
        log("Successfully fetched " + std::to_string(orgs.size()) + " organizations", LogLevel::INFO);
    } catch(const std::exception& e) {
        log("Failed to fetch organization list: " + std::string(e.what()), LogLevel::ERRORS);
        log("Server will start without multi-tenancy support", LogLevel::INFO);
        // Continue with empty org list - server will run but without multi-tenant endpoints
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
        config, 53531, &certificate, &privateKey, trustList, trustListSize, issuerList,
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

    // Add historizing configuration
    g_gathering = (UA_HistoryDataGathering *)UA_malloc(sizeof(UA_HistoryDataGathering));
    *g_gathering = UA_HistoryDataGathering_Default(1);
    config->historyDatabase = UA_HistoryDatabase_default(*g_gathering);

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                "Historizing configuration initialized");
    log("Historizing configuration initialized successfully", LogLevel::INFO);

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
                            UA_Variant_setScalar(&rangeVariant, &range,
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
                                NULL, NULL);

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
                                UA_Variant_setScalar(&alarmVariant, &value,
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
                                // UA_Variant_clear(&alarmVariant);
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
                            UA_Variant_setScalar(&unitVariant, &unit,
                                                 &UA_TYPES[UA_TYPES_STRING]);

                            UA_VariableAttributes unitAttr =
                                UA_VariableAttributes_default;
                            unitAttr.displayName =
                                UA_LOCALIZEDTEXT_ALLOC("en-US", "Engineering Unit");
                            unitAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                            UA_Variant_copy(&unitVariant, &unitAttr.value);

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
                                UA_Variant_setScalar(&deadBandVariant, &deadBand,
                                                     &UA_TYPES[UA_TYPES_DOUBLE]);

                                UA_VariableAttributes deadBandAttr =
                                    UA_VariableAttributes_default;
                                deadBandAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", "DeadBand");
                                deadBandAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                UA_Variant_copy(&deadBandVariant, &deadBandAttr.value);

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
                                UA_Variant_setScalar(&precisionVariant, &precision,
                                                     &UA_TYPES[UA_TYPES_STRING]);

                                UA_VariableAttributes precisionAttr =
                                    UA_VariableAttributes_default;
                                precisionAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", "Precision Type");
                                precisionAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                UA_Variant_copy(&precisionVariant, &precisionAttr.value);

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
                                UA_Variant_setScalar(&parameterGroupVariant,
                                                     &parameterGroup,
                                                     &UA_TYPES[UA_TYPES_STRING]);

                                UA_VariableAttributes parameterGroupAttr =
                                    UA_VariableAttributes_default;
                                parameterGroupAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", "Parameter Group");
                                parameterGroupAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                UA_Variant_copy(&parameterGroupVariant,
                                                &parameterGroupAttr.value);

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
                                UA_Variant_setScalar(&tagTypeVariant, &tagType,
                                                     &UA_TYPES[UA_TYPES_STRING]);

                                UA_VariableAttributes tagTypeAttr =
                                    UA_VariableAttributes_default;
                                tagTypeAttr.displayName =
                                    UA_LOCALIZEDTEXT_ALLOC("en-US", "Tag Type");
                                tagTypeAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                                UA_Variant_copy(&tagTypeVariant, &tagTypeAttr.value);

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
            std::string triggerTopic = baseTriggerTopic + ".event";
            
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

    std::thread mqtt_thread([&]() { ioc.run(); });
    mqtt_thread.detach();

    // string bearerToken = getBearerToken();
    // json topicList = getTopicList(bearerToken);

  

    log("Starting OPC UA Server...", LogLevel::INFO);
    log("Added repeated callback for counter updates", LogLevel::DEBUG);

    UA_Server_run_startup(server);
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

    try {
        while(running) {
            UA_Server_run_iterate(server, true);
            // log("Main loop iteration...", LogLevel::DEBUG); // Too verbose
        }
    } catch (const std::exception& e) {
        log("🔥 CRITICAL: Unhandled exception in main loop: " + std::string(e.what()), LogLevel::ERRORS);
        std::cerr << "CRITICAL: Unhandled exception: " << e.what() << std::endl;
    } catch (...) {
        log("🔥 CRITICAL: Unknown exception in main loop", LogLevel::ERRORS);
        std::cerr << "CRITICAL: Unknown exception in main loop" << std::endl;
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

// Build an OPC UA Server that dynamically updates its address space using data received
// via MQTT, which in turn is sourced from a database.
