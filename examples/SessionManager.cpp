#include "SessionManager.h"
#include "Logger.h"
#include "fetchAPI.h"
#include <sstream>
#include <iomanip>

// Forward declaration of worker thread function
// Forward declaration of worker thread function
void sessionWorkerThread(std::shared_ptr<SessionContext> ctx, UA_Server* server,
                        const std::string& bearerToken,
                        const std::string& apiHost, const std::string& apiPort);

// Destructor - cleanup all sessions
SessionManager::~SessionManager() {
    shutdown();
}

// Explicit shutdown
void SessionManager::shutdown() {
    std::lock_guard<std::mutex> lock(managerMutex);
    
    if(sessions.empty()) return; // Already shut down

    for(auto& sessionPair : sessions) {
        if(sessionPair.second) {
            sessionPair.second->shouldStop.store(true);
            // Also notify condition variable to wake up valid contexts
            sessionPair.second->cv.notify_all();

            if(sessionPair.second->workerThread.joinable()) {
                sessionPair.second->workerThread.join();
            }
        }
    }
    sessions.clear();
}

// Initialize endpoint mappings from organization configs
void SessionManager::initialize(const std::vector<OrgConfig>& orgs) {
    std::lock_guard<std::mutex> lock(managerMutex);
    
    // Store organizations for authentication-based routing
    organizations = orgs;
    
    // Clear legacy endpoint mappings (not used in auth-based routing)
    endpointMappings.clear();
}

// Convert UA_NodeId to string for use as map key
std::string SessionManager::formatNodeId(const UA_NodeId* nodeId) const {
    if(!nodeId) return "";
    
    std::ostringstream oss;
    oss << "ns=" << nodeId->namespaceIndex << ";";
    
    switch(nodeId->identifierType) {
        case UA_NODEIDTYPE_NUMERIC:
            oss << "i=" << nodeId->identifier.numeric;
            break;
        case UA_NODEIDTYPE_STRING:
            oss << "s=" << std::string((char*)nodeId->identifier.string.data,
                                      nodeId->identifier.string.length);
            break;
        case UA_NODEIDTYPE_GUID: {
            const UA_Guid* guid = &nodeId->identifier.guid;
            oss << "g=" << std::hex << std::setfill('0')
                << std::setw(8) << guid->data1 << "-"
                << std::setw(4) << guid->data2 << "-"
                << std::setw(4) << guid->data3 << "-";
            for(int i = 0; i < 8; i++) {
                oss << std::setw(2) << (int)guid->data4[i];
            }
            break;
        }
        case UA_NODEIDTYPE_BYTESTRING:
            oss << "b=<bytestring>";
            break;
    }
    
    return oss.str();
}

// Register new session and spawn worker thread
bool SessionManager::registerSession(const UA_NodeId& sessionId,
                                     const std::string& shortCode,
                                     UA_Server* server,
                                     const std::string& bearerToken,
                                     const std::string& apiHost,
                                     const std::string& apiPort) {
    std::lock_guard<std::mutex> lock(managerMutex);
    
    // Find organization by shortCode from stored list
    const OrgConfig* orgConfig = nullptr;
    for(const auto& org : organizations) {
        if(org.shortCode == shortCode) {
            orgConfig = &org;
            break;
        }
    }
    
    if(!orgConfig) {
        log("Unknown organization ShortCode: " + shortCode, LogLevel::ERRORS);
        return false;
    }
    
    // Convert session ID to key
    std::string sessionKey = formatNodeId(&sessionId);
    
    // Create session context
    auto ctx = std::make_shared<SessionContext>();
    ctx->sessionKey = sessionKey;
    ctx->shortCode = shortCode;
    ctx->orgId = orgConfig->orgId;
    ctx->namespaceUri = "Anexee:" + orgConfig->shortCode;
    ctx->shouldStop.store(false);
    
    // Synchronously register namespace to avoid race conditions with Access Control
    // This ensures ctx->namespaceIndex can be resolved immediately by CheckAccess
    ctx->namespaceIndex = UA_Server_addNamespace(server, ctx->namespaceUri.c_str());
    log("✓ Registered namespace '" + ctx->namespaceUri + "' at index " + std::to_string(ctx->namespaceIndex), LogLevel::INFO);
    
    log("🧵 Creating dedicated worker thread for org '" + shortCode + "' (OrgID: " + 
        std::to_string(orgConfig->orgId) + ")", LogLevel::INFO);
    
    // Spawn dedicated worker thread
    // Keep a weak_ptr or just pass shared_ptr (worker holds reference)
    ctx->workerThread = std::thread(sessionWorkerThread, ctx, server,
                                    bearerToken, apiHost, apiPort);
    
    // Store context
    sessions[sessionKey] = std::move(ctx);
    
    // Convert thread ID to string using ostringstream
    std::ostringstream threadIdStream;
    threadIdStream << sessions[sessionKey]->workerThread.get_id();
    
    log("✓ Session registered and worker thread started for org '" + shortCode + 
        "' (OrgID: " + std::to_string(orgConfig->orgId) + 
        ", Thread ID: " + threadIdStream.str() + ")", 
        LogLevel::INFO);
    
    return true;
}

// Unregister session and cleanup
// Unregister session and cleanup
void SessionManager::unregisterSession(const UA_NodeId& sessionId) {
    std::shared_ptr<SessionContext> ctx_ptr;
    std::string shortCode;

    {
        std::lock_guard<std::mutex> lock(managerMutex);
        
        std::string sessionKey = formatNodeId(&sessionId);
        auto it = sessions.find(sessionKey);
        
        if(it != sessions.end()) {
            // Take ownership of the context locally
            ctx_ptr = std::move(it->second);
            // Remove from map immediately
            sessions.erase(it);
            
            if(ctx_ptr) {
                shortCode = ctx_ptr->shortCode;
                log(" Closing session for org '" + shortCode + "' (OrgID: " + 
                    std::to_string(ctx_ptr->orgId) + ")", LogLevel::INFO);
            }
        }
    } // Unlock managerMutex here

    // Perform cleanup without holding the lock
    if(ctx_ptr) {
        // Signal worker thread to stop
        ctx_ptr->shouldStop.store(true);
        ctx_ptr->cv.notify_all(); // Wake up worker thread immediately
        
        log("⏳ Waiting for worker thread to terminate...", LogLevel::DEBUG);
        
        // Wait for thread to finish
        if(ctx_ptr->workerThread.joinable()) {
            ctx_ptr->workerThread.join();
            log("✓ Worker thread terminated successfully", LogLevel::INFO);
        }
        
        log("✓ Session closed and resources cleaned up for org '" + 
            shortCode + "'", LogLevel::INFO);
        
        // ctx_ptr destructor runs here, freeing the memory
    }
}

// Get session context (thread-safe, shared ownership)
std::shared_ptr<SessionContext> SessionManager::getSession(const UA_NodeId& sessionId) {
    std::lock_guard<std::mutex> lock(managerMutex);
    
    std::string sessionKey = formatNodeId(&sessionId);
    auto it = sessions.find(sessionKey);
    
    if(it != sessions.end()) {
        return it->second;
    }
    
    return nullptr;
}

// Get active session count
size_t SessionManager::getActiveSessionCount() const {
    std::lock_guard<std::mutex> lock(managerMutex);
    return sessions.size();
}

// Validate shortCode
bool SessionManager::isValidShortCode(const std::string& shortCode) const {
    std::lock_guard<std::mutex> lock(managerMutex);
    for(const auto& org : organizations) {
        if(org.shortCode == shortCode) return true;
    }
    return false;
}

// ============================================================================
// SessionContext Implementation (Cleanup)
// ============================================================================
extern std::unordered_map<std::string, UA_NodeId> g_alarmByKey;
extern std::mutex g_alarmMutex;

SessionContext::~SessionContext() {
    // 1. Stop worker if running
    shouldStop = true;
    cv.notify_all();
    if (workerThread.joinable()) {
        try {
            workerThread.join();
        } catch(...) {}
    }

    // 2. Remove Alarms from Global Map (Thread-Safe)
    if(!alarmMap.empty()) {
        std::lock_guard<std::mutex> lock(g_alarmMutex);
        for(const auto& pair : alarmMap) {
            // Only remove if it exists (safe check)
            // DISABLED: User requested to keep address space persistent across reconnections
            // g_alarmByKey.erase(pair.first); 
        }
    }

    // 3. Clear Local Maps (Memory Cleanup)
    for(auto& pair : alarmMap) {
        UA_NodeId_clear(&pair.second);
    }
    alarmMap.clear();

    for(auto& pair : nodeMap) {
        UA_NodeId_clear(&pair.second);
    }
    nodeMap.clear();
}
