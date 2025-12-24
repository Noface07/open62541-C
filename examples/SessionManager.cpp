#include "SessionManager.h"
#include "Logger.h"
#include "fetchAPI.h"
#include <sstream>
#include <iomanip>
#include "InstrumentedMutex.cpp"
#include "AandC.h"

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
    
    // DEDUPLICATION: Check if worker already exists for this Org
    std::shared_ptr<SessionContext> ctx;
    bool reused = false;
    
    if(activeWorkers.find(orgConfig->orgId) != activeWorkers.end()) {
        ctx = activeWorkers[orgConfig->orgId].lock();
        if(ctx) {
            reused = true;
            log("♻️ Reusing active worker for org '" + shortCode + "' (OrgID: " + 
                std::to_string(orgConfig->orgId) + ") - Shared by multiple sessions", LogLevel::INFO);
        }
    }
    
    if(!ctx) {
        // Create NEW session context
        ctx = std::make_shared<SessionContext>();
        ctx->sessionKey = sessionKey; // Note: Primary key. Shared contexts might have multiple keys logic? 
                                      // Actually context sessionKey field is less relevant if shared.
        ctx->shortCode = shortCode;
        ctx->orgId = orgConfig->orgId;
        ctx->namespaceUri = "Anexee:" + orgConfig->shortCode;
        ctx->shouldStop.store(false);
        
        // Synchronously register namespace 
        ctx->namespaceIndex = UA_Server_addNamespace(server, ctx->namespaceUri.c_str());
        log("✓ Registered namespace '" + ctx->namespaceUri + "' at index " + std::to_string(ctx->namespaceIndex), LogLevel::INFO);
        
        log("🧵 Creating dedicated worker thread for org '" + shortCode + "' (OrgID: " + 
            std::to_string(orgConfig->orgId) + ")", LogLevel::INFO);
        
        // Spawn dedicated worker thread
        ctx->workerThread = std::thread(sessionWorkerThread, ctx, server,
                                        bearerToken, apiHost, apiPort);
                                        
        // Register in activeWorkers
        activeWorkers[orgConfig->orgId] = ctx;
    }
    
    // Store context in sessions map (Increments refCount)
    sessions[sessionKey] = ctx;
    
    if(!reused) {
        // Convert thread ID to string using ostringstream
        std::ostringstream threadIdStream;
        threadIdStream << ctx->workerThread.get_id();
        
        log("✓ Session registered and worker thread started for org '" + shortCode + 
            "' (OrgID: " + std::to_string(orgConfig->orgId) + 
            ", Thread ID: " + threadIdStream.str() + ")", 
            LogLevel::INFO);
    }
    
    return true;
}

// Unregister session and cleanup
void SessionManager::unregisterSession(const UA_NodeId& sessionId) {
    std::shared_ptr<SessionContext> ctx_ptr;
    std::string shortCode;

    {
        std::lock_guard<std::mutex> lock(managerMutex);
        
        std::string sessionKey = formatNodeId(&sessionId);
        auto it = sessions.find(sessionKey);
        
        if(it != sessions.end()) {
            ctx_ptr = std::move(it->second); // Take local ownership
            sessions.erase(it);              // Decrement global refCount
            
            if(ctx_ptr) {
                shortCode = ctx_ptr->shortCode;
                // log("ℹ️ Unregistering session for org '" + shortCode + "'", LogLevel::DEBUG);
            }
        }
    } // Unlock managerMutex

    // RAII Cleanup:
    // If 'sessions' held the last shared_ptr, ctx_ptr is now the last owner.
    // When ctx_ptr goes out of scope (end of function), destructor is called.
    // Destructor stops the thread.
    // If activeWorkers still holds weak_ptr, it doesn't count towards ownership.
    // If another session shares this context, refCount > 1, so destructor is NOT called.
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
//extern std::mutex g_alarmMutex;

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
        InstrumentedGuard lock(g_alarmMutex);
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
