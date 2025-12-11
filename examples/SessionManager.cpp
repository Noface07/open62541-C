#include "SessionManager.h"
#include "Logger.h"
#include "fetchAPI.h"
#include <sstream>
#include <iomanip>

// Forward declaration of worker thread function
void sessionWorkerThread(SessionContext* ctx, UA_Server* server,
                        const std::string& bearerToken,
                        const std::string& apiHost, const std::string& apiPort);

// Destructor - cleanup all sessions
SessionManager::~SessionManager() {
    std::lock_guard<std::mutex> lock(managerMutex);
    
    for(auto& sessionPair : sessions) {
        if(sessionPair.second) {
            sessionPair.second->shouldStop.store(true);
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
    auto ctx = std::make_unique<SessionContext>();
    ctx->sessionKey = sessionKey;
    ctx->shortCode = shortCode;
    ctx->orgId = orgConfig->orgId;
    ctx->namespaceUri = "anexee:" + orgConfig->shortCode;
    ctx->shouldStop.store(false);
    
    log("🧵 Creating dedicated worker thread for org '" + shortCode + "' (OrgID: " + 
        std::to_string(orgConfig->orgId) + ")", LogLevel::INFO);
    
    // Spawn dedicated worker thread
    ctx->workerThread = std::thread(sessionWorkerThread, ctx.get(), server,
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
void SessionManager::unregisterSession(const UA_NodeId& sessionId) {
    std::lock_guard<std::mutex> lock(managerMutex);
    
    std::string sessionKey = formatNodeId(&sessionId);
    auto it = sessions.find(sessionKey);
    
    if(it != sessions.end()) {
        auto& ctx = it->second;
        
        log("🛑 Closing session for org '" + ctx->shortCode + "' (OrgID: " + 
            std::to_string(ctx->orgId) + ")", LogLevel::INFO);
        
        // Signal worker thread to stop
        ctx->shouldStop.store(true);
        
        log("⏳ Waiting for worker thread to terminate...", LogLevel::DEBUG);
        
        // Wait for thread to finish
        if(ctx->workerThread.joinable()) {
            ctx->workerThread.join();
            log("✓ Worker thread terminated successfully", LogLevel::INFO);
        }
        
        // Save data for logging before destruction
        std::string shortCode = ctx->shortCode;
        
        // Remove from map (unique_ptr auto-deletes)
        sessions.erase(it);
        
        log("✓ Session closed and resources cleaned up for org '" + 
            shortCode + "'", LogLevel::INFO);
    }
}

// Get session context (thread-safe read)
SessionContext* SessionManager::getSession(const UA_NodeId& sessionId) {
    std::lock_guard<std::mutex> lock(managerMutex);
    
    std::string sessionKey = formatNodeId(&sessionId);
    auto it = sessions.find(sessionKey);
    
    return (it != sessions.end()) ? it->second.get() : nullptr;
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
