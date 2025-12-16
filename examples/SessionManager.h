#pragma once

#include <string>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <map>
#include <open62541/server.h>
#include "OrgConfig.h"

// Configuration
static size_t g_maxConcurrentSessions = 0; // 0 = unlimited

// Mapping of endpoint path (ShortCode) to organization details
struct OrgEndpointMapping {
    std::string shortCode;
    int orgId;
    std::string namespaceUri; // Format: "anexee:{shortCode}"
};

// Per-session context - holds all org-specific resources
struct SessionContext {
    std::string shortCode;
    int orgId;
    std::string namespaceUri;
    UA_UInt16 namespaceIndex;
    
    // Dedicated worker thread
    std::thread workerThread;
    std::atomic<bool> shouldStop;
    
    // Thread synchronization for fast shutdown
    std::condition_variable cv;
    std::mutex cvMutex;

    // Org-specific resources
    std::vector<std::string> topics;
    std::map<std::string, UA_NodeId> nodeMap;
    std::unordered_map<std::string, UA_NodeId> alarmMap; // Key: emitterNodeName-alarmName
    
    // Session metadata
    std::string sessionKey;
    
    SessionContext() : shouldStop(false), namespaceIndex(0), orgId(0) {}
};


// Thread-safe session manager
class SessionManager {
public:
    SessionManager() = default;
    ~SessionManager();
    
    // Initialize with organization configurations
    void initialize(const std::vector<OrgConfig>& orgs);
    
    // Register new session (spawns worker thread)
    bool registerSession(const UA_NodeId& sessionId, const std::string& shortCode,
                        UA_Server* server, const std::string& bearerToken,
                        const std::string& apiHost, const std::string& apiPort);
    
    // Unregister session (stops worker and cleans up)
    void unregisterSession(const UA_NodeId& sessionId);
    
    // Get session context (thread-safe)
    SessionContext* getSession(const UA_NodeId& sessionId);
    
    // Get active session count
    size_t getActiveSessionCount() const;
    
    // Check if shortCode is valid
    bool isValidShortCode(const std::string& shortCode) const;
    
private:
    mutable std::mutex managerMutex;
    std::unordered_map<std::string, std::unique_ptr<SessionContext>> sessions;
    std::unordered_map<std::string, OrgEndpointMapping> endpointMappings;
    std::vector<OrgConfig> organizations;  // Store all organizations for auth-based routing
    
    // Helper: Convert UA_NodeId to string key
    std::string formatNodeId(const UA_NodeId* nodeId) const;
};
