#ifndef ALARM_HANDLER_H
#define ALARM_HANDLER_H

#include <open62541/server.h>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <nlohmann/json.hpp>
#include "InstrumentedMutex.cpp"

/**
 * @brief A custom comparator for using UA_NodeId as a key in std::map.
 *
 * The open62541 library provides UA_NodeId_order for this purpose, which
 * this struct wraps for easy use with standard C++ containers.
 */
struct UA_NodeId_less_than {
    bool operator()(const UA_NodeId& lhs, const UA_NodeId& rhs) const {
        return UA_NodeId_order(&lhs, &rhs) < 0;
    }
};

/**
 * @brief Custom Hash and Equality for UA_NodeId to be used in std::unordered_map
 */
struct UA_NodeId_Hasher {
    std::size_t operator()(const UA_NodeId& id) const {
        // Simple hash combining namespace and identifier
        std::size_t h1 = std::hash<UA_UInt16>{}(id.namespaceIndex);
        std::size_t h2 = 0;
        if(id.identifierType == UA_NODEIDTYPE_NUMERIC) {
            h2 = std::hash<UA_UInt32>{}(id.identifier.numeric);
        } else if(id.identifierType == UA_NODEIDTYPE_STRING) {
            h2 = std::hash<std::string>{}(std::string((char*)id.identifier.string.data, id.identifier.string.length));
        } else if(id.identifierType == UA_NODEIDTYPE_GUID) {
             // Basic hash for GUID (using data1)
             h2 = std::hash<UA_UInt32>{}(id.identifier.guid.data1);
        }
        return h1 ^ (h2 << 1); 
    }
};

struct UA_NodeId_KeyEqual {
    bool operator()(const UA_NodeId& lhs, const UA_NodeId& rhs) const {
        return UA_NodeId_equal(&lhs, &rhs);
    }
};

/**
 * @brief Cached NodeIds for an Alarm Condition to avoid repeated SDK lookups
 */
struct AlarmConditionCache {
    UA_NodeId sourceNodeId;
    UA_NodeId enabledStateNodeId;
    UA_NodeId conditionId; // The key
    
    AlarmConditionCache() {
        UA_NodeId_init(&sourceNodeId);
        UA_NodeId_init(&enabledStateNodeId);
        UA_NodeId_init(&conditionId);
    }
    
    ~AlarmConditionCache() {
        UA_NodeId_clear(&sourceNodeId);
        UA_NodeId_clear(&enabledStateNodeId);
        UA_NodeId_clear(&conditionId);
    }
    
    // Copy Constructor
    AlarmConditionCache(const AlarmConditionCache& other) {
        UA_NodeId_copy(&other.sourceNodeId, &sourceNodeId);
        UA_NodeId_copy(&other.enabledStateNodeId, &enabledStateNodeId);
        UA_NodeId_copy(&other.conditionId, &conditionId);
    }
    
    // Move Constructor
    AlarmConditionCache(AlarmConditionCache&& other) noexcept {
        sourceNodeId = other.sourceNodeId;
        enabledStateNodeId = other.enabledStateNodeId;
        conditionId = other.conditionId;
        UA_NodeId_init(&other.sourceNodeId);
        UA_NodeId_init(&other.enabledStateNodeId);
        UA_NodeId_init(&other.conditionId);
    }
    
    // Move Assignment
     AlarmConditionCache& operator=(AlarmConditionCache&& other) noexcept {
        if(this != &other) {
            UA_NodeId_clear(&sourceNodeId);
            UA_NodeId_clear(&enabledStateNodeId);
            UA_NodeId_clear(&conditionId);
            sourceNodeId = other.sourceNodeId;
            enabledStateNodeId = other.enabledStateNodeId;
            conditionId = other.conditionId;
            UA_NodeId_init(&other.sourceNodeId);
            UA_NodeId_init(&other.enabledStateNodeId);
            UA_NodeId_init(&other.conditionId);
        }
        return *this;
     }

     AlarmConditionCache& operator=(const AlarmConditionCache&) = default;
};

/* Map trigger topic (applicableTagName) to list of alarm keys (emitter+alarmName) */
struct TriggerToAlarmMapping {
    std::string triggerTopic;           // MQTT topic from applicableTagName
    std::string alarmKey;               // Key in g_alarmByKey (emitterNodeName + "-" + AlarmName)
    int triggerId;   
    int alarmId;
    int alarmInstanceId;
};

extern std::unordered_map<std::string, std::vector<TriggerToAlarmMapping>> g_triggerToAlarmMap;


extern void GlobalMQTT_Subscribe(const std::string &topic);

/**
 * @brief Information about an MQTT Topic (tag) for Generic Telemetry
 */
struct TopicInfo {
    int tagId;
    std::string name;
    std::string tagType;
    double rangeMin;
    double rangeMax;
};

extern std::unordered_map<std::string, TopicInfo> topicMap;
extern std::mutex g_topicMap_mutex;







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

    AlarmBranchInfo() {
        UA_NodeId_init(&branchNodeId);
        UA_NodeId_init(&conditionNodeId);
        isMainBranch = false;
    }

    ~AlarmBranchInfo() {
        UA_NodeId_clear(&branchNodeId);
        UA_NodeId_clear(&conditionNodeId);
    }

    // Copy Constructor (Deep Copy)
    AlarmBranchInfo(const AlarmBranchInfo& other) {
        UA_NodeId_copy(&other.branchNodeId, &branchNodeId);
        UA_NodeId_copy(&other.conditionNodeId, &conditionNodeId);
        guid = other.guid;
        isMainBranch = other.isMainBranch;
    }

    // Copy Assignment (Deep Copy)
    AlarmBranchInfo& operator=(const AlarmBranchInfo& other) {
        if(this != &other) {    
            UA_NodeId_clear(&branchNodeId);
            UA_NodeId_clear(&conditionNodeId);
            UA_NodeId_copy(&other.branchNodeId, &branchNodeId);
            UA_NodeId_copy(&other.conditionNodeId, &conditionNodeId);
            guid = other.guid;
            isMainBranch = other.isMainBranch;
        }
        return *this;
    }

    // Move Constructor
    AlarmBranchInfo(AlarmBranchInfo&& other) noexcept {
        branchNodeId = other.branchNodeId;
        conditionNodeId = other.conditionNodeId;
        guid = std::move(other.guid);
        isMainBranch = other.isMainBranch;
        
        UA_NodeId_init(&other.branchNodeId);
        UA_NodeId_init(&other.conditionNodeId);
    }

    // Move Assignment
    AlarmBranchInfo& operator=(AlarmBranchInfo&& other) noexcept {
        if(this != &other) {
            UA_NodeId_clear(&branchNodeId);
            UA_NodeId_clear(&conditionNodeId);
            
            branchNodeId = other.branchNodeId;
            conditionNodeId = other.conditionNodeId;
            guid = std::move(other.guid);
            isMainBranch = other.isMainBranch;
            
            UA_NodeId_init(&other.branchNodeId);
            UA_NodeId_init(&other.conditionNodeId);
        }
        return *this;
    }
};

// Map: alarmKey → (GUID → BranchInfo)
extern std::unordered_map<std::string, std::unordered_map<std::string, AlarmBranchInfo>> g_alarmBranches;
extern std::mutex g_alarmBranches_mutex;

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



extern std::unordered_map<std::string, std::vector<TriggerToAlarmMapping>> g_triggerToAlarmMap;

extern InstrumentedMutex g_alarmMutex; 
extern std::unordered_map<std::string, UA_NodeId> g_alarmByKey;
extern std::unordered_map<std::string, std::unordered_map<std::string, AlarmBranchInfo>> g_alarmBranches;
extern std::unordered_map<std::string, std::unordered_map<std::string, BranchState>> g_branchStates;

// Optimization Maps
extern std::unordered_map<std::string, AlarmConditionCache> g_alarmConditionCache;
extern std::unordered_map<UA_NodeId, std::string, UA_NodeId_Hasher, UA_NodeId_KeyEqual> g_nodeIdToGuidMap;
extern std::shared_mutex g_cache_mutex;


extern void GlobalMQTT_Subscribe(const std::string &topic);
void publish_to_mqtt(const std::string &topic, const std::string &payload);

void performDisable(UA_Server *server, const UA_NodeId &alarmId,const std::string &alarmKey);
std::string getPreciseTimestamp();
UA_StatusCode getOrCreateAlarmBranch(UA_Server *server, const UA_NodeId &conditionId,
                       const std::string &guid, const std::string &alarmKey,
                       UA_NodeId *outBranchId);

// Helper Functions exposed for SessionWorker.cpp
UA_NodeId findChildNodeIdAnyNS(UA_Server *server, UA_NodeId parentId, const char *searchName);
UA_NodeId findNodeByPath(UA_Server *server, UA_NodeId startNode, const std::vector<const char*>& path);
UA_StatusCode setStealthValueByPath(UA_Server *server, UA_NodeId baseNode, 
                           std::vector<const char*> path, 
                           void *newValue, const UA_DataType *type);
UA_StatusCode setStealthValueChecked(UA_Server *server, UA_NodeId baseNode, 
                            const char* name, 
                            void *newValue, const UA_DataType *type);
std::string findAlarmKeyForCondition(const UA_NodeId *alarmNodeId);

void cleanupBranches(const std::string &alarmKey);

std::string findGUIDForNodeId(const UA_NodeId *nodeId, const std::string &alarmKey);



// --- Alarm Method Callbacks (Exposed for Multi-Tenancy) ---

UA_StatusCode ConditionRefreshMethodCallback(UA_Server *server, const UA_NodeId *sessionId,
                                           void *sessionContext, const UA_NodeId *methodId,
                                           void *methodContext, const UA_NodeId *objectId,
                                           void *objectContext, size_t inputSize,
                                           const UA_Variant *input, size_t outputSize,
                                           UA_Variant *output);

UA_StatusCode customAcknowledgeCallback(UA_Server *server, const UA_NodeId *sessionId,
                                      void *sessionContext, const UA_NodeId *methodId,
                                      void *methodContext, const UA_NodeId *objectId,
                                      void *objectContext, size_t inputSize,
                                      const UA_Variant *input, size_t outputSize,
                                      UA_Variant *output);

UA_StatusCode customConfirmCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
                                  const UA_NodeId *methodId, void *methodContext,
                                  const UA_NodeId *objectId, void *objectContext, size_t inputSize,
                                  const UA_Variant *input, size_t outputSize, UA_Variant *output);

UA_StatusCode customAddCommentCallback(UA_Server *server, const UA_NodeId *sessionId,
                                     void *sessionContext, const UA_NodeId *methodId,
                                     void *methodContext, const UA_NodeId *objectId,
                                     void *objectContext, size_t inputSize, const UA_Variant *input,
                                     size_t outputSize, UA_Variant *output);

UA_StatusCode customEnableCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
                                 const UA_NodeId *methodId, void *methodContext,
                                 const UA_NodeId *objectId, void *objectContext, size_t inputSize,
                                 const UA_Variant *input, size_t outputSize, UA_Variant *output);

UA_StatusCode customDisableCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
                                  const UA_NodeId *methodId, void *methodContext,
                                  const UA_NodeId *objectId, void *objectContext, size_t inputSize,
                                  const UA_Variant *input, size_t outputSize, UA_Variant *output);

// RAII Wrapper for UA_Variant to ensure cleanup
struct ScopedVariant {
    UA_Variant var;
    ScopedVariant() { UA_Variant_init(&var); }
    ~ScopedVariant() { UA_Variant_clear(&var); }
    UA_Variant* get() { return &var; }
    UA_Variant* operator&() { return &var; } // Helper for legacy C calls
    // Note: Do not copy/move without deep copy logic.
};

#endif // ALARM_HANDLER_H
