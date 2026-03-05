#include "AandC.h"
#include <open62541/plugin/log_stdout.h>
#include <iostream>
#include <chrono>
#include <shared_mutex>
#include "alarm_enums.h"
#include "Logger.h"

using json = nlohmann::ordered_json;


//Global Maps

// Global Maps
InstrumentedMutex g_alarmMutex("g_alarmMutex");
std::unordered_map<std::string, std::vector<TriggerToAlarmMapping>> g_triggerToAlarmMap;
std::unordered_map<std::string, UA_NodeId> g_alarmByKey;
std::unordered_map<std::string, std::unordered_map<std::string, AlarmBranchInfo>> g_alarmBranches;
std::unordered_map<std::string, std::unordered_map<std::string, BranchState>> g_branchStates;

std::unordered_map<std::string, AlarmConditionCache> g_alarmConditionCache;
std::unordered_map<UA_NodeId, std::string, UA_NodeId_Hasher, UA_NodeId_KeyEqual> g_nodeIdToGuidMap;
std::shared_mutex g_cache_mutex;



//Helpers

UA_NodeId findChildNodeIdAnyNS(UA_Server *server, UA_NodeId parentId, const char *searchName) {
    UA_NodeId result = UA_NODEID_NULL;
    UA_String searchNameStr = UA_STRING((char*)searchName);

    UA_BrowseDescription bd;
    UA_BrowseDescription_init(&bd);
    bd.nodeId = parentId;
    bd.browseDirection = UA_BROWSEDIRECTION_FORWARD;
    bd.includeSubtypes = true;
    bd.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HIERARCHICALREFERENCES);
    bd.resultMask = UA_BROWSERESULTMASK_BROWSENAME;

    UA_BrowseResult bres = UA_Server_browse(server, 0, &bd);

    for(size_t i = 0; i < bres.referencesSize; ++i) {
        UA_ReferenceDescription *ref = &bres.references[i];
        if(UA_String_equal(&ref->browseName.name, &searchNameStr)) {
            UA_NodeId_copy(&ref->nodeId.nodeId, &result);
            break;
        }
    }
    UA_BrowseResult_clear(&bres);
    return result;
}


UA_NodeId findNodeByPath(UA_Server *server, UA_NodeId startNode, const std::vector<const char*>& path) {
    UA_NodeId current;
    UA_NodeId_copy(&startNode, &current);
    
    for(const char* name : path) {
        UA_NodeId next = findChildNodeIdAnyNS(server, current, name);
        UA_NodeId_clear(&current); // Clear previous
        if(UA_NodeId_isNull(&next)) return UA_NODEID_NULL;
        current = next;
    }
    return current;
}

// extern std::recursive_mutex g_server_mutex; // Extern declaration - REMOVED: Redundant with Job Queue

extern thread_local bool is_internal_write;

UA_StatusCode setStealthValueByPath(UA_Server *server, UA_NodeId baseNode, 
                                 std::vector<const char*> path, 
                                 void *newValue, const UA_DataType *type) {
    // Protect against concurrent deletion (SessionWorker)
    // std::lock_guard<std::recursive_mutex> lock(g_server_mutex); // REMOVED: Already on Server Thread via Job Queue

    UA_NodeId targetNode = findNodeByPath(server, baseNode, path);
    if(UA_NodeId_isNull(&targetNode)) {
        UA_NodeId_clear(&targetNode);
        return UA_STATUSCODE_BADNOTFOUND;
    }
    
    // Set internal flag to prevent writeCallback from publishing this update back to MQTT
    is_internal_write = true;
    
    ScopedVariant val;
    UA_Variant_setScalarCopy(val.get(), newValue, type);

    // --------------------------------------------------------
    // DEDUPLICATION: Read current value and compare
    // --------------------------------------------------------
    ScopedVariant currentVal;
    if(UA_Server_readValue(server, targetNode, currentVal.get()) == UA_STATUSCODE_GOOD) {
        if(val.get()->type == currentVal.get()->type) {
            bool isEqual = false;
            if(type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
                isEqual = (*(UA_Boolean*)val.get()->data == *(UA_Boolean*)currentVal.get()->data);
            } else if(type == &UA_TYPES[UA_TYPES_UINT16]) {
                isEqual = (*(UA_UInt16*)val.get()->data == *(UA_UInt16*)currentVal.get()->data);
            } else if(type == &UA_TYPES[UA_TYPES_INT32]) {
                isEqual = (*(UA_Int32*)val.get()->data == *(UA_Int32*)currentVal.get()->data);
            }
             // Add other types as needed, but Alarms mostly use Bool/UInt16
            
            if(isEqual) {
                // Value matches, do NOT write (prevents internal events)
                is_internal_write = false;
                UA_NodeId_clear(&targetNode);
                return UA_STATUSCODE_GOOD;
            }
        }
    }

    UA_StatusCode sc = UA_Server_writeValue(server, targetNode, val.var);
    
    is_internal_write = false;

    UA_NodeId_clear(&targetNode);
    return sc;
}

UA_StatusCode setStealthValueChecked(UA_Server *server, UA_NodeId baseNode, 
                                  const char* name, 
                                  void *newValue, const UA_DataType *type) {
    return setStealthValueByPath(server, baseNode, {name}, newValue, type);
}

std::string
findAlarmKeyForCondition(const UA_NodeId *alarmNodeId) {
    // 1. Check legacy global map
    for(const auto &kv : g_alarmByKey) {
        if(UA_NodeId_equal(&kv.second, alarmNodeId)) {
            return kv.first;
        }
    }

    // 2. Check if it's a String NodeId (Multi-tenant)
    // The alarmKey IS the NodeId string
    if(alarmNodeId->identifierType == UA_NODEIDTYPE_STRING) {
        return std::string((char *)alarmNodeId->identifier.string.data,
                           alarmNodeId->identifier.string.length);
    }

    return "";
}


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

// Helper to populate/retrieve cached NodeIds for an alarm
AlarmConditionCache ensureAlarmCache(UA_Server *server, const std::string &alarmKey, const UA_NodeId &conditionId) {
    std::shared_lock<std::shared_mutex> lock(g_cache_mutex);
    auto it = g_alarmConditionCache.find(alarmKey);
    if (it != g_alarmConditionCache.end()) {
        return it->second;
    }

    // Not found, cache it
    AlarmConditionCache cache;
    UA_NodeId_copy(&conditionId, &cache.conditionId);

    // Lookup SourceNode
    ScopedVariant sourceVar;
    if (UA_Server_readObjectProperty(server, conditionId,
                                     UA_QUALIFIEDNAME(0, (char *)"SourceNode"),
                                     sourceVar.get()) == UA_STATUSCODE_GOOD) {
        if (UA_Variant_hasScalarType(sourceVar.get(), &UA_TYPES[UA_TYPES_NODEID])) {
            UA_NodeId_copy((UA_NodeId *)sourceVar.var.data, &cache.sourceNodeId);
        }
    }
    // Fallback if SourceNode not found (point to self)
    if (UA_NodeId_isNull(&cache.sourceNodeId)) {
        UA_NodeId_copy(&conditionId, &cache.sourceNodeId);
    }

    // Lookup EnabledState
    cache.enabledStateNodeId = findChildNodeIdAnyNS(server, conditionId, (char *)"EnabledState");

    g_alarmConditionCache[alarmKey] = cache;
    return cache;
}




/* Get or create a branch for a given alarm and GUID */
UA_StatusCode
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

    // 4. Create New Virtual Branch
    // Format: ns=1;s=Branch:<GUID>
    std::string nodeIdStr = "Branch:" + guid;

    // Create the "Master" NodeId that will live in the Map
    UA_NodeId masterBranchId = UA_NODEID_STRING_ALLOC(1, nodeIdStr.c_str());

    // Store in Map
    AlarmBranchInfo branchInfo;
    branchInfo.branchNodeId = masterBranchId;  // Transfer ownership (raw struct copy)
    UA_NodeId_init(&masterBranchId);           // Clear local var so it doesn't own data anymore
    
    // Also set ConditionId
    UA_NodeId_copy(&conditionId, &branchInfo.conditionNodeId);

    branchInfo.guid = guid;
    branchInfo.isMainBranch = false;

    // Insert into map (Use MOVE to avoid extra deep copies if possible, though compiler optimizes)
    // Note: branchInfo has Move Constructor now.
    branchMap[guid] = std::move(branchInfo);

    // UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
    //             "✓ Created virtual branch for GUID '%s' (NodeId: ns=%u;s=%s)",
    //             guid.c_str(), masterBranchId.namespaceIndex, nodeIdStr.c_str());

    // 5. Return a DEEP COPY to the caller from the MAP (persistent source)
    
    // Register in Reverse Lookup Map (Optimization)
    {
        std::shared_lock<std::shared_mutex> lock(g_cache_mutex);
        g_nodeIdToGuidMap[branchMap[guid].branchNodeId] = guid;
    }

    return UA_NodeId_copy(&branchMap[guid].branchNodeId, outBranchId);
}


void performDisable(UA_Server *server, 
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

//Callbacks

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
    InstrumentedGuard lock(g_alarmMutex);

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
    InstrumentedGuard lock(g_alarmMutex);

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
    InstrumentedGuard lock(g_alarmMutex);

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
    InstrumentedGuard lock(g_alarmMutex);

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
    InstrumentedGuard lock(g_alarmMutex);

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



struct RefreshEventData {
    std::string guid;
    UA_NodeId conditionId;
    UA_NodeId branchId;
    UA_NodeId sourceNodeId;
    BranchState state; // Handles deep copy internally
    std::string alarmName;

    RefreshEventData() {
        UA_NodeId_init(&conditionId);
        UA_NodeId_init(&branchId);
        UA_NodeId_init(&sourceNodeId);
    }

    ~RefreshEventData() {
        UA_NodeId_clear(&conditionId);
        UA_NodeId_clear(&branchId);
        UA_NodeId_clear(&sourceNodeId);
        // state destructor handles eventIds
    }
    
    RefreshEventData(const RefreshEventData& other) : state(other.state) {
        guid = other.guid;
        alarmName = other.alarmName;
        UA_NodeId_copy(&other.conditionId, &conditionId);
        UA_NodeId_copy(&other.branchId, &branchId);
        UA_NodeId_copy(&other.sourceNodeId, &sourceNodeId);
    }
    
    RefreshEventData(RefreshEventData&& other) noexcept : state(std::move(other.state)) {
        guid = std::move(other.guid);
        alarmName = std::move(other.alarmName);
        conditionId = other.conditionId;
        branchId = other.branchId;
        sourceNodeId = other.sourceNodeId;
        
        UA_NodeId_init(&other.conditionId);
        UA_NodeId_init(&other.branchId);
        UA_NodeId_init(&other.sourceNodeId);
    }
};

/* Custom ConditionRefresh method callback */
UA_StatusCode
ConditionRefreshMethodCallback(UA_Server *server, const UA_NodeId *sessionId,
                               void *sessionContext, const UA_NodeId *methodId,
                               void *methodContext, const UA_NodeId *objectId,
                               void *objectContext, size_t inputSize,
                               const UA_Variant *input, size_t outputSize,
                               UA_Variant *output) {

    log(">>> ConditionRefresh CALLBACK CALLED! <<<", LogLevel::INFO);

    // Snapshot storage
    std::vector<RefreshEventData> eventsToFire;
    eventsToFire.reserve(100);

    // STEP 1: Lock Mutex for SNAPSHOT Phase
    {
        InstrumentedGuard lock(g_alarmMutex);
    
        // Fire RefreshStartEvent (Quick enough to do under lock to ensure ordering)
        {
            UA_ByteString eventId = UA_BYTESTRING_NULL;
            UA_LocalizedText msg = UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Refresh Start");

            UA_Server_createEvent(server, UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER),
                                  UA_NODEID_NUMERIC(0, UA_NS0ID_REFRESHSTARTEVENTTYPE), 
                                  100, msg, NULL, NULL, &eventId);
            UA_ByteString_clear(&eventId);
        }

    // STEP 2: Iterate through all alarms to COLLECT data
    for(auto &alarmPair : g_alarmBranches) {
        std::string alarmKey = alarmPair.first;
        auto &branchMap = alarmPair.second;
        if(branchMap.empty())
            continue;

        UA_NodeId conditionNodeId = branchMap.begin()->second.conditionNodeId;
        if(UA_NodeId_isNull(&conditionNodeId))
            continue;
            
        // Use CACHED NodeIds (Optimization #1 & #3)
        // ensureAlarmCache is thread-safe (uses its own mutex) but we also hold g_alarmMutex here so it's safe to read map keys
        AlarmConditionCache cache = ensureAlarmCache(server, alarmKey, conditionNodeId);
        
        // Check EnabledState (Optimization #1)
        // Read directly from cached NodeId without finding it again
        UA_Boolean isEnabled = UA_TRUE;
        if(!UA_NodeId_isNull(&cache.enabledStateNodeId)) {
             UA_Variant val;
             UA_Variant_init(&val);
             // Using readValue is faster than readObjectProperty for "Id"
             if(UA_Server_readValue(server, cache.enabledStateNodeId, &val) == UA_STATUSCODE_GOOD) {
                 if(UA_Variant_hasScalarType(&val, &UA_TYPES[UA_TYPES_BOOLEAN])) {
                     isEnabled = *(UA_Boolean*)val.data;
                 }
                 UA_Variant_clear(&val);
             }
        }
        
        if(!isEnabled) {
             // Skipping disabled alarm
             continue;
        }

        auto branchStateMapIt = g_branchStates.find(alarmKey);
        if(branchStateMapIt == g_branchStates.end()) {
            continue;
        }

        // Collect Events for this Alarm
        for(auto &branchPair : branchStateMapIt->second) {
            std::string guid = branchPair.first;
            BranchState &bs = branchPair.second;

            // Retain Logic: Active OR Unacked
            bool shouldRetain = (bs.active || !bs.acked);
            if(!shouldRetain)
                continue;

            // Create Snapshot Data
            RefreshEventData data;
            data.guid = guid;
            data.alarmName = alarmKey;
            
            UA_NodeId_copy(&conditionNodeId, &data.conditionId);
            UA_NodeId_copy(&cache.sourceNodeId, &data.sourceNodeId); // Use Cached SourceNode
            
            // Get BranchId
            auto bi = branchMap.find(guid);
            if(bi != branchMap.end()) {
                UA_NodeId_copy(&bi->second.branchNodeId, &data.branchId);
            }
            
            // Deep Copy State
            data.state = bs; // Uses Copy Assignment
            
            eventsToFire.push_back(std::move(data));
        }
    }
    
    } // UNLOCK Mutex (End of Phase 1)
    

    // STEP 3: Fire Events from Snapshot (Optimization #3: Template Map)
    log("ConditionRefresh: Firing " + std::to_string(eventsToFire.size()) + " events...", LogLevel::INFO);
    
    // 1. Create a "Template" map OUTSIDE the loop
    UA_KeyValueMap map = UA_KEYVALUEMAP_NULL;
    UA_NodeId eventTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE);
    UA_Boolean enabledVal = UA_TRUE;
    UA_LocalizedText enabledLT = UA_LOCALIZEDTEXT((char*)"en", (char*)"Enabled");
    UA_Boolean retainVal = UA_TRUE; // Always true for Refresh

    // Pre-set fields that are the same for all events
    UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"EventType"), &eventTypeId, &UA_TYPES[UA_TYPES_NODEID]);
    UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"EnabledState/Id"), &enabledVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
    UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"EnabledState"), &enabledLT, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
    UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"Retain"), &retainVal, &UA_TYPES[UA_TYPES_BOOLEAN]);
    
    // Placeholders for dynamic fields (Optimization: Initialize once)
    UA_NodeId nullNodeId = UA_NODEID_NULL;
    UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"ConditionClassId"), &nullNodeId, &UA_TYPES[UA_TYPES_NODEID]);

    for(auto &evt : eventsToFire) {
        
        // 2. Only update the DYNAMIC fields (Time, ActiveState, Severity, etc.)
        
        // ActiveState/Id
        UA_Boolean activeId = evt.state.active;
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"ActiveState/Id"), &activeId, &UA_TYPES[UA_TYPES_BOOLEAN]);
        
        // ActiveState
        UA_LocalizedText activeLT = evt.state.active ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Active") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Inactive");
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"ActiveState"), &activeLT, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

        // AckedState/Id
        UA_Boolean ackedId = evt.state.acked;
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"AckedState/Id"), &ackedId, &UA_TYPES[UA_TYPES_BOOLEAN]);

        // AckedState
        UA_LocalizedText ackedLT = evt.state.acked ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Acknowledged") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unacknowledged");
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"AckedState"), &ackedLT, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

        // ConfirmedState/Id
        UA_Boolean confirmedId = evt.state.confirmed;
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"ConfirmedState/Id"), &confirmedId, &UA_TYPES[UA_TYPES_BOOLEAN]);

        // ConfirmedState
        UA_LocalizedText confirmedLT = evt.state.confirmed ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Confirmed") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unconfirmed");
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"ConfirmedState"), &confirmedLT, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

        // Time
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"Time"), &evt.state.time, &UA_TYPES[UA_TYPES_DATETIME]);

        // ReceiveTime
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"ReceiveTime"), &evt.state.receiveTime, &UA_TYPES[UA_TYPES_DATETIME]);

        // Quality
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"Quality"), &evt.state.quality, &UA_TYPES[UA_TYPES_STATUSCODE]);

        // BranchId
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"BranchId"), &evt.branchId, &UA_TYPES[UA_TYPES_NODEID]);

        // SourceNode
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"SourceNode"), &evt.sourceNodeId, &UA_TYPES[UA_TYPES_NODEID]);

        // ConditionName
        UA_String condName = UA_STRING((char *)evt.alarmName.c_str());
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"ConditionName"), &condName, &UA_TYPES[UA_TYPES_STRING]);

        // ConditionId
        UA_KeyValueMap_setScalar(&map, UA_QUALIFIEDNAME(0, (char*)"ConditionId"), &evt.conditionId, &UA_TYPES[UA_TYPES_NODEID]);

        // Message & Severity (Arguments)
        UA_LocalizedText msgText = UA_LOCALIZEDTEXT((char*)"en-US", (char*)evt.state.message.c_str()); 
        
        // Fire Event
        UA_ByteString newEventId = UA_BYTESTRING_NULL;
        
        UA_StatusCode rc = UA_Server_createEvent(
            server, evt.sourceNodeId,
            UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE),
            evt.state.severity, msgText, &map, NULL, &newEventId);

        if(rc == UA_STATUSCODE_GOOD && newEventId.length > 0) {
            // Note: We cannot update the 'state' eventIds here safely because it might have changed
            // in the global map since we unlocked. However, refresh events are usually transient.
            // If we need to track this EventId for Acknowledge, we need to re-lock and update g_branchStates.
            
            // Optimization: Only lock if we need to add the ID
            InstrumentedGuard lock(g_alarmMutex);
            auto &states = g_branchStates[evt.alarmName];
            auto it = states.find(evt.guid);
            if(it != states.end()) {
                it->second.addEventId(&newEventId);
            }
        } else {
            UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                          "Failed to create temp event for GUID '%s': %s",
                          evt.guid.c_str(), UA_StatusCode_name(rc));
        }
        
        UA_ByteString_clear(&newEventId);
        // DO NOT Clear map here! We reuse it.
    }
    
    UA_KeyValueMap_clear(&map); // Clean up once at the very end

    // STEP 8: Fire RefreshEndEvent
    {
        UA_ByteString eventId = UA_BYTESTRING_NULL;
        UA_LocalizedText msg = UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Refresh Complete");
        // Source: Server, Type: RefreshEndEventType, Severity: 100
        UA_Server_createEvent(server, UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER),
                              UA_NODEID_NUMERIC(0, UA_NS0ID_REFRESHENDEVENTTYPE),
                              100, msg, NULL, NULL, &eventId);
        UA_ByteString_clear(&eventId);
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


/* Find GUID for a given NodeId (branch or condition) */
std::string findGUIDForNodeId(const UA_NodeId *nodeId, const std::string &alarmKey) {
    if(!nodeId) return "";
    
    std::shared_lock<std::shared_mutex> lock(g_cache_mutex);
    auto it = g_nodeIdToGuidMap.find(*nodeId);
    if(it != g_nodeIdToGuidMap.end()) {
        return it->second;
    }
    
    return ""; 
}

/* Cleanup inactive and acknowledged branches */
void
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
            
            // Remove from Reverse Lookup Map
            {
                std::shared_lock<std::shared_mutex> lock(g_cache_mutex);
                auto bit = branchMap.find(guid);
                if(bit != branchMap.end()) {
                    g_nodeIdToGuidMap.erase(bit->second.branchNodeId);
                }
            }

            if(stateIt != branchStateMap.end()) {
                branchStateMap.erase(stateIt);
            }
            it = branchMap.erase(it);
        } else {
            ++it;
        }
    }
}