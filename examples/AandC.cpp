#include "AandC.h"
#include <open62541/plugin/log_stdout.h>
#include <iostream>
#include <chrono>
#include "alarm_enums.h"

using json = nlohmann::ordered_json;


//Global Maps

// Global Maps
std::mutex g_alarmMutex;
std::unordered_map<std::string, std::vector<TriggerToAlarmMapping>> g_triggerToAlarmMap;
std::unordered_map<std::string, UA_NodeId> g_alarmByKey;
std::unordered_map<std::string, std::unordered_map<std::string, AlarmBranchInfo>> g_alarmBranches;
std::unordered_map<std::string, std::unordered_map<std::string, BranchState>> g_branchStates;


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

UA_StatusCode setStealthValueByPath(UA_Server *server, UA_NodeId baseNode, 
                                 std::vector<const char*> path, 
                                 void *newValue, const UA_DataType *type) {
    UA_NodeId targetNode = findNodeByPath(server, baseNode, path);
    if(UA_NodeId_isNull(&targetNode)) {
        UA_NodeId_clear(&targetNode);
        return UA_STATUSCODE_BADNOTFOUND;
    }
    
    
    ScopedVariant val;
    UA_Variant_setScalarCopy(val.get(), newValue, type);
    UA_StatusCode sc = UA_Server_writeValue(server, targetNode, val.var);
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
        UA_ByteString eventId = UA_BYTESTRING_NULL;
        UA_LocalizedText msg = UA_LOCALIZEDTEXT((char *)"en-US", (char *)"Refresh Start");

        UA_Server_createEvent(server, UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER),
                              UA_NODEID_NUMERIC(0, UA_NS0ID_REFRESHSTARTEVENTTYPE), 
                              100, msg, NULL, NULL, &eventId);
        UA_ByteString_clear(&eventId);
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
        ScopedVariant sourceVar;
        if(UA_Server_readObjectProperty(server, conditionNodeId,
                                        UA_QUALIFIEDNAME(0, (char *)"SourceNode"),
                                        sourceVar.get()) == UA_STATUSCODE_GOOD) {
            if(UA_Variant_hasScalarType(sourceVar.get(), &UA_TYPES[UA_TYPES_NODEID])) {
                UA_NodeId_copy((UA_NodeId *)sourceVar.var.data, &sourceNodeId);
            }
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
                ScopedVariant enabledVar;
                UA_StatusCode enabledRc = UA_Server_readObjectProperty(server, enabledStateId, qId, enabledVar.get());
                if(enabledRc == UA_STATUSCODE_GOOD && UA_Variant_hasScalarType(enabledVar.get(), &UA_TYPES[UA_TYPES_BOOLEAN])) {
                    isEnabled = *(UA_Boolean*)enabledVar.var.data;
                }
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

            // === CREATE EVENT WITH KEY-VALUE MAP ===
            UA_KeyValueMap *map = UA_KeyValueMap_new();
            if(map) {
                // ActiveState/Id
                UA_Boolean val = bs.active;
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"ActiveState/Id"), &val, &UA_TYPES[UA_TYPES_BOOLEAN]);
                
                // ActiveState
                UA_LocalizedText valLT = bs.active ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Active") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Inactive");
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"ActiveState"), &valLT, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                // AckedState/Id
                val = bs.acked;
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"AckedState/Id"), &val, &UA_TYPES[UA_TYPES_BOOLEAN]);

                // AckedState
                valLT = bs.acked ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Acknowledged") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unacknowledged");
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"AckedState"), &valLT, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                // ConfirmedState/Id
                val = bs.confirmed;
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"ConfirmedState/Id"), &val, &UA_TYPES[UA_TYPES_BOOLEAN]);

                // ConfirmedState
                valLT = bs.confirmed ? UA_LOCALIZEDTEXT((char*)"en", (char*)"Confirmed") : UA_LOCALIZEDTEXT((char*)"en", (char*)"Unconfirmed");
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"ConfirmedState"), &valLT, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                // Retain
                val = shouldRetain;
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"Retain"), &val, &UA_TYPES[UA_TYPES_BOOLEAN]);

                // Time
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"Time"), &bs.time, &UA_TYPES[UA_TYPES_DATETIME]);

                // ReceiveTime
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"ReceiveTime"), &bs.receiveTime, &UA_TYPES[UA_TYPES_DATETIME]);

                // Quality
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"Quality"), &bs.quality, &UA_TYPES[UA_TYPES_STATUSCODE]);

                // EnabledState/Id
                val = UA_TRUE;
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"EnabledState/Id"), &val, &UA_TYPES[UA_TYPES_BOOLEAN]);

                // EnabledState
                valLT = UA_LOCALIZEDTEXT((char*)"en", (char*)"Enabled");
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"EnabledState"), &valLT, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

                // BranchId
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"BranchId"), &branchId, &UA_TYPES[UA_TYPES_NODEID]);

                // SourceNode
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"SourceNode"), &sourceNodeId, &UA_TYPES[UA_TYPES_NODEID]);

                // EventType
                UA_NodeId eventTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE);
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"EventType"), &eventTypeId, &UA_TYPES[UA_TYPES_NODEID]);

                // ConditionClassId
                UA_NodeId condClassId = UA_NODEID_NULL;
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"ConditionClassId"), &condClassId, &UA_TYPES[UA_TYPES_NODEID]);

                // ConditionName
                UA_String condName = UA_STRING((char *)alarmKey.c_str());
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"ConditionName"), &condName, &UA_TYPES[UA_TYPES_STRING]);

                // ConditionId
                UA_KeyValueMap_setScalar(map, UA_QUALIFIEDNAME(0, (char*)"ConditionId"), &conditionNodeId, &UA_TYPES[UA_TYPES_NODEID]);
            }

            // Message & Severity (Arguments)
            UA_LocalizedText msgText = UA_LOCALIZEDTEXT((char*)"en-US", (char*)bs.message.c_str()); // Stack allocated is fine for immediate call?
            // Actually UA_LOCALIZEDTEXT macro expects char pointers, creates shallow copies on stack.
            // UA_Server_createEvent copies. 
            
            // DEBUG: Log what we're about to fire
            UA_String branchIdStr = UA_STRING_NULL;
            UA_NodeId_print(&branchId, &branchIdStr);
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "ConditionRefresh: Triggering MANUAL event for GUID='%s', "
                       "BranchId=%.*s, ACTIVE=%d, ACKED=%d, severity=%u",
                       guid.c_str(), (int)branchIdStr.length, branchIdStr.data,
                       bs.active, bs.acked, bs.severity);
            UA_String_clear(&branchIdStr);

            // Fire Event
            UA_ByteString newEventId = UA_BYTESTRING_NULL;
            UA_StatusCode rc = UA_Server_createEvent(
                server, sourceNodeId,
                UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE),
                bs.severity, msgText, map, NULL, &newEventId);

            if(rc == UA_STATUSCODE_GOOD && newEventId.length > 0) {
                // Store the new EventId so client can acknowledge it
                bs.addEventId(&newEventId);
            } else {
                UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                              "Failed to create temp event for GUID '%s': %s",
                              guid.c_str(), UA_StatusCode_name(rc));
            }
            
            UA_ByteString_clear(&newEventId);
            UA_KeyValueMap_delete(map);

        }


        UA_NodeId_clear(&sourceNodeId);
    }

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
            if(stateIt != branchStateMap.end()) {
                branchStateMap.erase(stateIt);
            }
            it = branchMap.erase(it);
        } else {
            ++it;
        }
    }
}