#include "SessionManager.h"
#include "Logger.h"
#include "fetchAPI.h"
#include "AlarmConfig.h"
#include "AandC.h"
#include <nlohmann/json.hpp>
#include <future>
#include <map>
#include <sstream>


using json = nlohmann::ordered_json;

extern std::unordered_map<std::string, UA_NodeId> g_alarmByKey;
extern void GlobalMQTT_Subscribe(const std::string &topic);
extern void GlobalMQTT_SubscribeBatch(const std::vector<std::string> &topics);
extern void GlobalMQTT_Unsubscribe(const std::string &topic);
extern void GlobalMQTT_UnsubscribeBatch(const std::vector<std::string> &topics); // Optimization

extern std::map<std::string, UA_NodeId> nodeMap;
extern std::mutex g_nodeMap_mutex;

// External callback from server.cpp
extern void writeCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
              const UA_NodeId *nodeId, void *nodeContext, const UA_NumericRange *range,
              const UA_DataValue *data);


// ============================================================================
// Helper Functions for Address Space Creation
// ============================================================================

/**
 * Split a string by delimiter
 */
static std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while(std::getline(tokenStream, token, delimiter)) {
        if(!token.empty()) {
            tokens.push_back(token);
        }
    }
    return tokens;
}

/**
 * Get or create a folder node in the namespace
 * Returns the NodeId of the folder
 */
static UA_NodeId getOrCreateFolder(UA_Server* server, 
                                  const std::string& path,
                                  const std::string& displayName,
                                  UA_NodeId parent,
                                  UA_UInt16 namespaceIndex,
                                  std::map<std::string, UA_NodeId>& folderMap,
                                  std::map<std::string, UA_NodeId>& nodeMap) {
    
    // Check if folder already exists
    auto it = folderMap.find(path);
    if(it != folderMap.end()) {
        return it->second;
    }
    
    // Create new folder
    UA_ObjectAttributes objAttr = UA_ObjectAttributes_default;
    objAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", displayName.c_str());
    // objAttr.eventNotifier = 0x01; // SubscribeToEvents removed as per user request (only on emitters now)
    
    UA_NodeId folderId = UA_NODEID_STRING_ALLOC(namespaceIndex, path.c_str());
    
    UA_QualifiedName folderName = UA_QUALIFIEDNAME_ALLOC(namespaceIndex, displayName.c_str());
    UA_StatusCode rc = UA_Server_addObjectNode(
        server, folderId, parent,
        UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
        folderName,
        UA_NODEID_NUMERIC(0, UA_NS0ID_FOLDERTYPE),
        objAttr, NULL, NULL);

    UA_QualifiedName_clear(&folderName);
    
        UA_ObjectAttributes_clear(&objAttr);
        if(rc == UA_STATUSCODE_GOOD || rc == UA_STATUSCODE_BADNODEIDEXISTS) {
            folderMap[path] = folderId;
            nodeMap[path] = folderId; // Add to nodeMap so alarms can find it!
            return folderId;
        }
    
    // If creation failed, return parent
    return parent;
}

/**
 * Dedicated worker thread for a single session
 * Creates org-specific address space and manages resources
 */
void sessionWorkerThread(SessionContext* ctx, UA_Server* server,
                        const std::string& bearerToken,
                        const std::string& apiHost, const std::string& apiPort) {
    
    log("🚀 [THREAD START] Worker thread started for org '" + ctx->shortCode + 
        "' (OrgID: " + std::to_string(ctx->orgId) + ", Namespace: " + 
        ctx->namespaceUri + ")", LogLevel::INFO);
    
    UA_NodeId orgRootFolder = UA_NODEID_NULL; // Declared outside for cleanup
    
    try {
        // ====================================================================
        // STEP 1: Register dynamic namespace
        // ====================================================================
        log("📝 Registering namespace for org '" + ctx->shortCode + "'...", LogLevel::DEBUG);
        ctx->namespaceIndex = UA_Server_addNamespace(server, ctx->namespaceUri.c_str());
        
        if(ctx->namespaceIndex == 0) {
            log("❌ Failed to register namespace for org '" + ctx->shortCode + "'", 
                LogLevel::ERRORS);
            return;
        }
        
        log("✓ Namespace '" + ctx->namespaceUri + "' registered at index " + 
            std::to_string(ctx->namespaceIndex), LogLevel::INFO);
        
        // ====================================================================
        // STEP 2: Fetch org-specific topics
        // ====================================================================
        log("📡 Fetching topics for OrgID: " + std::to_string(ctx->orgId) + 
            " (" + ctx->shortCode + ")", LogLevel::INFO);
        
        std::string topicJsonBody = R"({
            "orgId": )" + std::to_string(ctx->orgId) + R"(,
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
                "isLogging": true
            }
        })";
        
        auto futureResponse =
            std::async(std::launch::async, getResponse,
                                        apiHost, apiPort, bearerToken,
                                        topicJsonBody, "/api/GetTopicList");
        json topicResponse = futureResponse.get();
        
        // ====================================================================
        // STEP 3: Store org-specific topics
        // ====================================================================
        if(topicResponse.contains("data") && topicResponse["data"].is_array()) {
            log("✓ Fetched " + std::to_string(topicResponse["data"].size()) +
                " topics for org '" + ctx->shortCode + "' (OrgID: " + 
                std::to_string(ctx->orgId) + ")", LogLevel::INFO);
            
            for(const auto& item : topicResponse["data"]) {
                if(item.contains("namespace")) {
                    std::string ns = item["namespace"].get<std::string>();
                    ctx->topics.push_back(ns);
                }
            }
            
            log("✓ Stored " + std::to_string(ctx->topics.size()) + 
                " topics for org '" + ctx->shortCode + "'", LogLevel::INFO);
        } else {
            log("⚠️ No topics found in response for org '" + ctx->shortCode + "'", 
                LogLevel::DEBUG);
        }
        
        // ====================================================================
        // STEP 3: Create Address Space for Topics
        // ====================================================================
        log("📦 Creating address space for " + std::to_string(topicResponse["data"].size()) + 
            " topics...", LogLevel::INFO);
        
        // ====================================================================
        // Create organization-specific root folder in Objects
        // ====================================================================
        UA_ObjectAttributes rootObjAttr = UA_ObjectAttributes_default;
        rootObjAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", ctx->shortCode.c_str());
        rootObjAttr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", 
                                    ("Root folder for organization: " + ctx->shortCode).c_str());
        
        orgRootFolder = UA_NODEID_STRING_ALLOC(ctx->namespaceIndex, 
                                    ("OrgRoot_" + ctx->shortCode).c_str());
        
        UA_QualifiedName rootName = UA_QUALIFIEDNAME_ALLOC(ctx->namespaceIndex, ctx->shortCode.c_str());
        UA_StatusCode rc = UA_Server_addObjectNode(
            server, orgRootFolder,
            UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
            UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
            rootName,
            UA_NODEID_NUMERIC(0, UA_NS0ID_FOLDERTYPE),
            rootObjAttr, NULL, NULL);
        
        UA_QualifiedName_clear(&rootName);
        
        if(rc != UA_STATUSCODE_GOOD && rc != UA_STATUSCODE_BADNODEIDEXISTS) {
            log("❌ Failed to create root folder for org '" + ctx->shortCode + "'", 
                LogLevel::ERRORS);
            orgRootFolder = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER); // Fallback
        } else {
            log("✓ Created root folder '" + ctx->shortCode + "' in namespace " + 
                std::to_string(ctx->namespaceIndex), LogLevel::INFO);
        }
        UA_ObjectAttributes_clear(&rootObjAttr);
        
        std::map<std::string, UA_NodeId> folderMap;
        // std::map<std::string, UA_NodeId> nodeMap; // Removed local map, using ctx->nodeMap
        
        int nodesCreated = 0;
        
        for(const auto& item : topicResponse["data"]) {
            if(!item.contains("namespace") || !item.contains("tagId")) {
                continue;
            }
            
            std::string ns = item["namespace"].get<std::string>();
            auto parts = split(ns, '/');
            
            if(parts.empty()) continue;
            
            // Build folder hierarchy UNDER the org root folder
            std::string currentPath;
            UA_NodeId parent = orgRootFolder; // Start from org-specific root!
            
            for(size_t i = 0; i < parts.size() - 1; i++) {
                if(!currentPath.empty()) currentPath += "/";
                currentPath += parts[i];
                
                parent = getOrCreateFolder(server, currentPath, parts[i], 
                                          parent, ctx->namespaceIndex, folderMap, ctx->nodeMap);
            }
            
            // Create variable node for the topic IN THE ORG'S NAMESPACE
            UA_VariableAttributes attr = UA_VariableAttributes_default;
            UA_Int32 value = 0;
            UA_Variant_setScalarCopy(&attr.value, &value, &UA_TYPES[UA_TYPES_INT32]);
            
            attr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", parts.back().c_str());
            if(item.contains("name")) {
                attr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", 
                                    item["name"].get<std::string>().c_str());
            }
            attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
            
            // Create node ID using tagId IN ORG'S NAMESPACE
            int tagId = item["tagId"].get<int>();
            UA_NodeId nodeId = UA_NODEID_NUMERIC(ctx->namespaceIndex, tagId);
            
            UA_QualifiedName nodeName = UA_QUALIFIEDNAME_ALLOC(ctx->namespaceIndex, parts.back().c_str());
            rc = UA_Server_addVariableNode(
                server, nodeId, parent,
                UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
                nodeName,
                UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
                attr, NULL, NULL);
            
            UA_QualifiedName_clear(&nodeName);

            if(rc == UA_STATUSCODE_GOOD) {
                // Attach Write Callback for MQTT Publishing
                UA_ValueCallback callback;
                callback.onRead = NULL;
                callback.onWrite = writeCallback;
                UA_Server_setVariableNode_valueCallback(server, nodeId, callback);
            }
            UA_VariableAttributes_clear(&attr);
            
            if(rc == UA_STATUSCODE_GOOD || rc == UA_STATUSCODE_BADNODEIDEXISTS) {
                ctx->nodeMap[ns] = nodeId;
                {
                    std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                    nodeMap[ns] = nodeId;
                }
                
                // Populate Global TopicMap for Generic Telemetry (Write Callback)
                {
                    std::lock_guard<std::mutex> lock(g_topicMap_mutex);
                    TopicInfo info;
                    info.tagId = tagId;
                    if(item.contains("name")) info.name = item["name"].get<std::string>();
                    if(item.contains("tagType")) info.tagType = item["tagType"].get<std::string>();
                    if(item.contains("rangeMin")) info.rangeMin = item["rangeMin"].get<double>();
                    else info.rangeMin = 0.0;
                    if(item.contains("rangeMax")) info.rangeMax = item["rangeMax"].get<double>();
                    else info.rangeMax = 0.0;
                    
                    topicMap[ns] = info;
                }
                // 📡 Dynamic Subscribe
                // Optimization: ctx->topics is already populated in Step 3 (Lines 161-166)
                // We do NOT need to scan and add it again here. This removes an O(N^2) bottleneck.
                /* 
                if(std::find(ctx->topics.begin(), ctx->topics.end(), ns) == ctx->topics.end()) {
                    ctx->topics.push_back(ns);
                } 
                */ 

                nodesCreated++;
                
                // Add EURange property if available
                if(item.contains("rangeMin") && item.contains("rangeMax")) {
                    UA_Range range;
                    range.low = item["rangeMin"].get<double>();
                    range.high = item["rangeMax"].get<double>();
                    
                    UA_Variant rangeVariant;
                    UA_Variant_setScalarCopy(&rangeVariant, &range, &UA_TYPES[UA_TYPES_RANGE]);
                    
                    UA_VariableAttributes rangeAttr = UA_VariableAttributes_default;
                    rangeAttr.value = rangeVariant;
                    rangeAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "EURange");
                    
                    UA_NodeId rangeNodeId = UA_NODEID_NUMERIC(ctx->namespaceIndex, tagId * 1000 + 1);
                    
                    UA_QualifiedName rangeName = UA_QUALIFIEDNAME_ALLOC(0, "EURange");
                    
                    UA_Server_addVariableNode(
                        server, rangeNodeId, nodeId,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                        rangeName,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                        rangeAttr, NULL, NULL);
                    
                    UA_QualifiedName_clear(&rangeName);
                    UA_VariableAttributes_clear(&rangeAttr);
                }
                
                // Add alarm limits if available
                if(item.contains("alarmHiHi")) {
                    UA_Double alarmHiHi = item["alarmHiHi"].get<double>();
                    UA_Variant alarmVariant;
                    UA_Variant_setScalarCopy(&alarmVariant, &alarmHiHi, &UA_TYPES[UA_TYPES_DOUBLE]);
                    
                    UA_VariableAttributes alarmAttr = UA_VariableAttributes_default;
                    alarmAttr.value = alarmVariant;
                    alarmAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "AlarmHiHi");
                    
                    UA_NodeId alarmNodeId = UA_NODEID_NUMERIC(ctx->namespaceIndex, tagId * 1000 + 2);
                    UA_QualifiedName alarmName = UA_QUALIFIEDNAME_ALLOC(0, "AlarmHiHi");
                    UA_Server_addVariableNode(
                        server, alarmNodeId, nodeId,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                        alarmName,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                        alarmAttr, NULL, NULL);
                        
                    UA_QualifiedName_clear(&alarmName);
                    UA_VariableAttributes_clear(&alarmAttr);
                }
                
                // Add engineering unit if available
                if(item.contains("measurmentUnitType")) {
                    UA_String unit = UA_STRING_ALLOC(item["measurmentUnitType"].get<std::string>().c_str());
                    UA_Variant unitVariant;
                    UA_Variant_setScalarCopy(&unitVariant, &unit, &UA_TYPES[UA_TYPES_STRING]);
                    
                    UA_VariableAttributes unitAttr = UA_VariableAttributes_default;
                    unitAttr.value = unitVariant;
                    unitAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "EngineeringUnit");
                    
                    UA_NodeId unitNodeId = UA_NODEID_NUMERIC(ctx->namespaceIndex, tagId * 1000 + 6);
                    UA_QualifiedName unitName = UA_QUALIFIEDNAME_ALLOC(0, "EngineeringUnit");
                    UA_Server_addVariableNode(
                        server, unitNodeId, nodeId,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                        unitName,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                        unitAttr, NULL, NULL);
                    
                    UA_QualifiedName_clear(&unitName);
                    UA_String_clear(&unit);
                    UA_VariableAttributes_clear(&unitAttr);
                }
            }
        }
        
        log("✓ Created " + std::to_string(nodesCreated) + " variable nodes for org '" + 
            ctx->shortCode + "' in namespace " + std::to_string(ctx->namespaceIndex) + 
            " ('" + ctx->namespaceUri + "')", LogLevel::INFO);
            
        // 📡 BATCH SUBSCRIBE: Now that all nodes are created, perform subscription
        if(!ctx->topics.empty()) {
            log("📡 Batch subscribing to " + std::to_string(ctx->topics.size()) + " topics...", LogLevel::INFO);
            GlobalMQTT_SubscribeBatch(ctx->topics);
        }

        // ====================================================================
        // STEP 4: Fetch org-specific alarms
        // ====================================================================
        log("📡 Fetching alarms for OrgID: " + std::to_string(ctx->orgId) + 
            " (" + ctx->shortCode + ")", LogLevel::INFO);
        
        std::string alarmJsonBody = R"({
            "filterModel": {
                "currentPage": 1,
                "pageSize": 100
            },
            "orgId": )" + std::to_string(ctx->orgId) + R"(
        })";
        
        std::vector<AlarmConfig> alarms = ParseAlarmConfig(apiHost, apiPort, bearerToken,
                                                           alarmJsonBody,
                                                           "/api/GetAlarmsConfigDetailList");
        
        log("✓ Fetched " + std::to_string(alarms.size()) + " alarms for org '" + 
            ctx->shortCode + "' (OrgID: " + std::to_string(ctx->orgId) + ")", 
            LogLevel::INFO);
        
        // ====================================================================
        // STEP 5: Create Alarm Conditions
        // ====================================================================
        log("📋 Creating alarm conditions in namespace " + std::to_string(ctx->namespaceIndex) + "...", LogLevel::INFO);
        
        int alarmsCreated = 0;
        for(const auto& alarm : alarms) {
            if(!alarm.alarmEmitters.has_value()) continue;
            
            for(const auto& emitter : alarm.alarmEmitters.value()) {
                // Sanitize emitter name (remove trailing slash)
                std::string searchKey = emitter.emitterNodeName;
                if(!searchKey.empty() && searchKey.back() == '/') {
                    searchKey.pop_back();
                }

                // Find emitter node in org's nodeMap
                auto it = ctx->nodeMap.find(searchKey);
                if(it == ctx->nodeMap.end()) {
                    log("⚠️ No node found for emitter: " + emitter.emitterNodeName, LogLevel::ERRORS);
                    
                    // DEBUG: Dump first 10 keys in map to see what IS there
                    int limit = 0;
                    log("--- Dumping Available NodeMap keys (First 10) ---", LogLevel::INFO);
                    for(const auto& pair : ctx->nodeMap) {
                        log("Key: '" + pair.first + "'", LogLevel::INFO);
                        if(++limit >= 10) break;
                    }
                    log("--- End Dump ---", LogLevel::INFO);
                    
                    continue;
                }
                
                UA_NodeId sourceNode = it->second;
                std::string alarmKey = emitter.emitterNodeName + "-" + alarm.name;
                
                // Deterministic NodeId (String) for Multi-Tenancy
                UA_NodeId requestedNodeId = UA_NODEID_STRING_ALLOC(ctx->namespaceIndex, alarmKey.c_str());
                
                // Set EventNotifier on emitter BEFORE creating condition
                // This ensures the A&C subsystem registers this node as a valid ConditionSource
                UA_Byte eventNotifier = 0x01; 
                UA_StatusCode evtRc = UA_Server_writeEventNotifier(server, sourceNode, eventNotifier);
                if(evtRc != UA_STATUSCODE_GOOD) {
                    log("❌ Failed to set EventNotifier on source node " + emitter.emitterNodeName + ": " + UA_StatusCode_name(evtRc), LogLevel::ERRORS);
                } else {
                    log("✓ EventNotifier set on source node " + emitter.emitterNodeName, LogLevel::INFO);
                }

                // Create condition in org's namespace
                UA_NodeId alarmId = UA_NODEID_NULL;
                UA_StatusCode sc = UA_Server_createCondition(
                    server, requestedNodeId,
                    UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE),
                    UA_QUALIFIEDNAME(ctx->namespaceIndex, (char*)alarm.name.c_str()), 
                    sourceNode,
                    UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT),
                    &alarmId
                );
                
                UA_NodeId_clear(&requestedNodeId); // Clear our copy
                
                if(sc == UA_STATUSCODE_BADNODEIDEXISTS) {
                     // Alarm already exists (created by another session for this org)
                     // Reconstruct NodeId for local map
                     alarmId = UA_NODEID_STRING_ALLOC(ctx->namespaceIndex, alarmKey.c_str());
                     ctx->alarmMap[alarmKey] = alarmId;
                     continue; 
                }
                
                if(sc == UA_STATUSCODE_GOOD) {
                    // Initialize alarm state (STEALTH MODE)
                    UA_Boolean enabled = alarm.enable ? UA_TRUE : UA_FALSE;
                    setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &enabled, &UA_TYPES[UA_TYPES_BOOLEAN]);
                    
                    UA_Boolean inactive = UA_FALSE;
                    setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                    setStealthValueChecked(server, alarmId, "ActiveState", &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                    setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                    setStealthValueChecked(server, alarmId, "Retain", &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                    
                    UA_UInt16 initialSeverity = 0;
                    setStealthValueChecked(server, alarmId, "Severity", &initialSeverity, &UA_TYPES[UA_TYPES_UINT16]);
                    
                    UA_LocalizedText initialMsg = UA_LOCALIZEDTEXT((char*)"en-US", (char*)"Alarm initialized (inactive)");
                    setStealthValueChecked(server, alarmId, "Message", &initialMsg, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
                    

                    // Verify NodeClass of created alarm
                    UA_NodeClass nodeClass;
                    UA_Server_readNodeClass(server, alarmId, &nodeClass);
                    log("ℹ️ Created alarm '" + alarm.name + "' NodeClass: " + std::to_string(nodeClass) + " (1=Object, 2=Variable)", LogLevel::INFO);
                    
                    // Register method callbacks
                    UA_NodeId methodId;
                    methodId = findChildNodeIdAnyNS(server, alarmId, "Acknowledge");
                    if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customAcknowledgeCallback);
                    
                    methodId = findChildNodeIdAnyNS(server, alarmId, "Confirm");
                    if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customConfirmCallback);
                    
                    methodId = findChildNodeIdAnyNS(server, alarmId, "AddComment");
                    if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customAddCommentCallback);
                    
                    methodId = findChildNodeIdAnyNS(server, alarmId, "Enable");
                    if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customEnableCallback);
                    
                    methodId = findChildNodeIdAnyNS(server, alarmId, "Disable");
                    if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customDisableCallback);
                    
                    // Populate g_triggerToAlarmMap (Thread Safe)
                    // Populate g_triggerToAlarmMap using Emitter Topic (User Requirement)
                    std::string topic = searchKey; // Use sanitised emitter name
                    
                    if(!topic.empty()) {
                         if(alarm.alarmTriggers.has_value()) {
                             std::lock_guard<std::mutex> lock(g_alarmMutex);
                             for(const auto& trigger : alarm.alarmTriggers.value()) {
                                  TriggerToAlarmMapping mapping;
                                  mapping.triggerTopic = topic;
                                  mapping.alarmKey = alarmKey;
                                  mapping.triggerId = trigger.id;
                                  mapping.alarmId = alarm.id;
                                  mapping.alarmInstanceId = 0;
                                  
                              bool exists = false;
                              auto& list = g_triggerToAlarmMap[topic];
                              for(const auto& m : list) {
                                  if(m.alarmKey == alarmKey && m.triggerId == trigger.id) {
                                      exists = true;
                                      break;
                                  }
                              }
                              if(!exists) {
                                  g_triggerToAlarmMap[topic].push_back(mapping);
                              }

                             }
                             // Populate global lookup map
                             // Allocate a fresh NodeId to ensure validity (avoid dangling pointers from createCondition outputs)
                             UA_NodeId safeAlarmId = UA_NODEID_STRING_ALLOC(ctx->namespaceIndex, alarmKey.c_str());
                             // Check for existing key to avoid leak
                             if(g_alarmByKey.find(alarmKey) != g_alarmByKey.end()) {
                                 UA_NodeId_clear(&g_alarmByKey[alarmKey]);
                             }
                             g_alarmByKey[alarmKey] = safeAlarmId;
                         }


                         // via logic in perform_subscriptions (server.cpp) IF the topic is in the map.
                         log("DEBUG: Subscribing to Emitter Topic (Alarm Prepared): " + topic, LogLevel::INFO);
                         GlobalMQTT_Subscribe(topic);
                         ctx->subscribedEmitters.push_back(topic);
                    } else {
                         log("WARNING: Emitter (ID: " + std::to_string(emitter.id) + 
                             ") has EMPTY emitterNodeName! MQTT subscription skipped.", LogLevel::ERRORS);
                    }

                    // Store in map
                    ctx->alarmMap[alarmKey] = UA_NODEID_STRING_ALLOC(ctx->namespaceIndex, alarmKey.c_str());
                    alarmsCreated++;
                } else {
                    log("❌ Failed to create alarm '" + alarm.name + "': " + UA_StatusCode_name(sc), LogLevel::ERRORS);
                }
            }
        }
        
        log("✓ Created " + std::to_string(alarmsCreated) + " alarm conditions", LogLevel::INFO);
        
        // ====================================================================
        // STEP 5: Main worker loop - keep thread alive until session closes
        // ====================================================================
        log("✅ Worker ready for org '" + ctx->shortCode + "'. Entering main loop.", 
            LogLevel::INFO);
        
        while(!ctx->shouldStop.load()) {
            // Thread stays alive, handling periodic tasks if needed
            // MQTT messages are handled by global handler (filtered by namespace)
            
            // Wait for signal (immediate wake-up) or timeout (1s for periodic checks)
            std::unique_lock<std::mutex> lock(ctx->cvMutex);
            ctx->cv.wait_for(lock, std::chrono::seconds(1), [&]{ return ctx->shouldStop.load(); });
        }
        
    } catch(const std::exception& e) {
        log("❌ Worker error for org '" + ctx->shortCode + "': " + 
            std::string(e.what()), LogLevel::ERRORS);
    }
    
    log("🛑 [THREAD STOP] Worker thread stopped for org '" + ctx->shortCode + 
        "' (OrgID: " + std::to_string(ctx->orgId) + ")", LogLevel::INFO);

    // ====================================================================
    // RESOURCE CLEANUP (Prevent Memory Leaks)
    // ====================================================================
    log("🧹 Cleaning up resources for org '" + ctx->shortCode + "'", LogLevel::INFO);

    // 1. Unsubscribe from MQTT Topics
        if(!ctx->topics.empty()) {
            log("🧹 Batch unsubscribing from " + std::to_string(ctx->topics.size()) + " topics...", LogLevel::INFO);
            GlobalMQTT_UnsubscribeBatch(ctx->topics);
        }
        log("DEBUG: MQTT Unsubscribe complete. Clearing local maps...", LogLevel::DEBUG);

        // 2. Clear Alarm Map (Local)
        ctx->alarmMap.clear();
        log("DEBUG: Local Alarm Map cleared.", LogLevel::DEBUG);
        
        // 3. Remove from Global Maps (if we added them)
    // Also unsubscribe from Emitter topics used in alarms?! 
    // Those are dynamic. We should track them or rely on g_triggerToAlarmMap check?
    // Current implementation only tracks 'topics' (generic telemetry).
    // Alarm triggers (emitters) are effectively leaked subscriptions if unique.
    // Ideally we should track them in ctx context too.
    
    // 2. Clear Context Maps (Nodes)
    // ctx->nodeMap and ctx->alarmMap contain UA_NodeIds.
    // Since we allocated them (UA_NODEID_STRING_ALLOC etc), we should verify if they need clearing.
    // The UA_NodeId struct simply holds a pointer. The Server/AddressSpace owns the node content, 
    // but the NodeId struct in our map might own a string copy.
    // Answer: Yes, UA_NODEID_STRING_ALLOC allocates memory for the identifier.
    
    // 2. Clear Context Maps (Nodes)
    // MOVED: nodeMap clearing is now done later to ensure UA_Server_deleteNode is called first.
    // ctx->nodeMap.clear(); // REMOVED PREMATURE CLEAR

    for(auto& pair : ctx->alarmMap) {
         UA_NodeId_clear(&pair.second);
    }
    ctx->alarmMap.clear();

    // 3. Remove from Global Maps (if we added them)
    // We added to 'nodeMap' (global) and 'g_alarmByKey' and 'topicMap'.
    {
        std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
        for(const auto& topic : ctx->topics) {
             auto it = nodeMap.find(topic);
             if(it != nodeMap.end()) {
                 // CRITICAL FIX: Delete the node from the SERVER address space!
                 // Just clearing the NodeId struct leaks the actual node in the server.
                 UA_Server_deleteNode(server, it->second, true); // true = delete references

                 UA_NodeId_clear(&it->second);
                 nodeMap.erase(it);
             }
        }
    }

    // Also delete the Organization Root Folder (Recursively cleans up if children were missed)
    if(!UA_NodeId_isNull(&orgRootFolder)) {
        log("🧹 Deleting Org Root Folder from server...", LogLevel::INFO);
        UA_Server_deleteNode(server, orgRootFolder, true);
        UA_NodeId_clear(&orgRootFolder);
    }
    
    // Safety: Iterate local nodeMap and try to delete any remaining nodes
    // (In case they weren't in global map or under root folder)
    for(auto& pair : ctx->nodeMap) {
        // Ignore error if already deleted via hierarchy or global map
        UA_Server_deleteNode(server, pair.second, true);
        UA_NodeId_clear(&pair.second);
    }
    ctx->nodeMap.clear();

    // REMOVED from topicMap (Generic Telemetry)
    {
        std::lock_guard<std::mutex> lock(g_topicMap_mutex);
        for(const auto& topic : ctx->topics) {
            topicMap.erase(topic);
        }
    }

    // 4. Cleanup Global Alarm Maps (Prevent Leaks)
    {
        // Cleanup g_alarmByKey
        // Note: g_alarmByKey values are allocated strings. We must clear them.
        // We use ctx->alarmMap keys to identify which ones we own/created.
        for(const auto& pair : ctx->alarmMap) {
            auto it = g_alarmByKey.find(pair.first);
            if(it != g_alarmByKey.end()) {
                UA_NodeId_clear(&it->second); // Clear the global NodeId copy
                g_alarmByKey.erase(it);
            }
        }
    }

    // 5. Cleanup Emitter Subscriptions and Trigger Mappings
    if(!ctx->subscribedEmitters.empty()) {
        log("🧹 Unsubscribing from " + std::to_string(ctx->subscribedEmitters.size()) + " emitter topics...", LogLevel::INFO);
        // Unsubscribe from MQTT
        GlobalMQTT_UnsubscribeBatch(ctx->subscribedEmitters);
        
        // Cleanup g_triggerToAlarmMap
        std::lock_guard<std::mutex> lock(g_alarmMutex);
        for(const auto& topic : ctx->subscribedEmitters) {
            auto mapIt = g_triggerToAlarmMap.find(topic);
            if(mapIt != g_triggerToAlarmMap.end()) {
                 // Remove entries associated with our alarms
                 auto& list = mapIt->second;
                 // Remove if alarmKey exists in our local alarmMap
                 auto originalSize = list.size();
                 list.erase(std::remove_if(list.begin(), list.end(), 
                     [&](const TriggerToAlarmMapping& m) {
                         return ctx->alarmMap.find(m.alarmKey) != ctx->alarmMap.end();
                     }), list.end());
                 
                 if(list.empty()) {
                     g_triggerToAlarmMap.erase(mapIt);
                 }
            }
        }
    }
    
    // Ideally ctx should track 'myAlarmKeys'.
    
    UA_NodeId_clear(&orgRootFolder);
    log("✓ Cleanup complete for org '" + ctx->shortCode + "'", LogLevel::INFO);
}
