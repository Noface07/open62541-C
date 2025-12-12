#include "SessionManager.h"
#include "Logger.h"
#include "fetchAPI.h"
#include "AlarmConfig.h"
#include "AandC.h"
#include <nlohmann/json.hpp>
#include <future>
#include <map>
#include <sstream>
#include "AandC.h"

using json = nlohmann::ordered_json;

extern std::unordered_map<std::string, UA_NodeId> g_alarmByKey;
extern void GlobalMQTT_Subscribe(const std::string &topic);
extern std::map<std::string, UA_NodeId> nodeMap;
extern std::mutex g_nodeMap_mutex;

// External callback from server.cpp
extern void writeCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
              const UA_NodeId *nodeId, void *nodeContext, const UA_NumericRange *range,
              const UA_DataValue *data);

// ============================================================================
// Helper Functions for Alarm Creation
// ============================================================================

static UA_NodeId findChildNodeIdAnyNS(UA_Server *server, UA_NodeId parentId, const char *searchName) {
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

static UA_NodeId findNodeByPath(UA_Server *server, UA_NodeId startNode, const std::vector<const char*>& path) {
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

static void setStealthValueByPath(UA_Server *server, UA_NodeId baseNode, 
                                 std::vector<const char*> path, 
                                 void *newValue, const UA_DataType *type) {
    UA_NodeId targetNode = findNodeByPath(server, baseNode, path);
    if(UA_NodeId_isNull(&targetNode)) return;
    
    UA_Variant val;
    UA_Variant_init(&val);
    UA_Variant_setScalar(&val, newValue, type);
    UA_Server_writeValue(server, targetNode, val);
}

static void setStealthValueChecked(UA_Server *server, UA_NodeId baseNode, 
                                  const char* name, 
                                  void *newValue, const UA_DataType *type) {
    setStealthValueByPath(server, baseNode, {name}, newValue, type);
}

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
    
    UA_StatusCode rc = UA_Server_addObjectNode(
        server, folderId, parent,
        UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
        UA_QUALIFIEDNAME_ALLOC(namespaceIndex, displayName.c_str()),
        UA_NODEID_NUMERIC(0, UA_NS0ID_FOLDERTYPE),
        objAttr, NULL, NULL);
    
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
        
        auto futureResponse = std::async(std::launch::async, getHierarchy,
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
        
        UA_NodeId orgRootFolder = UA_NODEID_STRING_ALLOC(ctx->namespaceIndex, 
                                    ("OrgRoot_" + ctx->shortCode).c_str());
        
        UA_StatusCode rc = UA_Server_addObjectNode(
            server, orgRootFolder,
            UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
            UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
            UA_QUALIFIEDNAME_ALLOC(ctx->namespaceIndex, ctx->shortCode.c_str()),
            UA_NODEID_NUMERIC(0, UA_NS0ID_FOLDERTYPE),
            rootObjAttr, NULL, NULL);
        
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
            
            rc = UA_Server_addVariableNode(
                server, nodeId, parent,
                UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
                UA_QUALIFIEDNAME_ALLOC(ctx->namespaceIndex, parts.back().c_str()),
                UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
                attr, NULL, NULL);

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
                GlobalMQTT_Subscribe(ns);
                nodesCreated++;
                
                // Add EURange property if available
                if(item.contains("rangeMin") && item.contains("rangeMax")) {
                    UA_Range range;
                    range.low = item["rangeMin"].get<double>();
                    range.high = item["rangeMax"].get<double>();
                    
                    UA_Variant rangeVariant;
                    UA_Variant_setScalar(&rangeVariant, &range, &UA_TYPES[UA_TYPES_RANGE]);
                    
                    UA_VariableAttributes rangeAttr = UA_VariableAttributes_default;
                    rangeAttr.value = rangeVariant;
                    rangeAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "EURange");
                    
                    UA_NodeId rangeNodeId = UA_NODEID_NUMERIC(ctx->namespaceIndex, tagId * 1000 + 1);
                    
                    UA_Server_addVariableNode(
                        server, rangeNodeId, nodeId,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                        UA_QUALIFIEDNAME_ALLOC(0, "EURange"),
                        UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                        rangeAttr, NULL, NULL);
                }
                
                // Add alarm limits if available
                if(item.contains("alarmHiHi")) {
                    UA_Double alarmHiHi = item["alarmHiHi"].get<double>();
                    UA_Variant alarmVariant;
                    UA_Variant_setScalar(&alarmVariant, &alarmHiHi, &UA_TYPES[UA_TYPES_DOUBLE]);
                    
                    UA_VariableAttributes alarmAttr = UA_VariableAttributes_default;
                    alarmAttr.value = alarmVariant;
                    alarmAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "AlarmHiHi");
                    
                    UA_NodeId alarmNodeId = UA_NODEID_NUMERIC(ctx->namespaceIndex, tagId * 1000 + 2);
                    UA_Server_addVariableNode(
                        server, alarmNodeId, nodeId,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                        UA_QUALIFIEDNAME_ALLOC(0, "AlarmHiHi"),
                        UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                        alarmAttr, NULL, NULL);
                }
                
                // Add engineering unit if available
                if(item.contains("measurmentUnitType")) {
                    UA_String unit = UA_STRING_ALLOC(item["measurmentUnitType"].get<std::string>().c_str());
                    UA_Variant unitVariant;
                    UA_Variant_setScalar(&unitVariant, &unit, &UA_TYPES[UA_TYPES_STRING]);
                    
                    UA_VariableAttributes unitAttr = UA_VariableAttributes_default;
                    unitAttr.value = unitVariant;
                    unitAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "EngineeringUnit");
                    
                    UA_NodeId unitNodeId = UA_NODEID_NUMERIC(ctx->namespaceIndex, tagId * 1000 + 6);
                    UA_Server_addVariableNode(
                        server, unitNodeId, nodeId,
                        UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                        UA_QUALIFIEDNAME_ALLOC(0, "EngineeringUnit"),
                        UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                        unitAttr, NULL, NULL);
                }
            }
        }
        
        log("✓ Created " + std::to_string(nodesCreated) + " variable nodes for org '" + 
            ctx->shortCode + "' in namespace " + std::to_string(ctx->namespaceIndex) + 
            " ('" + ctx->namespaceUri + "')", LogLevel::INFO);
        
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
                                  
                                  g_triggerToAlarmMap[topic].push_back(mapping);
                             }
                             // Populate global lookup map
                             // Allocate a fresh NodeId to ensure validity (avoid dangling pointers from createCondition outputs)
                             UA_NodeId safeAlarmId = UA_NODEID_STRING_ALLOC(ctx->namespaceIndex, alarmKey.c_str());
                             g_alarmByKey[alarmKey] = safeAlarmId;
                         }


                         // via logic in perform_subscriptions (server.cpp) IF the topic is in the map.
                         log("DEBUG: Subscribing to Emitter Topic (Alarm Prepared): " + topic, LogLevel::INFO);
                         GlobalMQTT_Subscribe(topic);
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
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
    } catch(const std::exception& e) {
        log("❌ Worker error for org '" + ctx->shortCode + "': " + 
            std::string(e.what()), LogLevel::ERRORS);
    }
    
    log("🛑 [THREAD STOP] Worker thread stopped for org '" + ctx->shortCode + 
        "' (OrgID: " + std::to_string(ctx->orgId) + ")", LogLevel::INFO);
}
