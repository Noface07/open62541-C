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
extern std::mutex g_alarmMutex;

// Extern declarations for MQTT Globals (Required for batch filtering in SessionWorker)
#include <deque>
#include <unordered_set>
extern std::mutex g_sub_mutex;
extern std::deque<std::string> g_subscription_queue;
extern std::unordered_set<std::string> g_pending_subscriptions; // Set for O(1) queue lookups
extern std::unordered_set<std::string> g_subscribed_topics;
extern std::deque<std::string> g_unsubscription_queue;

// External callback from server.cpp
extern void writeCallback(UA_Server *server, const UA_NodeId *sessionId, void *sessionContext,
              const UA_NodeId *nodeId, void *nodeContext, const UA_NumericRange *range,
              const UA_DataValue *data);

// ============================================================================
// STRUCTURAL CONCURRENCY: Job Queue API
// ============================================================================
#include <functional>
enum class ServerJobType {
    AddNamespace,
    AddNodes,
    AddAlarms,
    WriteValue,
    SetEventNotifier,
    Custom
};
extern void enqueueServerJob(const std::function<void(UA_Server*)>& fn, ServerJobType type = ServerJobType::Custom);
// ============================================================================


// ============================================================================
// GLOBAL API CACHE (In-Memory)
// ============================================================================
#include "RedisClient.h"


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
// Safe lifecycle management via shared_ptr
void sessionWorkerThread(std::shared_ptr<SessionContext> ctx, UA_Server* server,
                        const std::string& bearerToken,
                        const std::string& apiHost, const std::string& apiPort) {
    
    // Set thread name for debugging
    std::string threadName = "Worker_" + ctx->shortCode; // Corrected to be syntactically valid
    
    log("🚀 [THREAD START] Worker thread started for org '" + ctx->shortCode + 
        "' (OrgID: " + std::to_string(ctx->orgId) + ", Namespace: " + 
        ctx->namespaceUri + ")", LogLevel::INFO);
    
    UA_NodeId orgRootFolder = UA_NODEID_NULL; // Declared outside for cleanup
    
    try {
        // ====================================================================
        // STEP 1: Register dynamic namespace (Already done in SessionManager)
        // ====================================================================
        log("✓ Namespace '" + ctx->namespaceUri + "' confirmed at index " + 
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
        
        json topicResponse;
        bool cacheHit = false;
        std::string cacheKey = "TOPIC_LIST_" + std::to_string(ctx->orgId);

        if(true) { // Always check cache if enabled via RedisClient connection
             auto start_time = std::chrono::steady_clock::now();
             auto cachedVal = g_redisClient.get(cacheKey);
             auto end_time = std::chrono::steady_clock::now();
             auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

             if(cachedVal) {
                 try {
                     topicResponse = json::parse(*cachedVal);
                     cacheHit = true;
                     log("⚡ Redis Cache HIT for Topics (OrgID: " + std::to_string(ctx->orgId) + ") - Fetched in " + std::to_string(elapsed_ms) + " ms", LogLevel::INFO);
                 } catch(const std::exception& e) {
                     log("⚠️ Redis Cache Parse Error: " + std::string(e.what()), LogLevel::WARNING);
                 }
             } else {
                 if(g_redisClient.isConnected())
                    log("📉 Redis Cache MISS for Topics (OrgID: " + std::to_string(ctx->orgId) + ") - Checked in " + std::to_string(elapsed_ms) + " ms", LogLevel::INFO);
             }
        }

        if(!cacheHit) {
            auto futureResponse =
                std::async(std::launch::async, getResponse,
                                            apiHost, apiPort, bearerToken,
                                            topicJsonBody, "/api/GetTopicList");
            
            // Responsive wait for API response
            while(futureResponse.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
                if(ctx->shouldStop.load()) {
                    log("🛑 Worker stop signal received during Topic Fetch. Aborting...", LogLevel::WARNING);
                    throw std::runtime_error("Session cancelled by user");
                }
            }
            topicResponse = futureResponse.get();
            
            // Store in Redis (TTL 10 mins = 600s)
            g_redisClient.set(cacheKey, topicResponse.dump(), 600);
        }

        // Check again immediately after getting result
        if(ctx->shouldStop.load()) throw std::runtime_error("Session cancelled by user");
        
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
        // STEP 3: Create Address Space for Topics (Via Job Queue)
        // ====================================================================
        // ====================================================================
        // STEP 3: Create Address Space for Topics (Via Job Queue - BATCHED)
        // ====================================================================
        size_t totalTopics = topicResponse["data"].size();
        log("📦 Enqueuing Address Space Creation for " + std::to_string(totalTopics) + 
            " topics (Batched)...", LogLevel::INFO);

        // Pre-create Root Folder in a separate single job to ensure it exists for subsequent batches
        std::shared_ptr<SessionContext> rootJobCtx = ctx;
        enqueueServerJob([rootJobCtx](UA_Server* server) {
            UA_ObjectAttributes rootObjAttr = UA_ObjectAttributes_default;
            rootObjAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", rootJobCtx->shortCode.c_str());
            rootObjAttr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", 
                                        ("Root folder for organization: " + rootJobCtx->shortCode).c_str());
            
            UA_NodeId orgRootFolder = UA_NODEID_STRING_ALLOC(rootJobCtx->namespaceIndex, 
                                        ("OrgRoot_" + rootJobCtx->shortCode).c_str());
            
            UA_QualifiedName rootName = UA_QUALIFIEDNAME_ALLOC(rootJobCtx->namespaceIndex, rootJobCtx->shortCode.c_str());
            
            // Try to add, ignore if exists
            UA_StatusCode rc = UA_Server_addObjectNode(
                server, orgRootFolder,
                UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
                UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
                rootName,
                UA_NODEID_NUMERIC(0, UA_NS0ID_FOLDERTYPE),
                rootObjAttr, NULL, NULL);
                
             UA_QualifiedName_clear(&rootName);
             UA_ObjectAttributes_clear(&rootObjAttr);
             UA_NodeId_clear(&orgRootFolder);
        }, ServerJobType::AddNodes);


        // Batch Processing
        const size_t BATCH_SIZE = 50;
        std::vector<json> currentBatch;
        currentBatch.reserve(BATCH_SIZE);

        for(const auto& item : topicResponse["data"]) {
             currentBatch.push_back(item);
             
             if(currentBatch.size() >= BATCH_SIZE) {
                 // Enqueue Batch
                 std::shared_ptr<SessionContext> jobCtx = ctx;
                 std::vector<json> jobBatch = std::move(currentBatch); // Move batch to avoid copy
                 
                 enqueueServerJob([jobCtx, jobBatch](UA_Server* server) {
                     if(jobCtx->shouldStop.load()) return;
                     
                     // Reconstruct Root NodeId (It exists now)
                     UA_NodeId orgRootFolder = UA_NODEID_STRING_ALLOC(jobCtx->namespaceIndex, 
                                                ("OrgRoot_" + jobCtx->shortCode).c_str());
                                                
                     std::map<std::string, UA_NodeId> folderMap; // Local cache for this batch
                     
                     for(const auto& item : jobBatch) {
                         // ... (Process Item Logic) ...
                        if(!item.contains("namespace") || !item.contains("tagId")) continue;
                        
                        std::string ns = item["namespace"].get<std::string>();
                        auto parts = split(ns, '/');
                        if(parts.empty()) continue;
                        
                        // Recursively create folders
                        std::string currentPath;
                        UA_NodeId parent = orgRootFolder;
                        
                        for(size_t i = 0; i < parts.size() - 1; i++) {
                            if(!currentPath.empty()) currentPath += "/";
                            currentPath += parts[i];
                            parent = getOrCreateFolder(server, currentPath, parts[i], 
                                                    parent, jobCtx->namespaceIndex, folderMap, jobCtx->nodeMap);
                        }
                        
                        // Create Variable
                        UA_VariableAttributes attr = UA_VariableAttributes_default;
                        UA_Int32 value = 0;
                        UA_Variant_setScalarCopy(&attr.value, &value, &UA_TYPES[UA_TYPES_INT32]);
                        attr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", parts.back().c_str());
                        if(item.contains("name")) {
                            attr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", item["name"].get<std::string>().c_str());
                        }
                        attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
                        
                        int tagId = item["tagId"].get<int>();
                        UA_NodeId nodeId = UA_NODEID_NUMERIC(jobCtx->namespaceIndex, tagId);
                        UA_QualifiedName nodeName = UA_QUALIFIEDNAME_ALLOC(jobCtx->namespaceIndex, parts.back().c_str());
                        
                        UA_StatusCode rc = UA_Server_addVariableNode(server, nodeId, parent,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
                            nodeName, UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
                            attr, NULL, NULL);
                        
                        UA_QualifiedName_clear(&nodeName);
                        if(rc == UA_STATUSCODE_GOOD) {
                            UA_ValueCallback callback;
                            callback.onRead = NULL;
                            callback.onWrite = writeCallback;
                            UA_Server_setVariableNode_valueCallback(server, nodeId, callback);
                        }
                        UA_VariableAttributes_clear(&attr);
                        
                        if(rc == UA_STATUSCODE_GOOD || rc == UA_STATUSCODE_BADNODEIDEXISTS) {
                            // Update Local Map (Thread-safe: Only Server Thread writes, sequential jobs read)
                            jobCtx->nodeMap[ns] = nodeId; 
                            
                            // Update Global Map (Protected, used by Generic Telemetry)
                            {
                                std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                                nodeMap[ns] = nodeId; 
                            }
                            
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
                                
                                UA_NodeId rangeNodeId = UA_NODEID_NUMERIC(jobCtx->namespaceIndex, tagId * 1000 + 1);
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
                                
                                UA_NodeId alarmNodeId = UA_NODEID_NUMERIC(jobCtx->namespaceIndex, tagId * 1000 + 2);
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
                                
                                UA_NodeId unitNodeId = UA_NODEID_NUMERIC(jobCtx->namespaceIndex, tagId * 1000 + 6);
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
                     
                     UA_NodeId_clear(&orgRootFolder);
                 }, ServerJobType::AddNodes);
                 
                 currentBatch.clear();
             }
        }
        
        // Enqueue remaining
        if(!currentBatch.empty()) {
             std::shared_ptr<SessionContext> jobCtx = ctx;
             std::vector<json> jobBatch = currentBatch;
             enqueueServerJob([jobCtx, jobBatch](UA_Server* server) {
                 if(jobCtx->shouldStop.load()) return;
                 UA_NodeId orgRootFolder = UA_NODEID_STRING_ALLOC(jobCtx->namespaceIndex, ("OrgRoot_" + jobCtx->shortCode).c_str());
                 std::map<std::string, UA_NodeId> folderMap;
                 for(const auto& item : jobBatch) {
                        if(!item.contains("namespace") || !item.contains("tagId")) continue;
                        std::string ns = item["namespace"].get<std::string>();
                        auto parts = split(ns, '/');
                        if(parts.empty()) continue;
                        std::string currentPath;
                        UA_NodeId parent = orgRootFolder;
                        for(size_t i = 0; i < parts.size() - 1; i++) {
                            if(!currentPath.empty()) currentPath += "/";
                            currentPath += parts[i];
                            parent = getOrCreateFolder(server, currentPath, parts[i], parent, jobCtx->namespaceIndex, folderMap, jobCtx->nodeMap);
                        }
                        UA_VariableAttributes attr = UA_VariableAttributes_default;
                        UA_Int32 value = 0;
                        UA_Variant_setScalarCopy(&attr.value, &value, &UA_TYPES[UA_TYPES_INT32]);
                        attr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", parts.back().c_str());
                        if(item.contains("name")) attr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", item["name"].get<std::string>().c_str());
                        attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
                        int tagId = item["tagId"].get<int>();
                        UA_NodeId nodeId = UA_NODEID_NUMERIC(jobCtx->namespaceIndex, tagId);
                        UA_QualifiedName nodeName = UA_QUALIFIEDNAME_ALLOC(jobCtx->namespaceIndex, parts.back().c_str());
                        UA_StatusCode rc = UA_Server_addVariableNode(server, nodeId, parent, UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES), nodeName, UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE), attr, NULL, NULL);
                        UA_QualifiedName_clear(&nodeName);
                        if(rc == UA_STATUSCODE_GOOD) {
                            UA_ValueCallback callback;
                            callback.onRead = NULL;
                            callback.onWrite = writeCallback;
                            UA_Server_setVariableNode_valueCallback(server, nodeId, callback);
                        }
                        UA_VariableAttributes_clear(&attr);
                        if(rc == UA_STATUSCODE_GOOD || rc == UA_STATUSCODE_BADNODEIDEXISTS) {
                            {
                                std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                                jobCtx->nodeMap[ns] = nodeId; 
                                nodeMap[ns] = nodeId; 
                            }
                            if(item.contains("rangeMin") && item.contains("rangeMax")) {
                                UA_Range range; range.low = item["rangeMin"].get<double>(); range.high = item["rangeMax"].get<double>();
                                UA_Variant rangeVariant; UA_Variant_setScalarCopy(&rangeVariant, &range, &UA_TYPES[UA_TYPES_RANGE]);
                                UA_VariableAttributes rangeAttr = UA_VariableAttributes_default; rangeAttr.value = rangeVariant; rangeAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "EURange");
                                UA_NodeId rangeNodeId = UA_NODEID_NUMERIC(jobCtx->namespaceIndex, tagId * 1000 + 1);
                                UA_QualifiedName rangeName = UA_QUALIFIEDNAME_ALLOC(0, "EURange");
                                UA_Server_addVariableNode(server, rangeNodeId, nodeId, UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY), rangeName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE), rangeAttr, NULL, NULL);
                                UA_QualifiedName_clear(&rangeName); UA_VariableAttributes_clear(&rangeAttr);
                            }
                            if(item.contains("alarmHiHi")) {
                                UA_Double alarmHiHi = item["alarmHiHi"].get<double>();
                                UA_Variant alarmVariant; UA_Variant_setScalarCopy(&alarmVariant, &alarmHiHi, &UA_TYPES[UA_TYPES_DOUBLE]);
                                UA_VariableAttributes alarmAttr = UA_VariableAttributes_default; alarmAttr.value = alarmVariant; alarmAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "AlarmHiHi");
                                UA_NodeId alarmNodeId = UA_NODEID_NUMERIC(jobCtx->namespaceIndex, tagId * 1000 + 2);
                                UA_QualifiedName alarmName = UA_QUALIFIEDNAME_ALLOC(0, "AlarmHiHi");
                                UA_Server_addVariableNode(server, alarmNodeId, nodeId, UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY), alarmName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE), alarmAttr, NULL, NULL);
                                UA_QualifiedName_clear(&alarmName); UA_VariableAttributes_clear(&alarmAttr);
                            }
                            if(item.contains("measurmentUnitType")) {
                                UA_String unit = UA_STRING_ALLOC(item["measurmentUnitType"].get<std::string>().c_str());
                                UA_Variant unitVariant; UA_Variant_setScalarCopy(&unitVariant, &unit, &UA_TYPES[UA_TYPES_STRING]);
                                UA_VariableAttributes unitAttr = UA_VariableAttributes_default; unitAttr.value = unitVariant; unitAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "EngineeringUnit");
                                UA_NodeId unitNodeId = UA_NODEID_NUMERIC(jobCtx->namespaceIndex, tagId * 1000 + 6);
                                UA_QualifiedName unitName = UA_QUALIFIEDNAME_ALLOC(0, "EngineeringUnit");
                                UA_Server_addVariableNode(server, unitNodeId, nodeId, UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY), unitName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE), unitAttr, NULL, NULL);
                                UA_QualifiedName_clear(&unitName); UA_String_clear(&unit); UA_VariableAttributes_clear(&unitAttr);
                            }
                        }
                 }
                 UA_NodeId_clear(&orgRootFolder);
             }, ServerJobType::AddNodes);
        } 

        // Populate Global TopicMap for Generic Telemetry (Write Callback)
        {
            std::lock_guard<std::mutex> lock(g_topicMap_mutex);
            for(const auto& item : topicResponse["data"]) {
                if(!item.contains("namespace") || !item.contains("tagId")) continue;
                
                std::string ns = item["namespace"].get<std::string>();
                int tagId = item["tagId"].get<int>();
                
                TopicInfo info; // Use TopicInfo
                info.tagId = tagId;
                info.tagType = item.contains("tagType") ? item["tagType"].get<std::string>() : "Double";
                info.rangeMin = item.contains("rangeMin") ? item["rangeMin"].get<double>() : 0.0;
                info.rangeMax = item.contains("rangeMax") ? item["rangeMax"].get<double>() : 0.0;
                
                // Using global topicMap directly
                topicMap[ns] = info;
            }
        }
        
        // Log completion (approximate, since job is async)
        log("✓ Submitted Address Space Creation Job for " + ctx->shortCode, LogLevel::INFO);
            
        // 📡 BATCH SUBSCRIBE: Now that all nodes are created, perform subscription
        if(ctx->shouldStop.load()) throw std::runtime_error("Session cancelled before subscription");

        std::vector<std::string> topicsToSubscribe;
        topicsToSubscribe.reserve(ctx->topics.size());

        {
            std::lock_guard<std::mutex> lock(g_sub_mutex);
            for(const auto &topic : ctx->topics) {
                // Skip if already globally subscribed
                if(g_subscribed_topics.contains(topic)) {
                    continue;
                }

                // Skip if already queued for subscription
                // O(1) Check using Pending Set (Fixes CPU Spike)
                if(g_pending_subscriptions.contains(topic)) {
                    continue;
                }

                topicsToSubscribe.emplace_back(topic);
            }
        }

        if(!topicsToSubscribe.empty()) {
            log("📡 Batch subscribing to " + std::to_string(topicsToSubscribe.size()) +
                    " new topics (filtered from " + std::to_string(ctx->topics.size()) +
                    ")",
                LogLevel::INFO);

            GlobalMQTT_SubscribeBatch(topicsToSubscribe);
        }

        // } // UNLOCK SERVER MUTEX - REMOVED STRAY BRACE

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
        
        std::vector<AlarmConfig> alarms;
        bool alarmCacheHit = false;
        std::string alarmCacheKey = "ALARMS_" + std::to_string(ctx->orgId);

        if(true) { // Always check cache if enabled via RedisClient connection
             auto start_time = std::chrono::steady_clock::now();
             auto cachedVal = g_redisClient.get(alarmCacheKey);
             auto end_time = std::chrono::steady_clock::now();
             auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

             if(cachedVal) {
                 try {
                     alarms = ParseAlarmConfigFromJson(json::parse(*cachedVal));
                     alarmCacheHit = true;
                     log("⚡ Redis Cache HIT for Alarms (OrgID: " + std::to_string(ctx->orgId) + ") - Fetched in " + std::to_string(elapsed_ms) + " ms", LogLevel::INFO);
                 } catch(const std::exception& e) {
                      log("⚠️ Redis Cache Parse Error for Alarms: " + std::string(e.what()), LogLevel::WARNING);
                 }
             } else {
                 if(g_redisClient.isConnected())
                    log("📉 Redis Cache MISS for Alarms (OrgID: " + std::to_string(ctx->orgId) + ") - Checked in " + std::to_string(elapsed_ms) + " ms", LogLevel::INFO);
             }
        }

        if(!alarmCacheHit) {
             auto futureResponse =
                std::async(std::launch::async, getResponse,
                                            apiHost, apiPort, bearerToken,
                                            alarmJsonBody, "/api/GetAlarmsConfigDetailList");
            
            // Responsive wait
            while(futureResponse.wait_for(std::chrono::milliseconds(100)) != std::future_status::ready) {
                if(ctx->shouldStop.load()) {
                    log("🛑 Worker stop signal received during Alarm Fetch. Aborting...", LogLevel::WARNING);
                    throw std::runtime_error("Session cancelled by user");
                }
            }
            json alarmResponse = futureResponse.get();
            
            // Store in Redis
            g_redisClient.set(alarmCacheKey, alarmResponse.dump(), 600);
            
            alarms = ParseAlarmConfigFromJson(alarmResponse);
        }
        
        log("✓ Fetched " + std::to_string(alarms.size()) + " alarms for org '" + 
            ctx->shortCode + "' (OrgID: " + std::to_string(ctx->orgId) + ")", 
            LogLevel::INFO);
        
        // ====================================================================
        // STEP 5: Create Alarm Conditions (Via Job Queue - BATCHED)
        // ====================================================================
        log("📋 Enqueuing Alarm Creation for " + std::to_string(alarms.size()) + " alarm configs (Batched)...", LogLevel::INFO);
        
        const size_t ALARM_BATCH_SIZE = 20;
        std::vector<AlarmConfig> currentAlarmBatch;
        currentAlarmBatch.reserve(ALARM_BATCH_SIZE);

        for(const auto& alarm : alarms) {
            currentAlarmBatch.push_back(alarm);
            
            if(currentAlarmBatch.size() >= ALARM_BATCH_SIZE) {
                 // Enqueue Batch
                 std::shared_ptr<SessionContext> alarmJobCtx = ctx;
                 std::vector<AlarmConfig> jobAlarms = std::move(currentAlarmBatch);
                 
                enqueueServerJob([alarmJobCtx, jobAlarms](UA_Server* server) {
                    if(alarmJobCtx->shouldStop.load()) return;
                    
                    int alarmsCreated = 0;
                    for(const auto& alarm : jobAlarms) {
                        if(alarmJobCtx->shouldStop.load()) {
                             log("🛑 Job: Worker stop signal received during Alarm Creation. Aborting...", LogLevel::WARNING);
                             return;
                        }

                        if(!alarm.alarmEmitters.has_value()) continue;
                        
                        alarmsCreated++;
                        
                        for(const auto& emitter : alarm.alarmEmitters.value()) {
                            // Sanitize emitter name
                            std::string searchKey = emitter.emitterNodeName;
                            if(!searchKey.empty() && searchKey.back() == '/') {
                                searchKey.pop_back();
                            }

                            // Find emitter node
                            UA_NodeId sourceNode = UA_NODEID_NULL;
                            if(alarmJobCtx->nodeMap.count(searchKey)) { // Check local map
                                sourceNode = alarmJobCtx->nodeMap[searchKey];
                            } else {
                                std::lock_guard<std::mutex> lock(g_nodeMap_mutex);
                                if(nodeMap.count(searchKey)) sourceNode = nodeMap[searchKey];
                            }
                            
                            if(UA_NodeId_isNull(&sourceNode)) {
                                continue;
                            }

                            std::string alarmKey = emitter.emitterNodeName + "-" + alarm.name;
                            
                            // Deterministic NodeId (String)
                            UA_NodeId requestedNodeId = UA_NODEID_STRING_ALLOC(alarmJobCtx->namespaceIndex, alarmKey.c_str());
                            
                            // Set EventNotifier
                            UA_Byte eventNotifier = 0x01; 
                            UA_Server_writeEventNotifier(server, sourceNode, eventNotifier);

                            // Create condition
                            UA_NodeId alarmId = UA_NODEID_NULL;
                            UA_StatusCode sc = UA_Server_createCondition(
                                server, requestedNodeId,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE),
                                UA_QUALIFIEDNAME_ALLOC(alarmJobCtx->namespaceIndex, alarm.name.c_str()), 
                                sourceNode,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT),
                                &alarmId
                            );
                            
                            UA_NodeId_clear(&requestedNodeId);
                            
                            if(sc == UA_STATUSCODE_BADNODEIDEXISTS) {
                                 alarmId = UA_NODEID_STRING_ALLOC(alarmJobCtx->namespaceIndex, alarmKey.c_str());
                            } else if(sc == UA_STATUSCODE_GOOD) {
                                // Initialize alarm state
                                UA_Boolean enabled = alarm.enable ? UA_TRUE : UA_FALSE;
                                setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &enabled, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                UA_Boolean inactive = UA_FALSE;
                                setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                setStealthValueChecked(server, alarmId, "Severity", &inactive, &UA_TYPES[UA_TYPES_UINT16]); // 0 severity
                                // Methods
                                UA_NodeId methodId = findChildNodeIdAnyNS(server, alarmId, "Acknowledge");
                                if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customAcknowledgeCallback);
                                methodId = findChildNodeIdAnyNS(server, alarmId, "Confirm");
                                if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customConfirmCallback);
                                methodId = findChildNodeIdAnyNS(server, alarmId, "AddComment");
                                if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customAddCommentCallback);
                                methodId = findChildNodeIdAnyNS(server, alarmId, "Enable");
                                if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customEnableCallback);
                                methodId = findChildNodeIdAnyNS(server, alarmId, "Disable");
                                if(!UA_NodeId_isNull(&methodId)) UA_Server_setMethodNodeCallback(server, methodId, customDisableCallback);
                            }
                            
                            // Populate Maps
                            if(!UA_NodeId_isNull(&alarmId)) {
                                // 1. Local Map
                                // 1. Local Map (Deep Copy)
                                UA_NodeId mapCopy;
                                UA_NodeId_copy(&alarmId, &mapCopy);
                                alarmJobCtx->alarmMap[alarmKey] = mapCopy;
                                
                                // Vector to collect subscriptions for THIS alarm
                                std::vector<std::string> pendingEventSubs; 

                                // 2. Global Maps (Protected)
                                {
                                    std::lock_guard<std::mutex> lock(g_alarmMutex);
                                    
                                    UA_NodeId globalId;
                                    UA_NodeId_copy(&alarmId, &globalId);
                                    
                                    if(g_alarmByKey.count(alarmKey)) {
                                         UA_NodeId_clear(&g_alarmByKey[alarmKey]);
                                    }
                                    g_alarmByKey[alarmKey] = globalId;

                                    // Populate g_triggerToAlarmMap with EMITTER Topic (e.g. TDSPL/JPR)
                                    // This allows alarms to be triggered by the device that emits them.
                                    std::string emitterTopic = searchKey; 
                                    if(!emitterTopic.empty()) {
                                         TriggerToAlarmMapping mapping;
                                         mapping.triggerTopic = emitterTopic;
                                         mapping.alarmKey = alarmKey;
                                         mapping.alarmId = alarm.id;
                                         mapping.alarmInstanceId = 0;
                                         mapping.triggerId = 0; // Default if no specific trigger

                                         // Use the first trigger ID if available, just in case
                                         if(alarm.alarmTriggers.has_value() && !alarm.alarmTriggers.value().empty()) {
                                              mapping.triggerId = alarm.alarmTriggers.value()[0].id;
                                         }
                                         
                                         // Deduplicate
                                         bool exists = false;
                                         for(const auto& m : g_triggerToAlarmMap[mapping.triggerTopic]) {
                                             if(m.alarmKey == mapping.alarmKey && m.alarmId == mapping.alarmId) { exists = true; break; }
                                         }
                                         if(!exists) {
                                            g_triggerToAlarmMap[mapping.triggerTopic].push_back(mapping);
                                            log("DEBUG: Mapped Emitter '" + mapping.triggerTopic + "' -> Alarm " + std::to_string(mapping.alarmId), LogLevel::INFO);
                                         }
                                    }

                                    // Populate g_triggerToAlarmMap with EXPLICIT TRIGGER Topics
                                    if(alarm.alarmTriggers.has_value()) {
                                        for(const auto& trigger : alarm.alarmTriggers.value()) {
                                            if(trigger.topic.empty() || trigger.topic == emitterTopic) continue; // Skip empty or if already mapped as emitter

                                            TriggerToAlarmMapping mapping;
                                            mapping.triggerTopic = trigger.topic; // The actual trigger topic!
                                            mapping.alarmKey = alarmKey;
                                            mapping.alarmId = alarm.id;
                                            mapping.alarmInstanceId = 0;
                                            mapping.triggerId = trigger.id;
                                            
                                            // Deduplicate
                                            bool exists = false;
                                            for(const auto& m : g_triggerToAlarmMap[mapping.triggerTopic]) {
                                                 if(m.alarmKey == mapping.alarmKey && m.alarmId == mapping.alarmId) { exists = true; break; }
                                            }
                                            if(!exists) {
                                                g_triggerToAlarmMap[mapping.triggerTopic].push_back(mapping);
                                            }
                                        }
                                    }
                                } // End Lock Scope

                                // ------------------------------------------------------------------
                                // 3. Subscribe to /Event topics (Safe outside lock)
                                // ------------------------------------------------------------------
                                std::string emitterTopic = searchKey;
                                if(!emitterTopic.empty()) {
                                    pendingEventSubs.push_back(emitterTopic + "/Event");
                                }
                                if(alarm.alarmTriggers.has_value()) {
                                    for(const auto& trigger : alarm.alarmTriggers.value()) {
                                        if(!trigger.topic.empty()) {
                                            pendingEventSubs.push_back(trigger.topic + "/Event");
                                        }
                                    }
                                }
                                
                                if(!pendingEventSubs.empty()) {
                                    GlobalMQTT_SubscribeBatch(pendingEventSubs);
                                }
                                
                                UA_NodeId_clear(&alarmId);
                            }
                        }
                    }
                }, ServerJobType::AddAlarms);

                currentAlarmBatch.clear();
            }
        }
        
        // Enqueue remaining
        if(!currentAlarmBatch.empty()) {
             std::shared_ptr<SessionContext> alarmJobCtx = ctx;
             std::vector<AlarmConfig> jobAlarms = std::move(currentAlarmBatch);
             enqueueServerJob([alarmJobCtx, jobAlarms](UA_Server* server) {
                  if(alarmJobCtx->shouldStop.load()) return;
                  for(const auto& alarm : jobAlarms) {
                        if(!alarm.alarmEmitters.has_value()) continue;
                        for(const auto& emitter : alarm.alarmEmitters.value()) {
                            std::string searchKey = emitter.emitterNodeName;
                            if(!searchKey.empty() && searchKey.back() == '/') searchKey.pop_back();
                            UA_NodeId sourceNode = UA_NODEID_NULL;
                            if(alarmJobCtx->nodeMap.count(searchKey)) sourceNode = alarmJobCtx->nodeMap[searchKey];
                            else { std::lock_guard<std::mutex> lock(g_nodeMap_mutex); if(nodeMap.count(searchKey)) sourceNode = nodeMap[searchKey]; }
                            if(UA_NodeId_isNull(&sourceNode)) continue;
                            std::string alarmKey = emitter.emitterNodeName + "-" + alarm.name;
                            UA_NodeId requestedNodeId = UA_NODEID_STRING_ALLOC(alarmJobCtx->namespaceIndex, alarmKey.c_str());
                            UA_Byte eventNotifier = 0x01; UA_Server_writeEventNotifier(server, sourceNode, eventNotifier);
                            UA_NodeId alarmId = UA_NODEID_NULL;
                            UA_StatusCode sc = UA_Server_createCondition(server, requestedNodeId, UA_NODEID_NUMERIC(0, UA_NS0ID_EXCLUSIVELIMITALARMTYPE), UA_QUALIFIEDNAME_ALLOC(alarmJobCtx->namespaceIndex, alarm.name.c_str()), sourceNode, UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT), &alarmId);
                            UA_NodeId_clear(&requestedNodeId);
                            if(sc == UA_STATUSCODE_BADNODEIDEXISTS) alarmId = UA_NODEID_STRING_ALLOC(alarmJobCtx->namespaceIndex, alarmKey.c_str());
                            else if(sc == UA_STATUSCODE_GOOD) {
                                UA_Boolean enabled = alarm.enable ? UA_TRUE : UA_FALSE; setStealthValueByPath(server, alarmId, {"EnabledState", "Id"}, &enabled, &UA_TYPES[UA_TYPES_BOOLEAN]);
                                UA_Boolean inactive = UA_FALSE; setStealthValueByPath(server, alarmId, {"ActiveState", "Id"}, &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]); setStealthValueByPath(server, alarmId, {"AckedState", "Id"}, &inactive, &UA_TYPES[UA_TYPES_BOOLEAN]); setStealthValueChecked(server, alarmId, "Severity", &inactive, &UA_TYPES[UA_TYPES_UINT16]);
                                UA_NodeId mId = findChildNodeIdAnyNS(server, alarmId, "Acknowledge"); if(!UA_NodeId_isNull(&mId)) UA_Server_setMethodNodeCallback(server, mId, customAcknowledgeCallback);
                                mId = findChildNodeIdAnyNS(server, alarmId, "Confirm"); if(!UA_NodeId_isNull(&mId)) UA_Server_setMethodNodeCallback(server, mId, customConfirmCallback);
                                mId = findChildNodeIdAnyNS(server, alarmId, "AddComment"); if(!UA_NodeId_isNull(&mId)) UA_Server_setMethodNodeCallback(server, mId, customAddCommentCallback);
                                mId = findChildNodeIdAnyNS(server, alarmId, "Enable"); if(!UA_NodeId_isNull(&mId)) UA_Server_setMethodNodeCallback(server, mId, customEnableCallback);
                                mId = findChildNodeIdAnyNS(server, alarmId, "Disable"); if(!UA_NodeId_isNull(&mId)) UA_Server_setMethodNodeCallback(server, mId, customDisableCallback);
                            }
                            if(!UA_NodeId_isNull(&alarmId)) {
                                UA_NodeId mapCopy;
                                UA_NodeId_copy(&alarmId, &mapCopy);
                                alarmJobCtx->alarmMap[alarmKey] = mapCopy;
                                
                                // Vector to collect subscriptions for THIS alarm
                                std::vector<std::string> pendingEventSubs; 

                                // 2. Global Maps (Protected)
                                {
                                    std::lock_guard<std::mutex> lock(g_alarmMutex);
                                    UA_NodeId globalId; UA_NodeId_copy(&alarmId, &globalId);
                                    if(g_alarmByKey.count(alarmKey)) UA_NodeId_clear(&g_alarmByKey[alarmKey]);
                                    g_alarmByKey[alarmKey] = globalId;

                                    std::string emitterTopic = searchKey;
                                    if(!emitterTopic.empty()) {
                                         TriggerToAlarmMapping mapping;
                                         mapping.triggerTopic = emitterTopic;
                                         mapping.alarmKey = alarmKey;
                                         mapping.alarmId = alarm.id;
                                         mapping.alarmInstanceId = 0;
                                         mapping.triggerId = 0; 

                                         if(alarm.alarmTriggers.has_value() && !alarm.alarmTriggers.value().empty()) {
                                              mapping.triggerId = alarm.alarmTriggers.value()[0].id;
                                         }
                                         
                                         bool exists = false;
                                         for(const auto& m : g_triggerToAlarmMap[mapping.triggerTopic]) {
                                             if(m.alarmKey == mapping.alarmKey && m.alarmId == mapping.alarmId) { exists = true; break; }
                                         }
                                         if(!exists) {
                                            g_triggerToAlarmMap[mapping.triggerTopic].push_back(mapping);
                                            log("DEBUG: Mapped Emitter '" + mapping.triggerTopic + "' -> Alarm " + std::to_string(mapping.alarmId), LogLevel::INFO);
                                         }
                                    }

                                    if(alarm.alarmTriggers.has_value()) {
                                        for(const auto& trigger : alarm.alarmTriggers.value()) {
                                            if(trigger.topic.empty() || trigger.topic == emitterTopic) continue;

                                            TriggerToAlarmMapping mapping;
                                            mapping.triggerTopic = trigger.topic; 
                                            mapping.alarmKey = alarmKey;
                                            mapping.alarmId = alarm.id;
                                            mapping.alarmInstanceId = 0;
                                            mapping.triggerId = trigger.id;
                                            
                                            bool exists = false;
                                            for(const auto& m : g_triggerToAlarmMap[mapping.triggerTopic]) {
                                                 if(m.alarmKey == mapping.alarmKey && m.alarmId == mapping.alarmId) { exists = true; break; }
                                            }
                                            if(!exists) {
                                                g_triggerToAlarmMap[mapping.triggerTopic].push_back(mapping);
                                            }
                                        }
                                    }
                                }

                                // 3. Subscribe to /Event topics
                                std::string emitterTopic = searchKey;
                                if(!emitterTopic.empty()) {
                                    pendingEventSubs.push_back(emitterTopic + "/Event");
                                }
                                if(alarm.alarmTriggers.has_value()) {
                                    for(const auto& trigger : alarm.alarmTriggers.value()) {
                                        if(!trigger.topic.empty()) {
                                            pendingEventSubs.push_back(trigger.topic + "/Event");
                                        }
                                    }
                                }
                                
                                if(!pendingEventSubs.empty()) {
                                    GlobalMQTT_SubscribeBatch(pendingEventSubs);
                                }
                                
                                UA_NodeId_clear(&alarmId);
                            }
                        }
                  }
             }, ServerJobType::AddAlarms);
        }

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
    log("🧹 Cleanup will be handled by SessionContext destructor.", LogLevel::INFO);
    
    // Note: Actual cleanup of nodeMap, alarmMap, and global keys happens 
    // when the SessionContext shared_ptr refCount drops to zero.
}

