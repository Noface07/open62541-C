/* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
 * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

//  ./server D:\OPC UA Server\OPCUA- open 62451\open62541-C\certs\server_cert.der D:\OPC UA Server\OPCUA- open 62451\open62541-C\certs\server_key.der [trust1.der trust2.der ...]
//  ./server D:\OPC UA Server\OPCUA- open 62451\open62541-C\certs\server_cert.der D:\OPC UA Server\OPCUA- open 62451\open62541-C\certs\server_key.der

#include <open62541/server_config_default.h>
#include <open62541/plugin/log_stdout.h>
#include <open62541/plugin/securitypolicy_default.h>
#include <open62541/plugin/accesscontrol_default.h>
#include <open62541/plugin/securitypolicy.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <open62541/plugin/certificategroup_default.h>
#include <open62541/server.h>
#include <iostream>
#include <map>
#include <vector>
#include <string>
#include <sstream>

#include <pqxx/pqxx>

#include <async_mqtt/all.hpp>
#include <async_mqtt/asio_bind/predefined_layer/mqtts.hpp>
#include <async_mqtt/asio_bind/predefined_layer/ws.hpp> 
#include <async_mqtt/asio_bind/predefined_layer/wss.hpp>
#include <boost/asio.hpp>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <chrono>
#include <thread>
#include <future>

#include <open62541/client.h>
#include <open62541/client_config_default.h>

#include <open62541/plugin/historydata/history_data_backend_memory.h>
#include <open62541/plugin/historydata/history_data_gathering_default.h>
#include <open62541/plugin/historydata/history_database_default.h>
#include <open62541/plugin/historydata/history_data_backend.h>


#include <AandC.cpp>
#include "Logger.h"


#include <nlohmann/json.hpp>
using json = nlohmann::json;


using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;       
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;  
UA_Boolean running = true;

static UA_HistoryDataGathering *g_gathering = NULL;

// Define user credentials
static UA_UsernamePasswordLogin usernamePasswordLogin[2] = {
    {UA_STRING_STATIC("user1"), UA_STRING_STATIC("password1")},
    {UA_STRING_STATIC("user2"), UA_STRING_STATIC("password2")}
};

// Custom access control
// static UA_ByteString
// getPassword(const UA_String *userName, void *userContext) {
//     if(UA_String_equal(userName, &usernamePasswordLogin[0].username))
//         return usernamePasswordLogin[0].password;
//     if(UA_String_equal(userName, &usernamePasswordLogin[1].username))
//         return usernamePasswordLogin[1].password;
//     return UA_BYTESTRING_NULL;
// }



// Your custom logger callback
static void
myLog(void *context, UA_LogLevel level, UA_LogCategory category, const char *msg,
      va_list /*args*/) {

    // Never format with va_list to avoid specifier/argument mismatches. Just pass through.
    try {
        const char *text = msg ? msg : "";
        switch(level) {
            case UA_LOGLEVEL_FATAL:
            case UA_LOGLEVEL_ERROR:
                log(std::string(text), LogLevel::ERRORS);
                break;
            case UA_LOGLEVEL_WARNING:
                log(std::string(text), LogLevel::INFO);
                break;
            case UA_LOGLEVEL_INFO:
                log(std::string(text), LogLevel::INFO);
                break;
            case UA_LOGLEVEL_DEBUG:
                log(std::string(text), LogLevel::DEBUG);
                break;
        }
    } catch (...) {
        // Swallow all exceptions to avoid unwinding across C boundary
    }
}

// Custom logger plugin
static UA_Logger myLogger = {myLog, nullptr, nullptr};




static UA_StatusCode
myLoginCallback(const UA_String *username, const UA_ByteString *password,
                size_t usernamePasswordLoginSize,
                const UA_UsernamePasswordLogin *usernamePasswordLogin,
                void **sessionContext, void *loginContext) {
    // Safely convert username to string, avoiding problematic format specifiers
    std::string usernameStr;
    if (username && username->data && username->length > 0) {
        // Ensure we don't exceed buffer bounds
        size_t maxLen = std::min(username->length, (size_t)255);
        usernameStr.assign((char*)username->data, maxLen);
    } else {
        usernameStr = "unknown";
    }
    
    for(size_t i = 0; i < usernamePasswordLoginSize; i++) {
        if(UA_String_equal(username, &usernamePasswordLogin[i].username) &&
           UA_ByteString_equal(password, &usernamePasswordLogin[i].password)) {
            // Grant admin access to user1
            if(UA_String_equal(username, &usernamePasswordLogin[0].username)) {
                *sessionContext = (void*)1; // Mark as admin
                log("Admin user login successful: " + usernameStr, LogLevel::INFO);
            } else {
                log("Regular user login successful: " + usernameStr, LogLevel::INFO);
            }
            return UA_STATUSCODE_GOOD;
        }
    }
    
    log("Login failed for user: " + usernameStr, LogLevel::ERRORS);
    UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Login failed for user: %s",
                   usernameStr.c_str());
    return UA_STATUSCODE_BADUSERACCESSDENIED;
}


static void
stopHandler(int sign) {
    log("Received shutdown signal", LogLevel::INFO);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
    running = false;
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "stopping server");
}

static UA_ByteString loadFile(const char *path) {
    UA_ByteString fileContents = UA_BYTESTRING_NULL;
    FILE *fp = fopen(path, "rb");
    if(!fp)
        return fileContents;
    fseek(fp, 0, SEEK_END);
    fileContents.length = (size_t)ftell(fp);
    fileContents.data = (UA_Byte *)UA_malloc(fileContents.length * sizeof(UA_Byte));
    fseek(fp, 0, SEEK_SET);
    if(fread(fileContents.data, sizeof(UA_Byte), fileContents.length, fp) != fileContents.length) {
        UA_ByteString_clear(&fileContents);
    }
    fclose(fp);
    return fileContents;
}

#ifdef _WIN32
static size_t
loadCertsFromDirectory(const char *dirPath, UA_ByteString **certs) {
    char searchPath[512];
    snprintf(searchPath, sizeof(searchPath), "%s\\*.der", dirPath);

    WIN32_FIND_DATAA findData;
    HANDLE hFind = FindFirstFileA(searchPath, &findData);

    if (hFind == INVALID_HANDLE_VALUE) {
        return 0;
    }

    std::vector<std::string> derFiles;
    do {
        derFiles.push_back(findData.cFileName);
    } while (FindNextFileA(hFind, &findData) != 0);
    FindClose(hFind);

    size_t count = derFiles.size();
    if (count == 0) {
        return 0;
    }

    *certs = (UA_ByteString*)UA_malloc(sizeof(UA_ByteString) * count);
    for (size_t i = 0; i < count; ++i) {
        char fullpath[512];
        snprintf(fullpath, sizeof(fullpath), "%s\\%s", dirPath, derFiles[i].c_str());
        (*certs)[i] = loadFile(fullpath);
    }

    return count;
}
#else
/* Load all .der files from a directory */
static size_t
loadCertsFromDirectory(const char *dirPath, UA_ByteString **certs) {
    DIR *dir = opendir(dirPath);
    if(!dir) return 0;

    struct dirent *entry;
    std::vector<std::string> derFiles;
    while((entry = readdir(dir)) != NULL) {
        if(strstr(entry->d_name, ".der"))
            derFiles.push_back(entry->d_name);
    }
    
    size_t count = derFiles.size();
    if(count > 0) {
        *certs = (UA_ByteString*)UA_malloc(sizeof(UA_ByteString) * count);
        for(size_t i = 0; i < count; i++) {
            char fullpath[512];
            snprintf(fullpath, sizeof(fullpath), "%s/%s", dirPath, derFiles[i].c_str());
            (*certs)[i] = loadFile(fullpath);
        }
    }
    
    closedir(dir);
    return count;
}
#endif

static UA_NodeId *g_counterNodeId = NULL;
static UA_NodeId *g_eventNodeId = NULL;
int i=123;
static void updateCounterAndTriggerEvent(UA_Server *server, void *data) {
    // Update the counter value
    UA_Double newValue = ++i;
    UA_Variant value;
    UA_Variant_setScalar(&value, &newValue, &UA_TYPES[UA_TYPES_DOUBLE]);
    UA_Server_writeValue(server, *g_counterNodeId, value);

    // Trigger the event
    // Sleep(10000);
    // UA_Server_triggerEvent(server, *g_eventNodeId, UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER), NULL, UA_TRUE);
}






    map<string, UA_NodeId> nodeMap;
    struct TopicInfo {
        int tagId;
        string name;
        string tagType;
        double rangeMin;
        double rangeMax;
        // int source;
        // int infoId;
        // int quality;
        // int updateType;
    };


    unordered_map<string, TopicInfo> topicMap;

    vector<string> split(const string& s, char delimiter) {
    vector<string> tokens;
    stringstream ss(s);
    string item;
    while (std::getline(ss, item, delimiter)) {
        tokens.push_back(item);
    }
    return tokens;
}


    UA_NodeId getOrCreateFolder(UA_Server* server, const string& path, const string& name, UA_NodeId parent) {
    if (nodeMap.count(path)) return nodeMap[path];
    UA_ObjectAttributes oAttr = UA_ObjectAttributes_default;
    oAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", name.c_str());
    if(name == "PLANT-001") {
        oAttr.eventNotifier = 1;
    }
    UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(1, path.c_str());
    UA_QualifiedName qName = UA_QUALIFIEDNAME_ALLOC(1, name.c_str());
    UA_Server_addObjectNode(server, nodeId, parent, UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES), qName, UA_NODEID_NUMERIC(0, UA_NS0ID_FOLDERTYPE), oAttr, NULL, NULL);
    nodeMap[path] = nodeId;
    return nodeId;
}

as::io_context ioc;
using client_t = am::client<am::protocol_version::v5, am::protocol::mqtt>;
client_t amcl{ioc.get_executor()};



thread_local bool is_internal_write = false;


// To publish from any thread:
void
publish_to_mqtt(const std::string &topic, const std::string &payload) {
    as::post(ioc, [topic, payload]() {
        as::co_spawn(
            ioc,
            [topic, payload]() -> as::awaitable<void> {
                try {
                    co_await amcl.async_publish(topic, payload, am::qos::at_most_once);
                } catch(const std::exception &e) {
                    log("MQTT publish error: " + std::string(e.what()), LogLevel::ERRORS);
                }
                co_return;
            },
            as::detached);
    });
};



        static UA_NodeId findChildByBrowseName(UA_Server *server, UA_NodeId parent, char *childName) {
            UA_BrowseDescription bd;
            UA_BrowseDescription_init(&bd);
            bd.nodeId = parent;
            bd.resultMask = UA_BROWSERESULTMASK_ALL;
            bd.browseDirection = UA_BROWSEDIRECTION_FORWARD;
            bd.referenceTypeId = UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT);

            UA_BrowseResult bres = UA_Server_browse(server, 0, &bd);
            UA_NodeId result = UA_NODEID_NULL;
            for (size_t i = 0; i < bres.referencesSize; ++i) {
                UA_ReferenceDescription *ref = &bres.references[i];
                UA_String childNameStr = UA_STRING(childName);
                if(UA_String_equal(&ref->browseName.name, &childNameStr)) {
                    result = ref->nodeId.nodeId;
                    break;
                }
            }
            UA_BrowseResult_clear(&bres);
            return result;
        }





// Define a structure to hold method callback context
struct MethodCallbackContext {
    UA_NodeId ackedStateNodeId;
    MonitoredNodeAlarmInfo *alarmInfo;
};

// Add a flag to track if callback is already set up
// struct MonitoredNodeAlarmInfo {
//     UA_NodeId processNodeId;
//     UA_NodeId alarmInstanceId;
//     double alarmHiHi;
//     double alarmHi;
//     double alarmLo;
//     double alarmLoLo;
//     double deadband;
//     std::string displayName;
//     bool acked;
//     bool callbackSetup;  // Flag to track if method callback is already set up
// };

static UA_StatusCode
CustomAckCallback(UA_Server *server,
                    const UA_NodeId *sessionId,
                    void *sessionContext,
                    const UA_NodeId *methodId,
                    void *methodContext,
                    const UA_NodeId *objectId,
                    void *objectContext,
                    size_t inputSize,
                    const UA_Variant *input,
                    size_t outputSize,
                    UA_Variant *output) {

    // Get the context from methodContext
    MethodCallbackContext *context = (MethodCallbackContext*)methodContext;
    
    if(!context || !context->alarmInfo) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Invalid context in method callback");
        return UA_STATUSCODE_BADINTERNALERROR;
    }

    UA_NodeId *ackedStateNodeId = &context->ackedStateNodeId;
    MonitoredNodeAlarmInfo *info = context->alarmInfo;

    // 1. Set AckedState.Id = true
    UA_Boolean acked = true;
    UA_Variant value;
    UA_Variant_setScalar(&value, &acked, &UA_TYPES[UA_TYPES_BOOLEAN]);

    UA_QualifiedName ackedStateName = UA_QUALIFIEDNAME(0, (char *)"AckedState");
    UA_QualifiedName idName = UA_QUALIFIEDNAME(0, (char *)"Id");

    UA_Server_setConditionVariableFieldProperty(
        server,
        *objectId,
        &value,
        ackedStateName,
        idName);

    // // 2. Update Comment field
    // if(inputSize > 1) {
    //     UA_QualifiedName commentFieldName = UA_QUALIFIEDNAME(0, (char *)"Comment");
    //     UA_Server_setConditionVariableFieldProperty(
    //         server,
    //         *objectId,
    //         (UA_Variant*)&input[1],
    //         commentFieldName,
    //         UA_QUALIFIEDNAME(0, (char *)"Value"));
    // }

    // 3. Optionally fire a condition refresh / event:
    // UA_Server_triggerConditionEvent(
    //     server,
    //     *objectId,
    //     info->processNodeId,
    //     NULL);

    // custom logic
    if(!UA_NodeId_isNull(ackedStateNodeId)) {
        UA_Boolean acked = false;
        UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
        UA_Variant idValueVariant;
        UA_StatusCode idStatus = UA_Server_readObjectProperty(server, *ackedStateNodeId, idName, &idValueVariant);
        if (idStatus == UA_STATUSCODE_GOOD && UA_Variant_hasScalarType(&idValueVariant, &UA_TYPES[UA_TYPES_BOOLEAN])) {
            acked = *(UA_Boolean*)idValueVariant.data;
            info->acked = acked;
        }
    }

    log("Custom acknowledgment logic triggered!", LogLevel::DEBUG);
    
    // Debug logging to verify context
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, 
                "Method callback context - AckedStateNodeId: ns=%d;i=%d, AlarmInfo: %p", 
                ackedStateNodeId->namespaceIndex, ackedStateNodeId->identifier.numeric, 
                (void*)info);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, 
                "Alarm info - DisplayName: %s, ProcessNodeId: ns=%d;i=%d", 
                info->displayName.c_str(), 
                info->processNodeId.namespaceIndex, info->processNodeId.identifier.numeric);

    return UA_STATUSCODE_GOOD;
}




// Write callback for OPC UA node value changes
static void writeCallback(
    UA_Server *server,
    const UA_NodeId *sessionId, void *sessionContext,
    const UA_NodeId *nodeId, void *nodeContext,
    const UA_NumericRange *range, const UA_DataValue *data
) {
    // if (is_internal_write) return;  // 🔒 Prevent feedback loop
 
    // Find the topic for this node
    std::string topic;
    
    
    if (is_internal_write) goto Alarms;
    
    
    for (const auto& pair : nodeMap) {
        if (UA_NodeId_equal(&pair.second, nodeId)) {
            topic = pair.first;
            break;
        }
    }
    if (topic.empty()) return; // Not a topic node

    // Only proceed if there is a value to write
    if (data && data->hasValue) {
        json payload;
        json dataPoint = json::object();  // Create empty object to maintain order

        // Handle different types
        if (UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_DOUBLE])) {
            double value = *(UA_Double*)data->value.data;
            dataPoint["Value"] = value;
        } else if (UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_INT32])) {
            int value = *(UA_Int32*)data->value.data;
            dataPoint["Value"] = value;
        } else if (UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_BOOLEAN])) {
            bool value = *(UA_Boolean*)data->value.data;
            dataPoint["Value"] = value;
        } else if (UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_STRING])) {
            UA_String* str = (UA_String*)data->value.data;
            dataPoint["Value"] = std::string((char*)str->data, str->length);
        } else {
            return;
        }

        // Add metadata to the same dataPoint
        dataPoint["TagId"] = topicMap[topic].tagId;
        dataPoint["TagType"] = topicMap[topic].tagType;
        dataPoint["DatapointId"] = topicMap[topic].tagId;


        dataPoint["TimeStamp"] = UA_DateTime_now();



        payload["Data"] = json::array({dataPoint});

        publish_to_mqtt(topic, payload.dump());
    }




    //Now for Alarms

    Alarms:
    
        // Find the alarm info for this nodeId using the global map
        auto it = monitoredAlarms.find(*nodeId);
        if (it == monitoredAlarms.end()) {

            return;
        }


        if (!UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_INT32]) && 
        !UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_FLOAT]) && 
        !UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_DOUBLE])) {
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "Process node value written is not a numeric type. Ignoring alarm logic for ns=%d;i=%d.",
                       nodeId->namespaceIndex, nodeId->identifier.numeric);
        return;
    }

    double currentValue;
    if (UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_INT32])) {
        currentValue = (double)*(UA_Int32*)(data->value.data);
    } else if (UA_Variant_hasScalarType(&data->value, &UA_TYPES[UA_TYPES_FLOAT])) {
        currentValue = (double)*(UA_Float*)(data->value.data);
    } else {
        currentValue = *(UA_Double*)(data->value.data);
    }



    MonitoredNodeAlarmInfo *info = &it->second;
    UA_NodeId alarmInstanceId = info->alarmInstanceId;
    double hiHi = info->alarmHiHi;
    double hi = info->alarmHi;
    double lo = info->alarmLo;
    double loLo = info->alarmLoLo;
    double deadband = info->deadband;
    const std::string& displayName = info->displayName;


    UA_StatusCode setStatus = UA_STATUSCODE_GOOD;
    UA_Variant val;
    UA_Boolean stateBool;
    UA_Boolean boolFalse = false;
    UA_Boolean boolTrue = true;
    UA_LocalizedText message;
    UA_UInt16 severity;
    bool anyLimitActive = false; // Flag to track if any limit is currently violated

    // Read current states of the ExclusiveLimitAlarm (HighHighState, HighState, LowState, LowLowState)
    // These are TwoStateVariables, read their 'Id' property (boolean)
    bool currentHiHiState = false;
    bool currentHiState = false;
    bool currentLoState = false;
    bool currentLoLoState = false;

    UA_Variant currentPropVal;
    
    // Read HighHighState
    UA_QualifiedName highHighStateName = UA_QUALIFIEDNAME_ALLOC(0, "HighHighState");
    if (UA_Server_readObjectProperty(server, alarmInstanceId, highHighStateName, &currentPropVal) == UA_STATUSCODE_GOOD && 
        UA_Variant_hasScalarType(&currentPropVal, &UA_TYPES[UA_TYPES_BOOLEAN])) {
        currentHiHiState = *(UA_Boolean*)currentPropVal.data;
    }
    
    // Read HighState
    UA_QualifiedName highStateName = UA_QUALIFIEDNAME_ALLOC(0, "HighState");
    if (UA_Server_readObjectProperty(server, alarmInstanceId, highStateName, &currentPropVal) == UA_STATUSCODE_GOOD && 
        UA_Variant_hasScalarType(&currentPropVal, &UA_TYPES[UA_TYPES_BOOLEAN])) {
        currentHiState = *(UA_Boolean*)currentPropVal.data;
    }

    
    // Read LowState
    UA_QualifiedName lowStateName = UA_QUALIFIEDNAME_ALLOC(0, "LowState");
    if (UA_Server_readObjectProperty(server, alarmInstanceId, lowStateName, &currentPropVal) == UA_STATUSCODE_GOOD && 
        UA_Variant_hasScalarType(&currentPropVal, &UA_TYPES[UA_TYPES_BOOLEAN])) {
        currentLoState = *(UA_Boolean*)currentPropVal.data;
    }

    // Read LowLowState
    UA_QualifiedName lowLowStateName = UA_QUALIFIEDNAME_ALLOC(0, "LowLowState");
    if (UA_Server_readObjectProperty(server, alarmInstanceId, lowLowStateName, &currentPropVal) == UA_STATUSCODE_GOOD && 
        UA_Variant_hasScalarType(&currentPropVal, &UA_TYPES[UA_TYPES_BOOLEAN])) {
        currentLoLoState = *(UA_Boolean*)currentPropVal.data;
    }



    // Determine the highest priority active alarm state
    std::string currentAlarmMessage = "Normal";
    UA_UInt16 currentAlarmSeverity = 0;
    bool alarmTransitionedToNormal = false; // Flag to track if the overall alarm state transitioned to normal

        

    // Logic for HighHigh Alarm
    if (currentValue >= hiHi) {
        if (!currentHiHiState) { // Transition to HighHigh
            stateBool = true;
            UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
            UA_QualifiedName highHighStateName = UA_QUALIFIEDNAME_ALLOC(0, "HighHighState");
            UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
            setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
                                                                     highHighStateName, idName);

            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: HighHigh Alarm (%.2f >= %.2f)",
                        displayName.c_str(), currentValue, hiHi);
        }

        anyLimitActive = true;
        currentAlarmMessage = "Value EXCEEDS HighHigh Limit!";
        currentAlarmSeverity = 900;
    } else if (currentValue < (hiHi - deadband) && currentHiHiState) { // Return from HighHigh
        stateBool = false;
        UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_QualifiedName highHighStateName = UA_QUALIFIEDNAME_ALLOC(0, "HighHighState");
        UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
        setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
                                                                 highHighStateName, idName);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: HighHigh Alarm Cleared (%.2f < %.2f)",
                    displayName.c_str(), currentValue, hiHi - deadband);
    }

    // Logic for High Alarm (but not HighHigh, or after HighHigh clears)
    // Check if HighHigh is no longer active, and then check High
    if (currentValue >= hi && currentValue < hiHi) {
        if (!currentHiState && !currentHiHiState) { // Only transition to High if not already in High or HighHigh
            stateBool = true;
            UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
            UA_QualifiedName highStateName = UA_QUALIFIEDNAME_ALLOC(0, "HighState");
            UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
            setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
                                                                     highStateName, idName);
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: High Alarm (%.2f >= %.2f)",
                        displayName.c_str(), currentValue, hi);
        }
        anyLimitActive = true;
        // Prioritize message/severity based on highest active state
        if (currentAlarmSeverity < 700) { // If HighHigh is not active
            currentAlarmMessage = "Value EXCEEDS High Limit!";
            currentAlarmSeverity = 700;
        }
    } else if (currentValue < (hi - deadband) && currentHiState) { // Return from High
        stateBool = false;
        UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_QualifiedName highStateName = UA_QUALIFIEDNAME_ALLOC(0, "HighState");
        UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
        setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
                                                                 highStateName, idName);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: High Alarm Cleared (%.2f < %.2f)",
                    displayName.c_str(), currentValue, hi - deadband);
    }

    // Logic for LowLow Alarm
    if (currentValue <= loLo) {
        if (!currentLoLoState) { // Transition to LowLow
            stateBool = true;
            UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
            UA_QualifiedName lowLowStateName = UA_QUALIFIEDNAME_ALLOC(0, "LowLowState");
            UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
            setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
                                                                     lowLowStateName, idName);
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: LowLow Alarm (%.2f <= %.2f)",
                        displayName.c_str(), currentValue, loLo);
        }
        anyLimitActive = true;
        // Prioritize message/severity based on highest active state
        if (currentAlarmSeverity < 900) { // If HighHigh is not active, this is highest for low side
            currentAlarmMessage = "Value FALLS BELOW LowLow Limit!";
            currentAlarmSeverity = 900;
        }
    } else if (currentValue > (loLo + deadband) && currentLoLoState) { // Return from LowLow
        stateBool = false;
        UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_QualifiedName lowLowStateName = UA_QUALIFIEDNAME_ALLOC(0, "LowLowState");
        UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
        setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
                                                                 lowLowStateName, idName);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: LowLow Alarm Cleared (%.2f > %.2f)",
                    displayName.c_str(), currentValue, loLo + deadband);
    }

    // Logic for Low Alarm (but not LowLow, or after LowLow clears)
    // Check if LowLow is no longer active, and then check Low
    if (currentValue <= lo && currentValue > loLo) {
        if (!currentLoState && !currentLoLoState) { // Only transition to Low if not already in Low or LowLow
            stateBool = true;
            UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
            UA_QualifiedName lowStateName = UA_QUALIFIEDNAME_ALLOC(0, "LowState");
            UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
            setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
                                                                     lowStateName, idName);
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: Low Alarm (%.2f <= %.2f)",
                        displayName.c_str(), currentValue, lo);
        }
        anyLimitActive = true;
        // Prioritize message/severity based on highest active state
        if (currentAlarmSeverity < 700) { // If HighHigh, High, LowLow are not active
            currentAlarmMessage = "Value FALLS BELOW Low Limit!";
            currentAlarmSeverity = 700;
        }
    } else if (currentValue > (lo + deadband) && currentLoState) { // Return from Low
        stateBool = false;
        UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_QualifiedName lowStateName = UA_QUALIFIEDNAME_ALLOC(0, "LowState");
        UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
        setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
                                                                 lowStateName, idName);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: Low Alarm Cleared (%.2f > %.2f)",
                    displayName.c_str(), currentValue, lo + deadband);
    }



    // Handle the overall ActiveState of the alarm
    // ExclusiveLimitAlarmType automatically sets its ActiveState based on its internal limit states.
    // We only need to manually trigger an event if the *overall* alarm is going from active to inactive.
    UA_Boolean wasActive = false;
    UA_NodeId ActiveStateNodeId = findChildByBrowseName(server, alarmInstanceId, (char *)"ActiveState");
    if(!UA_NodeId_isNull(&ActiveStateNodeId)) {
            UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
            UA_Variant ASidValueVariant;
            UA_StatusCode idStatus = UA_Server_readObjectProperty(server, ActiveStateNodeId, idName, &ASidValueVariant);
            if (idStatus == UA_STATUSCODE_GOOD && UA_Variant_hasScalarType(&ASidValueVariant, &UA_TYPES[UA_TYPES_BOOLEAN])) {
                wasActive = *(UA_Boolean*)ASidValueVariant.data;
            }
        }



    //Setting up CALLBACk
    if(!info->callbackSetup) {
    UA_NodeId ackedStateNodeId = findChildByBrowseName(server, alarmInstanceId, (char *)"AckedState");

    UA_NodeId acknowledgeMethodNodeId = findChildByBrowseName(server, alarmInstanceId, (char *)"Acknowledge");

    log("Inside Setting Callback", LogLevel::DEBUG);

    // Use the existing info pointer (MonitoredNodeAlarmInfo *info = &it->second) that's already available in this function
    // Allocate memory for the context and set both pieces of information
    MethodCallbackContext *context = (MethodCallbackContext*)UA_malloc(sizeof(MethodCallbackContext));
    if(context) {
        context->ackedStateNodeId = ackedStateNodeId;
        context->alarmInfo = info;  // Use the existing info pointer from writeCallback
        
        // Set the node context first
        UA_Server_setNodeContext(server, acknowledgeMethodNodeId, context);
        
        // Then set the method callback
        UA_Server_setMethodNodeCallback(
            server,
            acknowledgeMethodNodeId,
            CustomAckCallback);
    }

    info->callbackSetup = true;
    }
        



if (anyLimitActive) {
    // Alarm is ACTIVE
    if (!wasActive) {
        // transition from inactive → active
        UA_Boolean newActiveState = true;
        UA_Variant_setScalar(&val, &newActiveState, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_QualifiedName activeStateName = UA_QUALIFIEDNAME_ALLOC(0, "ActiveState");
        UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");

        setStatus |= UA_Server_setConditionVariableFieldProperty(
            server, alarmInstanceId, &val, activeStateName, idName);

        // Retain = true
        UA_Boolean retain = true;
        UA_Variant_setScalar(&val, &retain, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_QualifiedName retainName = UA_QUALIFIEDNAME_ALLOC(0, "Retain");
        UA_Server_setConditionField(server, alarmInstanceId, &val, retainName);

        // AckedState = false
        info->acked = false;
        UA_QualifiedName ackedName = UA_QUALIFIEDNAME_ALLOC(0, "AckedState");
        UA_Variant_setScalar(&val, &boolFalse, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_Server_setConditionVariableFieldProperty(
            server, alarmInstanceId, &val, ackedName, idName);
    }
}
else {
    // Alarm is INACTIVE

    currentHiHiState = false;
    currentHiState = false;
    currentLoState = false;
    currentLoLoState = false;

    UA_Boolean newActiveState = false;
    UA_Variant_setScalar(&val, &newActiveState, &UA_TYPES[UA_TYPES_BOOLEAN]);
    UA_QualifiedName activeStateName = UA_QUALIFIEDNAME_ALLOC(0, "ActiveState");
    UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");

    setStatus |= UA_Server_setConditionVariableFieldProperty(
        server, alarmInstanceId, &val, activeStateName, idName);

    if (info->acked) {
        // acknowledged → Retain = false
        UA_Boolean retain = false;
        UA_Variant_setScalar(&val, &retain, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_QualifiedName retainName = UA_QUALIFIEDNAME_ALLOC(0, "Retain");
        UA_Server_setConditionField(server, alarmInstanceId, &val, retainName);
    } else {
        // not acknowledged → Retain = true
        UA_Boolean retain = true;
        UA_Variant_setScalar(&val, &retain, &UA_TYPES[UA_TYPES_BOOLEAN]);
        UA_QualifiedName retainName = UA_QUALIFIEDNAME_ALLOC(0, "Retain");
        UA_Server_setConditionField(server, alarmInstanceId, &val, retainName);
    }
}





    // // Update Message and Severity based on the highest priority current active state
    // // This is important because the default `TwoStateVariable` changes won't automatically update Message/Severity




UA_Variant_setScalar(&val, &currentAlarmSeverity, &UA_TYPES[UA_TYPES_UINT16]);

UA_QualifiedName severityName = UA_QUALIFIEDNAME_ALLOC(0, "Severity");
setStatus = UA_Server_setConditionField(server, alarmInstanceId, &val, severityName);




UA_LocalizedText newAlarmMessage =
    UA_LOCALIZEDTEXT_ALLOC("en", currentAlarmMessage.c_str());


UA_Variant_setScalar(&val, &newAlarmMessage, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

// write to server
UA_QualifiedName messageName = UA_QUALIFIEDNAME_ALLOC(0, "Message");
setStatus = UA_Server_setConditionField(server, alarmInstanceId, &val, messageName);






    // Explicitly trigger the condition event if the overall alarm state changed to normal
    // (open62541 automatically triggers events for changes in TwoStateVariables like HighHighState)
    bool triggerExplicitEvent = alarmTransitionedToNormal;

    // If a limit is active and we manually set message/severity, we still need to trigger the event
        if (anyLimitActive && !wasActive) {
            triggerExplicitEvent = true;
        } else if (!anyLimitActive && wasActive) {
            triggerExplicitEvent = true;
        }


    

    if (triggerExplicitEvent) {
        UA_StatusCode triggerStatus = UA_Server_triggerConditionEvent(server, alarmInstanceId,
                                                                    *nodeId, NULL);
        if (triggerStatus != UA_STATUSCODE_GOOD) {
            UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                        "Failed to trigger condition event for %s. StatusCode %s",
                        displayName.c_str(), UA_StatusCode_name(triggerStatus));
        }
    }
}




void
mqtt_subscribe_and_update(UA_Server *server, const std::vector<std::string> &topics) {
    // Use global ioc and amcl
    as::co_spawn(
        ioc,
        [&topics, server]() -> as::awaitable<void> {
            // Reconnection loop
            while(running) {
                try {
                    log("Attempting to connect to MQTT broker...", LogLevel::INFO);
                    // Connect to broker
                    co_await amcl.async_underlying_handshake("216.48.184.131", "15579",
                                                             as::use_awaitable);

                    // Start MQTT session with username/password
                    auto connack_opt = co_await amcl.async_start(
                        am::v5::connect_packet{
                            true,          // clean_start
                            0x1234,        // keep_alive
                            "",            // Client Identifier
                            std::nullopt,  // no will
                            "portal",      // username
                            "dt0Unw7QRh"   // password
                        },
                        as::use_awaitable);
                    if(!connack_opt) {
                        throw std::runtime_error(
                            "Failed to start MQTT session (CONNACK not received).");
                    }
                    log("Successfully connected to MQTT broker.", LogLevel::INFO);

                    // Subscribe to all topics
                    std::vector<am::topic_subopts> sub_entry;
                    for(const auto &topic : topics) {
                        sub_entry.push_back({topic, am::qos::at_most_once});
                    }
                    auto suback_opt = co_await amcl.async_subscribe(
                        am::v5::subscribe_packet{*amcl.acquire_unique_packet_id(),
                                                 am::force_move(sub_entry)},
                        as::use_awaitable);
                    if(!suback_opt) {
                        throw std::runtime_error("Failed to subscribe to topics.");
                    }
                    log("Successfully subscribed to " + std::to_string(topics.size()) +
                            " topics.",
                        LogLevel::INFO);

                    // Receive loop
                    while(running) {
                        auto pv_opt = co_await amcl.async_recv(as::use_awaitable);
                        if(!pv_opt) {
                            // This indicates a graceful disconnect or an issue.
                            log("MQTT connection closed by broker or network issue.",
                                LogLevel::ERRORS);
                            break;  // Exit receive loop to trigger reconnection
                        }
                        pv_opt->visit(am::overload{
                            [&](client_t::publish_packet &p) {
                                std::string topic = p.topic();
                                std::string payload = p.payload();
                                auto it = nodeMap.find(topic);
                                if(it != nodeMap.end()) {
                                    try {
                                        auto j = json::parse(payload);
                                        if(j.contains("Data") && j["Data"].is_array() &&
                                           !j["Data"].empty()) {
                                            if(j["Data"][0]["Value"].is_number()) {
                                                double value =
                                                    j["Data"][0]["Value"].get<double>();
                                                UA_Variant var;
                                                UA_Variant_setScalar(
                                                    &var, &value,
                                                    &UA_TYPES[UA_TYPES_DOUBLE]);
                                                is_internal_write = true;
                                                UA_Server_writeValue(server, it->second,
                                                                     var);
                                                is_internal_write = false;
                                            } else if(j["Data"][0]["Value"]
                                                          .is_boolean()) {
                                                UA_Boolean value =
                                                    j["Data"][0]["Value"].get<bool>();
                                                UA_Variant var;
                                                UA_Variant_setScalar(
                                                    &var, &value,
                                                    &UA_TYPES[UA_TYPES_BOOLEAN]);
                                                is_internal_write = true;
                                                UA_Server_writeValue(server, it->second,
                                                                     var);
                                                is_internal_write = false;
                                            } else if(j["Data"][0]["Value"].is_string()) {
                                                std::string strValue =
                                                    j["Data"][0]["Value"]
                                                        .get<std::string>();
                                                UA_String value =
                                                    UA_STRING_ALLOC(strValue.c_str());
                                                UA_Variant var;
                                                UA_Variant_setScalar(
                                                    &var, &value,
                                                    &UA_TYPES[UA_TYPES_STRING]);
                                                is_internal_write = true;
                                                UA_Server_writeValue(server, it->second,
                                                                     var);
                                                is_internal_write = false;
                                                UA_String_clear(&value);
                                            }
                                        }
                                    } catch(const std::exception &e) {
                                        log("JSON parse error: " + std::string(e.what()),
                                            LogLevel::ERRORS);
                                    }
                                }
                            },
                            [](auto &) {}  // Ignore other packet types
                        });
                    }
                } catch(const std::exception &e) {
                    log("MQTT error: " + std::string(e.what()) +
                            ". Reconnecting in 5 seconds...",
                        LogLevel::ERRORS);
                }

                // If we've reached here, it's due to an error or disconnect.
                // Reset the client before attempting to reconnect.
                amcl = client_t{ioc.get_executor()};

                // Wait before retrying, but exit if the server is shutting down.
                for(int i = 0; i < 5 && running; ++i) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            }
            log("MQTT reconnection loop stopped due to server shutdown.", LogLevel::INFO);
            co_return;
        },
        as::detached);
}


json getBearerToken() {
    try {
        std::string host = "164.52.221.177";
        std::string port = "5128";
        std::string target = "/api/Login";
        int version = 11;

        // JSON body
        std::string json_body = R"({
        "Username":"ajay.sharma@techondater.co.in",
        "password":"VvvQVRdH7JheYR7lLgbPCp4fcNEslXnKqhR59bdFMK8="
        })";

        // Set up I/O context and resolver
        // as::io_context ioc;
        tcp::resolver resolver(ioc);
        beast::tcp_stream stream(ioc);

        // Resolve domain name
        auto const results = resolver.resolve(host, port);

        // Connect to host
        stream.connect(results);

        // Create HTTP POST request
        beast::http::request<beast::http::string_body> req{beast::http::verb::post, target, version};
        req.set(beast::http::field::host, host);
        req.set(beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
        req.set(beast::http::field::content_type, "application/json");
        req.body() = json_body;
        req.prepare_payload();

        // Send request
        beast::http::write(stream, req);

        // Read response
        beast::flat_buffer buffer;
        beast::http::response<beast::http::string_body> res;
        beast::http::read(stream, buffer, res);

        json result = json::parse(res.body());
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, res.body().c_str());

        // Gracefully close the connection
        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        if (ec && ec != beast::errc::not_connected)
            throw beast::system_error{ec};

        return result;
    } catch (const std::exception &e) {
        log("Error: " + std::string(e.what()), LogLevel::ERRORS);
        return json{};
    }
}



json getTopicList(string bearerToken) {

try{
    std::string host = "164.52.221.177";
    std::string port = "5128";
    std::string target = "/api/GetTopicList";
    int version = 11;

    // JSON body
    std::string json_body = R"(
    {
   
        "orgId": 0,
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
            "isLogging" : true
        }
    }
    )";

    // Set up I/O context and connection
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);

    // Resolve and connect
    auto const results = resolver.resolve(host, port);
    stream.connect(results);

    // Build HTTP POST request
    beast::http::request<beast::http::string_body> req{beast::http::verb::post, target, version};
    req.set(beast::http::field::host, host);
    req.set(beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(beast::http::field::content_type, "application/json");

    // Set Bearer Authorization header
    req.set(beast::http::field::authorization, "Bearer " + bearerToken);

    req.body() = json_body;
    req.prepare_payload();

    // Send request
    beast::http::write(stream, req);

    // Get response
    beast::flat_buffer buffer;
    beast::http::response<beast::http::string_body> res;
    beast::http::read(stream, buffer, res);

    
    // Output response

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, res.body().c_str());
    json result = json::parse(res.body());
    // Shutdown connection
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);

    return result;

    } catch (const std::exception &e) {
        log("Error: " + std::string(e.what()), LogLevel::ERRORS);
        return json{};
    }
    
}










int main(int argc, char* argv[]) {

    // Global logging control - only enable file logging with --debug
    g_logging_enabled = false; // Default: no file logging
    
    if(argc > 1 && std::string(argv[1]) == "--debug") {
        g_debug = true;
        g_logging_enabled = true; // Enable file logging in debug mode
        log("Debug mode enabled - file logging active", LogLevel::INFO);
    } else {
        g_debug = false;
        log("Debug mode disabled - no file logging", LogLevel::DEBUG);
    }
    
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);

    // Initialize logging with server-specific folder
    init_logging("logs/server", "server", true);
    log("Server logging initialized with day-wise log files", LogLevel::INFO);

    UA_ByteString certificate = loadFile("server/own/certs/server_cert.der");  
    UA_ByteString privateKey  = loadFile("server/own/certs/server_key.der");
    
    if(certificate.length == 0 || privateKey.length == 0) {
        log("Failed to load server certificate or key", LogLevel::ERRORS);
        UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "Could not load server certificate or key from certs/own/");
        return EXIT_FAILURE;
    }
    
    log("Server certificate and private key loaded successfully", LogLevel::INFO);

    UA_ByteString *trustList = NULL;
    size_t trustListSize = loadCertsFromDirectory("server/trusted/certs", &trustList);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Loaded %zu trusted certificate(s).", trustListSize);
    log("Loaded " + std::to_string(trustListSize) + " trusted certificate(s)", LogLevel::INFO);
    
    UA_ByteString *issuerList = NULL;
    size_t issuerListSize = loadCertsFromDirectory("server/issuers/certs", &issuerList);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Loaded %zu issuer certificate(s).", issuerListSize);
    log("Loaded " + std::to_string(issuerListSize) + " issuer certificate(s)", LogLevel::INFO);

    size_t revocationListSize = 0;
    UA_ByteString *revocationList = NULL;

    UA_Server *server = UA_Server_new();
    UA_ServerConfig *config = UA_Server_getConfig(server);
    
        // Custom logger is now enabled with safe format string handling
    // The myLog function now sanitizes problematic format specifiers before processing
    config->logging = &myLogger;
    
    log("OPC UA Server initialized", LogLevel::INFO);
    log("Setting up server configuration", LogLevel::DEBUG);
    
    size_t nsIdx = UA_Server_addNamespace(server, "urn:my.properties");
    log("Added namespace: urn:my.properties", LogLevel::DEBUG);

    // broker_start(argc, argv);
    // #ifdef UA_ENABLE_ENCRYPTION
    // UA_ServerConfig_setDefaultWithSecurityPolicies(config, 4840, &certificate, &privateKey, trustList, trustListSize, NULL, 0, NULL, 0);
    //     UA_ServerConfig_setDefaultWithSecurityPolicies(config, 4840, &certificate, &privateKey, NULL, NULL, NULL, 0, NULL, 0);
    // config->applicationDescription.applicationUri = UA_STRING_ALLOC("urn:Anexee.server.application");

    // UA_StatusCode retval =
    // UA_ServerConfig_setDefaultWithSecurityPolicies(config, 4840,
    //     &certificate, &privateKey,
    //     trustList, trustListSize,
    //     issuerList, issuerListSize,
    //     revocationList, revocationListSize);

        UA_StatusCode retval =
    UA_ServerConfig_setDefaultWithSecurityPolicies(config, 53531,
        &certificate, &privateKey,
        trustList, trustListSize,
        issuerList, issuerListSize,
        revocationList, revocationListSize);
        
    if(retval == UA_STATUSCODE_GOOD) {
        log("Security policies configured successfully", LogLevel::INFO);
        log("Server will listen on port 53531", LogLevel::DEBUG);
    } else {
        log("Failed to configure security policies", LogLevel::ERRORS);
    }

    // if(retval != UA_STATUSCODE_GOOD) {
    //     UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to set default security policies");
    //     goto cleanup;
    // }
        
    for(size_t i = 0; i < config->endpointsSize; i++) {
        UA_String_clear(&config->endpoints[i].endpointUrl);
        config->endpoints[i].endpointUrl = UA_STRING_ALLOC("opc.tcp://0.0.0.0:53531");
    }
    
    log("Configured server endpoints", LogLevel::DEBUG);
        
    // Accept all certificates for demo/testing
    // config->secureChannelPKI.clear(&config->secureChannelPKI);
    // config->sessionPKI.clear(&config->sessionPKI);
    // UA_CertificateGroup_AcceptAll(&config->secureChannelPKI);
    // UA_CertificateGroup_AcceptAll(&config->sessionPKI);

    config->applicationDescription.applicationUri = UA_STRING_ALLOC("urn:Anexee.server.application");
    config->applicationDescription.productUri = UA_STRING_ALLOC("urn:Anexee.server");
    config->applicationDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Anexee");
    
    log("Application description configured: Anexee Server", LogLevel::DEBUG);




    // Add historizing configuration
    g_gathering = (UA_HistoryDataGathering*)UA_malloc(sizeof(UA_HistoryDataGathering));
    *g_gathering = UA_HistoryDataGathering_Default(1);
    config->historyDatabase = UA_HistoryDatabase_default(*g_gathering);

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Historizing configuration initialized");
    log("Historizing configuration initialized successfully", LogLevel::INFO);

// ----------------
    UA_AccessControl_defaultWithLoginCallback(config, true, NULL, 2, usernamePasswordLogin, myLoginCallback, NULL);
    log("Access control configured with login callback", LogLevel::DEBUG);

    for(size_t i = 0; i < config->endpointsSize; i++) {
        UA_EndpointDescription *ep = &config->endpoints[i];
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Server Endpoint %zu: %.*s", i, (int)ep->endpointUrl.length, ep->endpointUrl.data);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "  Server SecurityPolicy: %.*s", (int)ep->securityPolicyUri.length, ep->securityPolicyUri.data);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "  Server SecurityMode: %d", ep->securityMode);
        for(size_t j = 0; j < ep->userIdentityTokensSize; j++) {
            UA_UserTokenPolicy *pol = &ep->userIdentityTokens[j];
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "    Server TokenType: %d, PolicyId: %.*s, SecurityPolicyUri: %.*s",
                pol->tokenType,
                (int)pol->policyId.length, pol->policyId.data,
                (int)pol->securityPolicyUri.length, pol->securityPolicyUri.data);
        }
    }

//    pqxx::connection c("dbname=postgres user=postgres password=payphone123@007");
//    pqxx::work txn(c);
//    pqxx::result r = txn.exec("SELECT topic FROM mqtt_topics");
//    for (auto row : r) {
//        topics.push_back(row[0].c_str());
//    }













    // add a variable node to the adresspace
    UA_VariableAttributes attr = UA_VariableAttributes_default;
        UA_Int32 myInteger = 42;
        UA_Variant_setScalarCopy(&attr.value, &myInteger, &UA_TYPES[UA_TYPES_INT32]);
        attr.description = UA_LOCALIZEDTEXT_ALLOC("en-US","the answer");
        attr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US","the answer");
        attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE | UA_ACCESSLEVELMASK_HISTORYREAD;
        attr.historizing = true;
        UA_NodeId myIntegerNodeId = UA_NODEID_STRING_ALLOC(1, "the.answer");
        UA_QualifiedName myIntegerName = UA_QUALIFIEDNAME_ALLOC(1, "the answer");
        UA_NodeId parentNodeId = UA_NS0ID(OBJECTSFOLDER);
        UA_NodeId parentReferenceNodeId = UA_NS0ID(ORGANIZES);
        UA_Server_addVariableNode(server, myIntegerNodeId, parentNodeId,
                                parentReferenceNodeId, myIntegerName,
                                UA_NODEID_NULL, attr, NULL, NULL);

    // Add a second variable that changes periodically
    UA_VariableAttributes attr2 = UA_VariableAttributes_default;
        UA_Double myDouble = 123456;
        UA_Variant_setScalarCopy(&attr2.value, &myDouble, &UA_TYPES[UA_TYPES_DOUBLE]);
        attr2.description = UA_LOCALIZEDTEXT_ALLOC("en-US","counter");
        attr2.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US","counter");
        attr2.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE | UA_ACCESSLEVELMASK_HISTORYREAD;
        attr2.historizing = true;

        UA_NodeId myDoubleNodeId = UA_NODEID_STRING_ALLOC(1, "counter");
        UA_QualifiedName myDoubleName = UA_QUALIFIEDNAME_ALLOC(1, "counter");
        UA_Server_addVariableNode(server, myDoubleNodeId, parentNodeId,
                                parentReferenceNodeId, myDoubleName,
                                UA_NODEID_NULL, attr2, NULL, NULL);




    
    
    
    //UA_VariableAttributes attr3 = UA_VariableAttributes_default;
        //UA_Double minValue = 0.0;
        //UA_Variant_setScalarCopy(&attr3.value, &minValue, &UA_TYPES[UA_TYPES_DOUBLE]);
        //attr3.description = UA_LOCALIZEDTEXT_ALLOC("en-US","MIN");
        //attr3.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US","MIN");
        //attr3.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
        //UA_NodeId minNodeId = UA_NODEID_STRING_ALLOC(1, "MIN");
        //UA_QualifiedName minName = UA_QUALIFIEDNAME_ALLOC(1, "MIN");
        //UA_Server_addVariableNode(server, minNodeId, myDoubleNodeId,
        //                        UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY), minName,
        //                        UA_NODEID_NULL, attr3, NULL, NULL);

        
    //for(const auto &topic : topics) {
    //         UA_VariableAttributes attr = UA_VariableAttributes_default;
    //         UA_Int32 value = 0;  // or whatever default
    //         UA_Variant_setScalarCopy(&attr.value, &value, &UA_TYPES[UA_TYPES_INT32]);
    //         attr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", topic.c_str());
    //         UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(1, topic.c_str());
    //         UA_QualifiedName nodeName = UA_QUALIFIEDNAME_ALLOC(1, topic.c_str());
    //         //UA_NodeId parentNodeId = UA_NS0ID(OBJECTSFOLDER);
    //         //UA_NodeId parentReferenceNodeId = UA_NS0ID(ORGANIZES);
    //         UA_Server_addVariableNode(server, nodeId, myDoubleNodeId,
    //                                   UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
    //                                   nodeName, UA_NODEID_NULL, attr, NULL, NULL);
    //         // Store nodeId for later updates
    //     }





// // For each topic:
// for (const auto& topic : topics) {
//     auto parts = split(topic, '/');
//     std::string currentPath;
//      UA_NodeId parent = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
//     for (size_t i = 0; i < parts.size(); ++i) {
//         if (!currentPath.empty()) currentPath += "/";
//         currentPath += parts[i];
//         if (i < parts.size() - 1) {
//             // Create folder/object for each level except the last
//             parent = getOrCreateFolder(server, currentPath, parts[i], parent);
//         } else {
//             // Last part: create variable node as child of parent
//             UA_VariableAttributes attr = UA_VariableAttributes_default;
//             UA_Int32 value = 0;
//             UA_Variant_setScalarCopy(&attr.value, &value, &UA_TYPES[UA_TYPES_INT32]);
//             attr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", parts[i].c_str());
//             attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
//             UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(1, currentPath.c_str());
//             UA_QualifiedName nodeName = UA_QUALIFIEDNAME_ALLOC(1, parts[i].c_str());
//             UA_Server_addVariableNode(server, nodeId, parent, UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT), nodeName, UA_NODEID_NULL, attr, NULL, NULL);
//             nodeMap[currentPath] = nodeId;

//             UA_ValueCallback callback;
//             callback.onWrite = writeCallback;
//             callback.onRead = NULL;
//             UA_Server_setVariableNode_valueCallback(server, nodeId, callback);
//         }
//     }
// }

auto futureToken = std::async(std::launch::async, getBearerToken);
string BearerToken = "";
json token = futureToken.get();
        // Extract access_token
if (token.contains("access_token")) {
   BearerToken = token["access_token"].get<std::string>();
}

  // waits here until getBearerToken finishes

// if (!BearerToken.empty()) { 
//     auto futureResponse = std::async(std::launch::async, getTopicList, BearerToken);
//     json response = futureResponse.get();
    
    // Process and store API and create Address space.

    // Global structure to hold alarm-related data for each monitored node


// std::map<UA_NodeId, MonitoredNodeAlarmInfo, UA_NodeId_less_than> monitoredAlarms;

// Add an ALARM FOLDER INSIDE SERVER THEN ALL NODES WITH HASEVENTSOURCE WILL BE ADDED TO THIS FOLDER
UA_NodeId areaNodeId = UA_NODEID_NUMERIC(0, 54624);
UA_ObjectAttributes objAttr = UA_ObjectAttributes_default;
objAttr.displayName = UA_LOCALIZEDTEXT((char *)"en", (char *)"Alarms");
UA_Server_addObjectNode(server,
    UA_NODEID_NULL,
    UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER),
    UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
    UA_QUALIFIEDNAME(1,(char *)"Alarms"),
    UA_NODEID_NUMERIC(0, UA_NS0ID_BASEOBJECTTYPE),
    objAttr, NULL,
    &areaNodeId);

UA_Server_addReference(server,
    UA_NODEID_NUMERIC(0, 2253), // Server
    UA_NODEID_NUMERIC(0, UA_NS0ID_HASNOTIFIER),
    UA_EXPANDEDNODEID_NUMERIC(areaNodeId.namespaceIndex, areaNodeId.identifier.numeric),
    UA_TRUE);
    
    int count = 0;
vector<string> topics;
if (!BearerToken.empty()) {
    auto futureResponse = std::async(std::launch::async, getTopicList, BearerToken);
    json response = futureResponse.get();
    
    if (response.contains("data") && response["data"].is_array()) {
        for (const auto& item : response["data"]) {
            if (item.contains("namespace")) {
                string ns = item["namespace"].get<string>();
                topics.push_back(ns);
                
                // Create address space for this topic
                auto parts = split(ns, '/');
                std::string currentPath;
                UA_NodeId parent = UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER);
                
                for (size_t i = 0; i < parts.size(); ++i) {
                    if (!currentPath.empty()) currentPath += "/";
                    currentPath += parts[i];
                    if (i < parts.size() - 1) {
                        parent = getOrCreateFolder(server, currentPath, parts[i], parent);
                    } else {
                        UA_VariableAttributes attr = UA_VariableAttributes_default;
                        UA_Int32 value = 0;
                        UA_Variant_setScalarCopy(&attr.value, &value, &UA_TYPES[UA_TYPES_INT32]);
                        attr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", parts[i].c_str());
                        attr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", item["name"].get<string>().c_str());
                        attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE | UA_ACCESSLEVELMASK_HISTORYREAD;
                        attr.historizing = true;

                        // UA_NodeId nodeId = UA_NODEID_STRING_ALLOC(1, currentPath.c_str());
                        UA_NodeId nodeId = UA_NODEID_NUMERIC(1, item["tagId"].get<int>());
                        UA_QualifiedName nodeName = UA_QUALIFIEDNAME_ALLOC(1, parts[i].c_str());
                        UA_Server_addVariableNode(server, nodeId, parent, 
                            UA_NODEID_NUMERIC(0, UA_NS0ID_HASCOMPONENT), 
                            nodeName, UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),  // Use proper type
                            attr, NULL, NULL);

                        UA_Range range;
                        range.low = item["rangeMin"].get<double>();
                        range.high = item["rangeMax"].get<double>();
                        UA_Variant rangeVariant;
                        UA_Variant_init(&rangeVariant);
                        UA_Variant_setScalar(&rangeVariant, &range, &UA_TYPES[UA_TYPES_RANGE]);

                        

                            
                        // Create attributes for the range property
                        UA_VariableAttributes rangeAttr = UA_VariableAttributes_default;
                        rangeAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "EURange");
                        rangeAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                        rangeAttr.value = rangeVariant;

                        UA_NodeId rangeNodeId = UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000  + 1);  // Unique ID for range
                        UA_QualifiedName rangeName = UA_QUALIFIEDNAME_ALLOC(0, "EURange");
                        UA_Server_addVariableNode(server, rangeNodeId, nodeId,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                            rangeName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                            rangeAttr, NULL, NULL);











                        

                        // After adding the EURange property, add the alarm limits
                        UA_Double alarmHiHi = item["alarmHiHi"].get<double>();
                        UA_Double alarmHi = item["alarmHi"].get<double>();
                        UA_Double alarmLo = item["alarmLo"].get<double>();
                        UA_Double alarmLoLo = item["alarmLoLo"].get<double>();

                        // Add each alarm limit as a property
                        auto addAlarmProperty = [&](const char* name, UA_Double value, UA_NodeId parentId, int offset) {
                            UA_VariableAttributes alarmAttr = UA_VariableAttributes_default;
                            UA_Variant alarmVariant;
                            UA_Variant_init(&alarmVariant);
                            UA_Variant_setScalar(&alarmVariant, &value, &UA_TYPES[UA_TYPES_DOUBLE]);
                            
                            alarmAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", name);
                            alarmAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                            alarmAttr.value = alarmVariant;
                            
                            UA_NodeId alarmNodeId = UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000 + offset);
                            UA_QualifiedName alarmName = UA_QUALIFIEDNAME_ALLOC(0, name);
                            UA_Server_addVariableNode(server, alarmNodeId, parentId,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                                alarmName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                                alarmAttr, NULL, NULL);


                            // clean up
                            // UA_Variant_clear(&alarmVariant);
                        };

                        addAlarmProperty("AlarmHiHi", alarmHiHi, nodeId, 2);
                        addAlarmProperty("AlarmHi", alarmHi, nodeId, 3);
                        addAlarmProperty("AlarmLo", alarmLo, nodeId, 4);
                        addAlarmProperty("AlarmLoLo", alarmLoLo, nodeId, 5);
















                        // Create the engineering unit property
                        UA_String unit = UA_STRING_ALLOC(item["measurmentUnitType"].get<string>().c_str());

                        UA_Variant unitVariant;
                        UA_Variant_init(&unitVariant);
                        UA_Variant_setScalar(&unitVariant, &unit, &UA_TYPES[UA_TYPES_STRING]);

                        UA_VariableAttributes unitAttr = UA_VariableAttributes_default;
                        unitAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Engineering Unit");
                        unitAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                        UA_Variant_copy(&unitVariant, &unitAttr.value);  

                        UA_NodeId unitNodeId = UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000 + 6);  // Use a different offset
                        UA_QualifiedName unitName = UA_QUALIFIEDNAME_ALLOC(0, "EngineeringUnit");
                        UA_Server_addVariableNode(server, unitNodeId, nodeId,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                            unitName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                            unitAttr, NULL, NULL);


                        // DeadBand
                        if (item.contains("deadband")) {
                        UA_Double deadBand = item["deadband"].get<double>();
                        UA_Variant deadBandVariant;
                        UA_Variant_init(&deadBandVariant);
                        UA_Variant_setScalar(&deadBandVariant, &deadBand, &UA_TYPES[UA_TYPES_DOUBLE]);

                        UA_VariableAttributes deadBandAttr = UA_VariableAttributes_default;
                        deadBandAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "DeadBand");
                        deadBandAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                        UA_Variant_copy(&deadBandVariant, &deadBandAttr.value); 

                        UA_NodeId deadBandNodeId = UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000 + 7);  // Use a different offset
                        UA_QualifiedName deadBandName = UA_QUALIFIEDNAME_ALLOC(0, "DeadBand");
                        UA_Server_addVariableNode(server, deadBandNodeId, nodeId,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                            deadBandName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                            deadBandAttr, NULL, NULL);
                        }

                        // Precision Type
                        if (item.contains("precisionType")) {
                        UA_String precision = UA_STRING_ALLOC(item["precisionType"].get<string>().c_str());

                        UA_Variant precisionVariant;
                        UA_Variant_init(&precisionVariant);
                        UA_Variant_setScalar(&precisionVariant, &precision, &UA_TYPES[UA_TYPES_STRING]);

                        UA_VariableAttributes precisionAttr = UA_VariableAttributes_default;
                        precisionAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Precision Type");
                        precisionAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                        UA_Variant_copy(&precisionVariant, &precisionAttr.value);  

                        UA_NodeId precisionNodeId = UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000 + 8);  // Use a different offset
                        UA_QualifiedName precisionName = UA_QUALIFIEDNAME_ALLOC(0, "Precision");
                        UA_Server_addVariableNode(server, precisionNodeId, nodeId,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                            precisionName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                            precisionAttr, NULL, NULL);
                        }

                        // Parameter Group
                        if (item.contains("parameterGroup")) {
                        UA_String parameterGroup = UA_STRING_ALLOC(item["parameterGroup"].get<string>().c_str());

                        UA_Variant parameterGroupVariant;
                        UA_Variant_init(&parameterGroupVariant);
                        UA_Variant_setScalar(&parameterGroupVariant, &parameterGroup, &UA_TYPES[UA_TYPES_STRING]);

                        UA_VariableAttributes parameterGroupAttr = UA_VariableAttributes_default;
                        parameterGroupAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Parameter Group");    
                        parameterGroupAttr.accessLevel = UA_ACCESSLEVELMASK_READ;       
                        UA_Variant_copy(&parameterGroupVariant, &parameterGroupAttr.value);  

                        UA_NodeId parameterGroupNodeId = UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000 + 9);  // Use a different offset
                        UA_QualifiedName parameterGroupName = UA_QUALIFIEDNAME_ALLOC(0, "Parameter Group");
                        UA_Server_addVariableNode(server, parameterGroupNodeId, nodeId, 
                        UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                        parameterGroupName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                        parameterGroupAttr, NULL, NULL);
                        }

                        //tagType
                        if (item.contains("tagType")) {
                        UA_String tagType = UA_STRING_ALLOC(item["tagType"].get<string>().c_str());

                        UA_Variant tagTypeVariant;
                        UA_Variant_init(&tagTypeVariant);
                        UA_Variant_setScalar(&tagTypeVariant, &tagType, &UA_TYPES[UA_TYPES_STRING]);

                        UA_VariableAttributes tagTypeAttr = UA_VariableAttributes_default;
                        tagTypeAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Tag Type");
                        tagTypeAttr.accessLevel = UA_ACCESSLEVELMASK_READ;
                        UA_Variant_copy(&tagTypeVariant, &tagTypeAttr.value);  

                        UA_NodeId tagTypeId = UA_NODEID_NUMERIC(2, item["tagId"].get<int>() * 1000 + 10);  // Use a different offset
                        UA_QualifiedName tagTypeName = UA_QUALIFIEDNAME_ALLOC(0, "TagType");
                        UA_Server_addVariableNode(server, tagTypeId, nodeId,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                            tagTypeName, UA_NODEID_NUMERIC(0, UA_NS0ID_PROPERTYTYPE),
                            tagTypeAttr, NULL, NULL);
                        }


                        nodeMap[currentPath] = nodeId;

                        // After creating each variable node, add this:
                        UA_HistorizingNodeIdSettings setting;
                        setting.historizingBackend = UA_HistoryDataBackend_Memory(200, 1000);
                        setting.maxHistoryDataResponseSize = 1000;
                        setting.historizingUpdateStrategy = UA_HISTORIZINGUPDATESTRATEGY_VALUESET;
                        // setting.pollingInterval = 1000;  
                        
                        // Register the node for historizing using the global gathering context
                        UA_StatusCode ret = g_gathering->registerNodeId(server, g_gathering->context, &nodeId, setting);


                        const UA_HistorizingNodeIdSettings* currentSettings = 
                            g_gathering->getHistorizingSetting(server, g_gathering->context, &nodeId);
                        if(currentSettings) {
                            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, 
                                        "Node historizing settings verified - Update Strategy: %d", 
                                        currentSettings->historizingUpdateStrategy);
                        }


                        
                        //Link Alarms?
                        //Added count so it creates alarm for every 25th node thus not overflowing the alarm queue
                        if (nodeId.identifier.numeric == 1618 || nodeId.identifier.numeric == 1569) {
                            
                            
                            MonitoredNodeAlarmInfo alarmInfo;
                            alarmInfo.processNodeId = nodeId;
                            alarmInfo.displayName = parts[i]; // Use the node's display name for alarm messages

                            // Get alarm thresholds from the JSON item

                            alarmInfo.alarmHiHi = (item["alarmHiHi"].get<double>() == 0) ? 20.0 : item["alarmHiHi"].get<double>();
                            alarmInfo.alarmHi = (item["alarmHi"].get<double>() == 0) ? 10.0 :item["alarmHi"].get<double>();
                            alarmInfo.alarmLo = (item["alarmLo"].get<double>() == 0) ? -10.0 : item["alarmLo"].get<double>();
                            alarmInfo.alarmLoLo = (item["alarmLoLo"].get<double>() == 0) ? -20.0 : item["alarmLoLo"].get<double>();

                            alarmInfo.deadband = item.contains("deadband") ? item["deadband"].get<double>() : 0.0;

                            UA_StatusCode alarmStatus = createAndLinkExclusiveLimitAlarm(server, &nodeId, alarmInfo.displayName, item, &alarmInfo.alarmInstanceId);
                            if (alarmStatus != UA_STATUSCODE_GOOD) {
                                UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to create alarm for node %s", parts[i].c_str());
                                // Handle error, maybe continue or return
                            } else {
                                // Store the alarm information in our global map
                                monitoredAlarms[nodeId] = alarmInfo;
                            }

                        }
                        count++;

                            UA_ValueCallback callback;
                            callback.onWrite = writeCallback;
                            callback.onRead = NULL;
                            UA_Server_setVariableNode_valueCallback(server, nodeId, callback);
                    }   
                }

                // Store topic info
                topicMap[ns] = {
                    item["tagId"].get<int>(),
                    item["name"].get<string>(),
                    item["tagType"].get<string>(),
                    item["rangeMin"].get<double>(),
                    item["rangeMax"].get<double>()
                };
                

            }
        }

                        //         // Clean up
                        // UA_String_clear(&unit);
                        // UA_Double_clear(&alarmHiHi);
                        // UA_Double_clear(&alarmHi);
                        // UA_Double_clear(&alarmLo);
                        // UA_Double_clear(&alarmLoLo);
                        // UA_Variant_clear(&unitVariant);
                        // UA_Variant_clear(&rangeVariant);

    }
        // TODO:
         // FIX IT WHEN CREATING THE ADDRESS SPACE AGAIN OTHERWISE IT WILL SUBSCRIBE TO THE TOPICS AGAIN
        mqtt_subscribe_and_update(server, topics);
}





std::thread mqtt_thread([&]() {
    ioc.run();
});
mqtt_thread.detach();

// string bearerToken = getBearerToken();
// json topicList = getTopicList(bearerToken);







// EVENT

// size_t nsIdx = UA_Server_addNamespace(server, "urn:my.events");



// Create event creates a temp node that is deleted after trigger event , so one create for one trigger !!!


/* 1) Define the custom EventType ------------------------------------- */
UA_ObjectTypeAttributes attri = UA_ObjectTypeAttributes_default;
attri.displayName =
    UA_LOCALIZEDTEXT(const_cast<char *>("en-US"), const_cast<char *>("SimpleEventType"));
attri.description = UA_LOCALIZEDTEXT(
    const_cast<char *>("en-US"), const_cast<char *>("The simple event type we created"));

UA_NodeId eventType;
UA_Server_addObjectTypeNode(
    server, UA_NODEID_NULL, UA_NODEID_NUMERIC(0, UA_NS0ID_BASEEVENTTYPE),
    UA_NODEID_NUMERIC(0, UA_NS0ID_HASSUBTYPE),
    UA_QUALIFIEDNAME(0, const_cast<char *>("SimpleEventType")), attri, NULL, &eventType);

/* 3) Now you can instantiate and fill the event ---------------------- */
UA_NodeId eventNodeId;
UA_Server_createEvent(server, eventType, &eventNodeId);

UA_DateTime eventTime = UA_DateTime_now();
UA_Server_writeObjectProperty_scalar(server, eventNodeId,
                                     UA_QUALIFIEDNAME(0, const_cast<char *>("Time")),
                                     &eventTime, &UA_TYPES[UA_TYPES_DATETIME]);

UA_UInt16 eventSeverity = 100;
UA_Server_writeObjectProperty_scalar(server, eventNodeId,
                                     UA_QUALIFIEDNAME(0, const_cast<char *>("Severity")),
                                     &eventSeverity, &UA_TYPES[UA_TYPES_UINT16]);

UA_LocalizedText eventMessage = UA_LOCALIZEDTEXT(
    const_cast<char *>("en-US"), const_cast<char *>("An event has been generated."));
UA_Server_writeObjectProperty_scalar(server, eventNodeId,
                                     UA_QUALIFIEDNAME(0, const_cast<char *>("Message")),
                                     &eventMessage, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);

UA_String eventSourceName = UA_STRING(const_cast<char *>("Server"));
UA_Server_writeObjectProperty_scalar(
    server, eventNodeId, UA_QUALIFIEDNAME(0, const_cast<char *>("SourceName")),
    &eventSourceName, &UA_TYPES[UA_TYPES_STRING]);


/* 4) Finally, trigger the event (don't forget the SourceNode argument) */

UA_Server_triggerEvent(server, eventNodeId,
        UA_NODEID_NUMERIC(0, UA_NS0ID_SERVER), NULL, true);





    /* Create a rudimentary objectType hierarchy
     * BaseObjectType
     * |
     * +- (OT) AnimalType
     *    + (V) Age
     *    + (OT) DogType
     *      |
     *      + (V) Name
     */

    

    // Create AnimalType
    UA_ObjectTypeAttributes animalTypeAttr = UA_ObjectTypeAttributes_default;
        animalTypeAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "AnimalType");
        animalTypeAttr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", "A base type for all animals");
        UA_NodeId animalTypeId = UA_NODEID_STRING_ALLOC(1, "AnimalType");
        UA_QualifiedName animalTypeName = UA_QUALIFIEDNAME_ALLOC(1, "AnimalType");
        UA_Server_addObjectTypeNode(server, animalTypeId,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_BASEOBJECTTYPE),
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASSUBTYPE),
                                animalTypeName, animalTypeAttr, NULL, NULL);

    // Add Age variable to AnimalType
    UA_VariableAttributes ageAttr = UA_VariableAttributes_default;
        UA_Int32 age = 0;
        UA_Variant_setScalarCopy(&ageAttr.value, &age, &UA_TYPES[UA_TYPES_INT32]);
        ageAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Age");
        ageAttr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", "The age of the animal");
        ageAttr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
        UA_NodeId ageNodeId = UA_NODEID_STRING_ALLOC(1, "AnimalType.Age");
        UA_QualifiedName ageName = UA_QUALIFIEDNAME_ALLOC(1, "Age");
        UA_Server_addVariableNode(server, ageNodeId, animalTypeId,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                                ageName, UA_NODEID_NULL, ageAttr, NULL, NULL);

        // Create DogType
        UA_ObjectTypeAttributes dogTypeAttr = UA_ObjectTypeAttributes_default;
        dogTypeAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "DogType");
        dogTypeAttr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", "A type for dogs");
        UA_NodeId dogTypeId = UA_NODEID_STRING_ALLOC(1, "DogType");
        UA_QualifiedName dogTypeName = UA_QUALIFIEDNAME_ALLOC(1, "DogType");
        UA_Server_addObjectTypeNode(server, dogTypeId, animalTypeId,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASSUBTYPE),
                                dogTypeName, dogTypeAttr, NULL, NULL);

        // Add Name variable to DogType
        UA_VariableAttributes nameAttr = UA_VariableAttributes_default;
        UA_String name = UA_STRING_ALLOC("Unknown");
        UA_Variant_setScalarCopy(&nameAttr.value, &name, &UA_TYPES[UA_TYPES_STRING]);
        nameAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Name");
        nameAttr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", "The name of the dog");
        nameAttr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
        UA_NodeId nameNodeId = UA_NODEID_STRING_ALLOC(1, "DogType.Name");
        UA_QualifiedName nameName = UA_QUALIFIEDNAME_ALLOC(1, "Name");
        UA_Server_addVariableNode(server, nameNodeId, dogTypeId,
                                UA_NODEID_NUMERIC(0, UA_NS0ID_HASPROPERTY),
                                nameName, UA_NODEID_NULL, nameAttr, NULL, NULL);

        // Create an instance of DogType
        UA_ObjectAttributes dogInstanceAttr = UA_ObjectAttributes_default;
        dogInstanceAttr.displayName = UA_LOCALIZEDTEXT_ALLOC("en-US", "MyDog");
        dogInstanceAttr.description = UA_LOCALIZEDTEXT_ALLOC("en-US", "An instance of a dog");
        UA_NodeId dogInstanceId = UA_NODEID_STRING_ALLOC(1, "MyDog");
        UA_QualifiedName dogInstanceName = UA_QUALIFIEDNAME_ALLOC(1, "MyDog");
        UA_Server_addObjectNode(server, dogInstanceId,
                            UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
                            UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
                            dogInstanceName, dogTypeId, dogInstanceAttr, NULL, NULL);

   
   
    // Clean up allocated resources
    UA_ObjectTypeAttributes_clear(&animalTypeAttr);
    UA_ObjectTypeAttributes_clear(&dogTypeAttr);
    UA_ObjectAttributes_clear(&dogInstanceAttr);
    UA_VariableAttributes_clear(&ageAttr);
    UA_VariableAttributes_clear(&nameAttr);
    UA_NodeId_clear(&animalTypeId);
    UA_NodeId_clear(&dogTypeId);
    UA_NodeId_clear(&dogInstanceId);
    UA_NodeId_clear(&ageNodeId);
    UA_NodeId_clear(&nameNodeId);
    UA_QualifiedName_clear(&animalTypeName);
    UA_QualifiedName_clear(&dogTypeName);
    UA_QualifiedName_clear(&dogInstanceName);
    UA_QualifiedName_clear(&ageName);
    UA_QualifiedName_clear(&nameName);
    UA_String_clear(&name);






    g_counterNodeId = &myDoubleNodeId;
    g_eventNodeId = &eventNodeId;
    UA_Server_addRepeatedCallback(server, updateCounterAndTriggerEvent, NULL, 10000, NULL);
    
    log("Starting OPC UA Server...", LogLevel::INFO);
    log("Added repeated callback for counter updates", LogLevel::DEBUG);

    UA_Server_run_startup(server);
        log("Server startup completed successfully", LogLevel::INFO);
    log("Server is ready to accept connections", LogLevel::INFO);
    // Register server with LDS now that it's running

            // register server
        // UA_ClientConfig cc;
        // memset(&cc, 0, sizeof(UA_ClientConfig));
        // UA_ClientConfig_setDefault(&cc);

        // UA_ByteString client_cert = loadFile("client/own/certs/client_cert.der");
        // UA_ByteString client_key = loadFile("client/own/certs/client_key.der");
        // UA_ByteString server_cert = loadFile("server/own/certs/server_cert.der");
        // UA_ByteString ca_cert = loadFile("ca/certs/ca.crt");
        // UA_ByteString revocation_cert = loadFile("server/trusted/crl/crl.crl");

        //     if (certificate.length == 0) {
        //         UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load client certificate"); 
        //         return EXIT_FAILURE;
        //     }
        //     if (privateKey.length == 0) {
        //         UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load client private key"); 
        //         return EXIT_FAILURE;
        //     }
        //     if (serverCerte.length == 0) {
        //         UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to load server certificate into trust list"); 
        //         return EXIT_FAILURE;
        //     }

        // UA_STACKARRAY(UA_ByteString, trustLists, 1);
        // trustLists[0] = serverCerte;
    
        // // Step 3: Apply encryption
        // UA_ClientConfig_setDefaultEncryption(&cc, certificatee, privateKeye, NULL, 0,
        // NULL, 0);

        // UA_CertificateGroup_AcceptAll(&cc.certificateVerification);

        // //cc.securityMode = UA_MESSAGESECURITYMODE_NONE;
        // //cc.securityPolicyUri =
        // //UA_STRING_STATIC("http://opcfoundation.org/UA/SecurityPolicy#None");

        // //UA_String_clear(&cc.applicationUri);
        // cc.clientDescription.applicationUri =
        // UA_STRING_ALLOC("urn:Anexee.server.application");
        // //cc.clientDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en-US",
        // //"Anexee"); 
        // //cc.clientDescription.productUri =
        // //UA_STRING_ALLOC("urn:Anexee.server");

        // //cc.userTokenPolicy.securityPolicyUri =
        // //    UA_STRING_STATIC("http://opcfoundation.org/UA/SecurityPolicy#None");
        // //

        // //cc.clientDescription.applicationUri =
        // //    UA_STRING_ALLOC("urn:Anexee.server.application");
        // cc.clientDescription.productUri = UA_STRING_ALLOC("urn:Anexee.server");
        // cc.clientDescription.applicationName = UA_LOCALIZEDTEXT_ALLOC("en-US", "Anexee");
        // cc.clientDescription.applicationType = UA_APPLICATIONTYPE_SERVER;

        // cc.endpointUrl = UA_STRING_ALLOC("opc.tcp://localhost:4840");

        // cc.userTokenPolicy.tokenType = UA_USERTOKENTYPE_ANONYMOUS;
        // cc.userTokenPolicy.policyId = UA_STRING_ALLOC("anonymous-policy");

        // cc.securityPolicyUri =
        //     UA_STRING_ALLOC("http://opcfoundation.org/UA/SecurityPolicy#None");
        // cc.securityMode = UA_MESSAGESECURITYMODE_NONE;

        // // Set if LDS requires user credentials (rare):
        // //cc.userIdentityToken.encoding = UA_EXTENSIONOBJECT_DECODED;
        // //cc.userIdentityToken.content.decoded.type =
        // //    &UA_TYPES[UA_TYPES_USERNAMEIDENTITYTOKEN];
        // //cc.userIdentityToken.content.decoded.data = UA_UserNameIdentityToken_new();
        // //UA_UserNameIdentityToken *token =
        // //    (UA_UserNameIdentityToken *)cc.userIdentityToken.content.decoded.data;
       

        // cc.applicationUri = UA_STRING_ALLOC("urn:Anexee.server.application");



        // const char *discoveryUrlStr = "opc.tcp://Asce:4840";
        // UA_String discoveryUrl = UA_String_fromChars(discoveryUrlStr);

       


        // UA_StatusCode result = UA_Server_registerDiscovery(server, &cc, discoveryUrl, UA_STRING_NULL);
        // if(result != UA_STATUSCODE_GOOD) {
        //    UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
        //                 "Could not create periodic job for server register. StatusCode %s",
        //                 UA_StatusCode_name(result));
        //    UA_Server_delete(server);
        //    return EXIT_FAILURE;
        // }

    log("Server is now running and listening for connections", LogLevel::INFO);
    
    while(running) {
        UA_Server_run_iterate(server, true);
    }
    
    log("Server shutdown initiated", LogLevel::INFO);
    //    // Unregister from LDS before shutdown
    //    memset(&cc, 0, sizeof(UA_ClientConfig));
    //    UA_ClientConfig_setDefault(&cc);

    //    cc.endpoint.securityMode = UA_MESSAGESECURITYMODE_NONE;
    //    
    //    cc.endpoint.userIdentityTokensSize = 1;
    //    cc.endpoint.userIdentityTokens = (UA_UserTokenPolicy *) UA_Array_new(1, &UA_TYPES[UA_TYPES_USERTOKENPOLICY]);
    //    UA_UserTokenPolicy_init(&cc.endpoint.userIdentityTokens[0]);
    //    cc.endpoint.userIdentityTokens[0].tokenType = UA_USERTOKENTYPE_ANONYMOUS;
    //    cc.endpoint.userIdentityTokens[0].policyId = UA_String_fromChars("open62541-anonymous-policy");
    //    UA_ByteString_clear(&cc.securityPolicyUri);
    //    cc.endpoint.userIdentityTokens[0].securityPolicyUri = UA_String_fromChars("http://opcfoundation.org/UA/SecurityPolicy#None");

    //    for(size_t j = 0; j < cc.endpoint.userIdentityTokensSize; j++) {
    //        UA_UserTokenPolicy *pol = &cc.endpoint.userIdentityTokens[j];
    //        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_CLIENT, "Deregistration Client TokenType: %d, PolicyId: %.*s, SecurityPolicyUri: %.*s",
    //            pol->tokenType,
    //            (int)pol->policyId.length, pol->policyId.data,
    //            (int)pol->securityPolicyUri.length, pol->securityPolicyUri.data);
    //    }

    //    UA_StatusCode res = UA_Server_deregisterDiscovery(server, &cc, discoveryUrl);
    //    if(res != UA_STATUSCODE_GOOD)
    //        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
    //                     "Could not unregister from discovery server. StatusCode %s",
    //                     UA_StatusCode_name(res));
    //    else
    //        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Unregistered from discovery server.");


    log("Cleaning up server resources", LogLevel::DEBUG);
    
    UA_VariableAttributes_clear(&attr);
    UA_VariableAttributes_clear(&attr2);
    //UA_VariableAttributes_clear(&attr3);
    UA_NodeId_clear(&myIntegerNodeId);
    UA_NodeId_clear(&myDoubleNodeId);
    //UA_NodeId_clear(&minNodeId);
    UA_QualifiedName_clear(&myIntegerName);
    UA_QualifiedName_clear(&myDoubleName);
    //UA_QualifiedName_clear(&minName);



    // Clean up security policies
    for(size_t i = 0; i < config->securityPoliciesSize; i++) {
        config->securityPolicies[i].clear(&config->securityPolicies[i]);
    }

    UA_ByteString_clear(&certificate);
    UA_ByteString_clear(&privateKey);

    if(trustList) {
        for(size_t i = 0; i < trustListSize; i++)
            UA_ByteString_clear(&trustList[i]);
        UA_free(trustList);
    }
    
    if(issuerList) {
        for(size_t i = 0; i < issuerListSize; i++)
            UA_ByteString_clear(&issuerList[i]);
        UA_free(issuerList);
    }
    // Clean up method callback contexts
    // Note: In a production environment, you might want to keep track of all allocated contexts
    // and clean them up individually. For this example, the server will handle most cleanup.
    // The MethodCallbackContext structures are stored as node contexts and will be cleaned up
    // when the server is deleted.
    log("Deleting server instance", LogLevel::DEBUG);
    UA_Server_delete(server);
    
    if(retval == UA_STATUSCODE_GOOD) {
        log("Server shutdown completed successfully", LogLevel::INFO);
        return EXIT_SUCCESS;
    } else {
        log("Server shutdown completed with errors", LogLevel::ERRORS);
        return EXIT_FAILURE;
    }
}




// Build an OPC UA Server that dynamically updates its address space using data received via MQTT, which in turn is sourced from a database.
