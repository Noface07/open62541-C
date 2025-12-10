#ifndef ALARM_HANDLER_H
#define ALARM_HANDLER_H

#include <open62541/server.h>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp> // Added for json type in function declaration

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
 * @brief Holds all necessary information for managing an alarm associated with a process variable.
 *
 * This structure stores the NodeIds for both the process variable and its corresponding
 * alarm instance, as well as the alarm's configuration limits and current state.
 */
struct MonitoredNodeAlarmInfo {
    UA_NodeId processNodeId;
    UA_NodeId alarmInstanceId;
    double alarmHiHi;
    double alarmHi;
    double alarmLo;
    double alarmLoLo;
    double deadband;
    std::string displayName;
    bool acked = false; // Acknowledged state
    bool callbackSetup = false; // Flag to track if method callback is already set up
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
extern std::mutex g_alarmMutex;
extern void GlobalMQTT_Subscribe(const std::string &topic);

/**
 * @brief A global map to store and access alarm information for each monitored process node.
 *
 * The key is the UA_NodeId of the process variable (e.g., a sensor reading).
 * The value is the MonitoredNodeAlarmInfo struct containing all related alarm data.
 *
 * Declared as 'extern' so it can be accessed by any file that includes this header,
 * while being defined in a single .cpp file.
 */
extern std::map<UA_NodeId, MonitoredNodeAlarmInfo, UA_NodeId_less_than> monitoredAlarms;

/**
 * @brief Creates an instance of ExclusiveLimitAlarmType and links it to a process node.
 *
 * @param server The UA_Server instance.
 * @param processNodeId The NodeId of the variable that this alarm is monitoring.
 * @param displayName The display name for the alarm, used to generate the alarm node's name.
 * @param item A JSON object containing alarm limit configurations (e.g., alarmHiHi, alarmHi).
 * @param outAlarmInstanceId A pointer to a UA_NodeId where the new alarm's NodeId will be stored.
 * @return UA_StatusCode indicating the result of the operation.
 */
UA_StatusCode createAndLinkExclusiveLimitAlarm(UA_Server *server,
                                             const UA_NodeId *processNodeId,
                                             const std::string &displayName,
                                             const nlohmann::json &item,
                                             UA_NodeId *outAlarmInstanceId);


// --- Alarm Method Callbacks (Exposed for Multi-Tenancy) ---

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

#endif // ALARM_HANDLER_H
