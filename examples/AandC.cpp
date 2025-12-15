// /* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
//  * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

// #include <open62541/plugin/log_stdout.h>
// #include <open62541/server.h>
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

// #include <pqxx/pqxx>

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

#include <nlohmann/json.hpp>
using json = nlohmann::json;

using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;       


struct UA_NodeId_less_than {
    bool operator()(const UA_NodeId& lhs, const UA_NodeId& rhs) const {
        return UA_NodeId_order(&lhs, &rhs) < 0;
    }
};

struct MonitoredNodeAlarmInfo {
    UA_NodeId processNodeId;
    UA_NodeId alarmInstanceId;
    double alarmHiHi;
    double alarmHi;
    double alarmLo;
    double alarmLoLo;
    double deadband; // Add deadband for hysteresis
    std::string displayName; // To use in alarm messages
    bool acked = false; //Acknowledged state
    bool callbackSetup = false; // Flag to track if method callback is already set up
};
std::map<UA_NodeId, MonitoredNodeAlarmInfo , UA_NodeId_less_than> monitoredAlarms;



// --- Alarm Creation Function ---
UA_StatusCode createAndLinkExclusiveLimitAlarm(UA_Server *server,
                                                      const UA_NodeId *processNodeId,
                                                      const std::string &displayName,
                                                      const nlohmann::json &item,
                                                      UA_NodeId *outAlarmInstanceId) {

    std::string alarmName = displayName + "_ExclusiveLimitAlarm";
    UA_QualifiedName alarmQualifiedName = UA_QUALIFIEDNAME_ALLOC(0, alarmName.c_str());

    // UA_Server_addReference(server,
    // UA_NODEID_NUMERIC(0, 54624),
    // UA_NS0ID(HASCOMPONENT),
    // UA_EXPANDEDNODEID_NUMERIC(processNodeId->namespaceIndex, processNodeId->identifier.numeric),
    // UA_TRUE);

    // UA_Server_addReference(server,
    // UA_NODEID_NUMERIC(0, 54624),
    // UA_NS0ID(HASEVENTSOURCE),
    // UA_EXPANDEDNODEID_NUMERIC(processNodeId->namespaceIndex, processNodeId->identifier.numeric),
    // UA_TRUE);

    UA_Server_addReference(server,
    UA_NODEID_STRING(1,(char *)"TDSPL/PLANT-001"),
    UA_NS0ID(HASEVENTSOURCE),
    UA_EXPANDEDNODEID_NUMERIC(processNodeId->namespaceIndex, processNodeId->identifier.numeric),
    UA_TRUE);

    UA_Server_addReference(server, UA_NODEID_STRING(1, (char *)"TDSPL/PLANT-001"),
                           UA_NS0ID(HASNOTIFIER),
                           UA_EXPANDEDNODEID_NUMERIC(processNodeId->namespaceIndex,
                                                     processNodeId->identifier.numeric),
        UA_TRUE);


    // Now create the alarm
    UA_StatusCode retval = UA_Server_createCondition(server,
        UA_NODEID_NULL,                                   // Parent node
        UA_NS0ID(EXCLUSIVELIMITALARMTYPE),
        alarmQualifiedName,
        *processNodeId,                               // Source node for events
        UA_NS0ID(HASCOMPONENT),                               // No ref in address space
        outAlarmInstanceId);

    if (retval != UA_STATUSCODE_GOOD) {
        UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "Failed to create ExclusiveLimitAlarm for %s. StatusCode %s",
                     displayName.c_str(), UA_StatusCode_name(retval));
        return retval;
    }

    UA_Variant value;
    UA_Boolean boolFalse = false;
    UA_Boolean boolTrue = true;
    UA_UInt16 initialSeverity = 0;
    UA_LocalizedText message = UA_LOCALIZEDTEXT_ALLOC("en", "Normal");


    // EnabledState.Id = true
    UA_QualifiedName enabledStateName = UA_QUALIFIEDNAME_ALLOC(0, "EnabledState");
    UA_QualifiedName idName = UA_QUALIFIEDNAME_ALLOC(0, "Id");
    UA_Variant_setScalar(&value, &boolTrue, &UA_TYPES[UA_TYPES_BOOLEAN]);
    UA_Server_setConditionVariableFieldProperty(server, *outAlarmInstanceId, &value, enabledStateName, idName);


    // ActiveState.Id = false
    UA_QualifiedName activeStateName = UA_QUALIFIEDNAME_ALLOC(0, "ActiveState");
    UA_QualifiedName idName2 = UA_QUALIFIEDNAME_ALLOC(0, "Id");
    UA_Variant_setScalar(&value, &boolFalse, &UA_TYPES[UA_TYPES_BOOLEAN]);
    UA_Server_setConditionVariableFieldProperty(server, *outAlarmInstanceId, &value, activeStateName, idName2);


    // AckedState.Id = False
    UA_QualifiedName ackedStateName = UA_QUALIFIEDNAME_ALLOC(0, "AckedState");
    UA_QualifiedName idName3 = UA_QUALIFIEDNAME_ALLOC(0, "Id");
    UA_Variant_setScalar(&value, &boolFalse, &UA_TYPES[UA_TYPES_BOOLEAN]);
    UA_Server_setConditionVariableFieldProperty(server, *outAlarmInstanceId, &value, ackedStateName, idName3);


    // ConfirmedState.Id = true
    UA_QualifiedName confirmedStateName = UA_QUALIFIEDNAME_ALLOC(0, "ConfirmedState");
    UA_QualifiedName idName4 = UA_QUALIFIEDNAME_ALLOC(0, "Id");
    UA_Variant_setScalar(&value, &boolTrue, &UA_TYPES[UA_TYPES_BOOLEAN]);
    UA_Server_setConditionVariableFieldProperty(server, *outAlarmInstanceId, &value, confirmedStateName, idName4);


    // Retain = true
    UA_Variant_setScalar(&value, &boolTrue, &UA_TYPES[UA_TYPES_BOOLEAN]);
    UA_Server_setConditionField(server, *outAlarmInstanceId, &value, UA_QUALIFIEDNAME(0, const_cast<char *>("Retain")));

    // Message = "Normal"
    UA_Variant_setScalar(&value, &message, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
    UA_Server_setConditionField(server, *outAlarmInstanceId, &value, UA_QUALIFIEDNAME(0, const_cast<char *>("Message")));


    // Severity = 0
    UA_Variant_setScalar(&value, &initialSeverity, &UA_TYPES[UA_TYPES_UINT16]);
    UA_Server_setConditionField(server, *outAlarmInstanceId, &value, UA_QUALIFIEDNAME(0, const_cast<char *>("Severity")));

    // Write limit properties
    if (item.contains("alarmHiHi")) {
        double v = item["alarmHiHi"].get<double>();
        UA_Server_writeObjectProperty_scalar(server, *outAlarmInstanceId,
                                             UA_QUALIFIEDNAME(0, const_cast<char *>("HighHighLimit")),
                                             &v, &UA_TYPES[UA_TYPES_DOUBLE]);
    }
    if (item.contains("alarmHi")) {
        double v = item["alarmHi"].get<double>();
        UA_Server_writeObjectProperty_scalar(server, *outAlarmInstanceId,
                                             UA_QUALIFIEDNAME(0, const_cast<char *>("HighLimit")),
                                             &v, &UA_TYPES[UA_TYPES_DOUBLE]);
    }
    if (item.contains("alarmLo")) {
        double v = item["alarmLo"].get<double>();
        UA_Server_writeObjectProperty_scalar(server, *outAlarmInstanceId,
                                             UA_QUALIFIEDNAME(0, const_cast<char *>("LowLimit")),
                                             &v, &UA_TYPES[UA_TYPES_DOUBLE]);
    }
    if (item.contains("alarmLoLo")) {
        double v = item["alarmLoLo"].get<double>();
        UA_Server_writeObjectProperty_scalar(server, *outAlarmInstanceId,
                                             UA_QUALIFIEDNAME(0, const_cast<char *>("LowLowLimit")),
                                             &v, &UA_TYPES[UA_TYPES_DOUBLE]);
    }

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                "Created ExclusiveLimitAlarm %s (NodeId: ns=%d;i=%d) for Process Node (ns=%d;i=%d)",
                alarmName.c_str(),
                outAlarmInstanceId->namespaceIndex, outAlarmInstanceId->identifier.numeric,
                processNodeId->namespaceIndex, processNodeId->identifier.numeric);
    
    // cleanup
    UA_QualifiedName_clear(&alarmQualifiedName);
    UA_QualifiedName_clear(&enabledStateName);
    UA_QualifiedName_clear(&idName);
    UA_QualifiedName_clear(&activeStateName);
    UA_QualifiedName_clear(&idName2);
    UA_QualifiedName_clear(&ackedStateName);
    UA_QualifiedName_clear(&idName3);
    UA_QualifiedName_clear(&confirmedStateName);
    UA_QualifiedName_clear(&idName4);
    UA_LocalizedText_clear(&message);

    return UA_STATUSCODE_GOOD;
}

