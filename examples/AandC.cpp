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

#include <nlohmann/json.hpp>
using json = nlohmann::json;

using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;       

// /**
//  * Using Alarms and Conditions Server
//  * ----------------------------------
//  *
//  * Besides the usage of monitored items and events to observe the changes in the
//  * server, it is also important to make use of the Alarms and Conditions Server
//  * Model. Alarms are events which are triggered automatically by the server
//  * dependent on internal server logic or user specific logic when the states of
//  * server components change. The state of a component is represented through a
//  * condition. So the values of all the condition children (Fields) are the
//  * actual state of the component.
//  *
//  * Trigger Alarm events by changing States
//  * ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//  *
//  * The following example will be based on the server events tutorial. Please
//  * make sure to understand the principle of normal events before proceeding with
//  * this example! */

// static UA_NodeId conditionSource;
// static UA_NodeId conditionInstance_1;
// static UA_NodeId conditionInstance_2;

// static UA_StatusCode
// addConditionSourceObject(UA_Server *server) {
//     UA_ObjectAttributes object_attr = UA_ObjectAttributes_default;
//     object_attr.eventNotifier = 1;

//     object_attr.displayName = UA_LOCALIZEDTEXT("en", "ConditionSourceObject");
//     UA_StatusCode retval =  UA_Server_addObjectNode(server, UA_NODEID_NULL,
//                                       UA_NS0ID(OBJECTSFOLDER), UA_NS0ID(ORGANIZES),
//                                       UA_QUALIFIEDNAME(0, "ConditionSourceObject"),
//                                       UA_NS0ID(BASEOBJECTTYPE),
//                                       object_attr, NULL, &conditionSource);

//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Creating Condition Source failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//     }

//     /* ConditionSource should be EventNotifier of another Object (usually the
//      * Server Object). If this Reference is not created by user then the A&C
//      * Server will create "HasEventSource" reference to the Server Object
//      * automatically when the condition is created*/
//     retval = UA_Server_addReference(server, UA_NS0ID(SERVER), UA_NS0ID(HASNOTIFIER),
//                                      UA_EXPANDEDNODEID_NUMERIC(conditionSource.namespaceIndex,
//                                                                conditionSource.identifier.numeric),
//                                      UA_TRUE);

//     return retval;
// }

// /**
//  * Create a condition instance from OffNormalAlarmType. The condition source is
//  * the Object created in addConditionSourceObject(). The condition will be
//  * exposed in Address Space through the HasComponent reference to the condition
//  * source. */
// static UA_StatusCode
// addCondition_1(UA_Server *server) {
//     UA_StatusCode retval = addConditionSourceObject(server);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "creating Condition Source failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//     }

//     retval = UA_Server_createCondition(server, UA_NODEID_NULL, UA_NS0ID(OFFNORMALALARMTYPE),
//                                        UA_QUALIFIEDNAME(0, "Condition 1"), conditionSource,
//                                        UA_NS0ID(HASCOMPONENT), &conditionInstance_1);

//     return retval;
// }

// /**
//  * Create a condition instance from OffNormalAlarmType. The condition source is
//  * the server Object. The condition won't be exposed in Address Space. */
// static UA_StatusCode
// addCondition_2(UA_Server *server) {
//     UA_StatusCode retval =
//         UA_Server_createCondition(server, UA_NODEID_NULL, UA_NS0ID(OFFNORMALALARMTYPE),
//                                   UA_QUALIFIEDNAME(0, "Condition 2"), UA_NS0ID(SERVER),
//                                   UA_NODEID_NULL, &conditionInstance_2);

//     return retval;
// }

// static void
// addVariable_1_triggerAlarmOfCondition_1(UA_Server *server, UA_NodeId* outNodeId) {
//     UA_VariableAttributes attr = UA_VariableAttributes_default;
//     attr.displayName = UA_LOCALIZEDTEXT("en", "Activate Condition 1");
//     attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
//     UA_Boolean tboolValue = UA_FALSE;
//     UA_Variant_setScalar(&attr.value, &tboolValue, &UA_TYPES[UA_TYPES_BOOLEAN]);

//     UA_QualifiedName CallbackTestVariableName = UA_QUALIFIEDNAME(0, "Activate Condition 1");
//     UA_NodeId parentNodeId = UA_NS0ID(OBJECTSFOLDER);
//     UA_NodeId parentReferenceNodeId = UA_NS0ID(ORGANIZES);
//     UA_NodeId variableTypeNodeId = UA_NS0ID(BASEDATAVARIABLETYPE);
//     UA_Server_addVariableNode(server, UA_NODEID_NULL, parentNodeId,
//                               parentReferenceNodeId, CallbackTestVariableName,
//                               variableTypeNodeId, attr, NULL, outNodeId);
// }

// static void
// addVariable_2_changeSeverityOfCondition_2(UA_Server *server,
//                                           UA_NodeId* outNodeId) {
//     UA_VariableAttributes attr = UA_VariableAttributes_default;
//     attr.displayName = UA_LOCALIZEDTEXT("en", "Change Severity Condition 2");
//     attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
//     UA_UInt16 severityValue = 0;
//     UA_Variant_setScalar(&attr.value, &severityValue, &UA_TYPES[UA_TYPES_UINT16]);

//     UA_QualifiedName CallbackTestVariableName =
//         UA_QUALIFIEDNAME(0, "Change Severity Condition 2");
//     UA_NodeId parentNodeId = UA_NS0ID(OBJECTSFOLDER);
//     UA_NodeId parentReferenceNodeId = UA_NS0ID(ORGANIZES);
//     UA_NodeId variableTypeNodeId = UA_NS0ID(BASEDATAVARIABLETYPE);
//     UA_Server_addVariableNode(server, UA_NODEID_NULL, parentNodeId,
//                               parentReferenceNodeId, CallbackTestVariableName,
//                               variableTypeNodeId, attr, NULL, outNodeId);
// }

// static void
// addVariable_3_returnCondition_1_toNormalState(UA_Server *server,
//                                               UA_NodeId* outNodeId) {
//     UA_VariableAttributes attr = UA_VariableAttributes_default;
//     attr.displayName = UA_LOCALIZEDTEXT("en", "Return to Normal Condition 1");
//     attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
//     UA_Boolean rtn = 0;
//     UA_Variant_setScalar(&attr.value, &rtn, &UA_TYPES[UA_TYPES_BOOLEAN]);

//     UA_QualifiedName CallbackTestVariableName =
//         UA_QUALIFIEDNAME(0, "Return to Normal Condition 1");
//     UA_NodeId parentNodeId = UA_NS0ID(OBJECTSFOLDER);
//     UA_NodeId parentReferenceNodeId = UA_NS0ID(ORGANIZES);
//     UA_NodeId variableTypeNodeId = UA_NS0ID(BASEDATAVARIABLETYPE);
//     UA_Server_addVariableNode(server, UA_NODEID_NULL, parentNodeId,
//                               parentReferenceNodeId, CallbackTestVariableName,
//                               variableTypeNodeId, attr, NULL, outNodeId);
// }

// static void
// afterWriteCallbackVariable_1(UA_Server *server, const UA_NodeId *sessionId,
//                              void *sessionContext, const UA_NodeId *nodeId,
//                              void *nodeContext, const UA_NumericRange *range,
//                              const UA_DataValue *data) {
//     UA_QualifiedName activeStateField = UA_QUALIFIEDNAME(0,"ActiveState");
//     UA_QualifiedName activeStateIdField = UA_QUALIFIEDNAME(0,"Id");
//     UA_Variant value;

//     UA_StatusCode retval =
//         UA_Server_writeObjectProperty_scalar(server, conditionInstance_1,
//                                              UA_QUALIFIEDNAME(0, "Time"),
//                                              &data->sourceTimestamp,
//                                              &UA_TYPES[UA_TYPES_DATETIME]);

//     if(*(UA_Boolean *)(data->value.data) == true) {
//         /* By writing "true" in ActiveState/Id, the A&C server will set the
//          * related fields automatically and then will trigger event
//          * notification. */
//         UA_Boolean activeStateId = true;
//         UA_Variant_setScalar(&value, &activeStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//         retval |= UA_Server_setConditionVariableFieldProperty(server, conditionInstance_1,
//                                                               &value, activeStateField,
//                                                               activeStateIdField);
//         if(retval != UA_STATUSCODE_GOOD) {
//             UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                          "Setting ActiveState/Id Field failed. StatusCode %s",
//                          UA_StatusCode_name(retval));
//             return;
//         }
//     } else {
//         /* By writing "false" in ActiveState/Id, the A&C server will set only
//          * the ActiveState field automatically to the value "Inactive". The user
//          * should trigger the event manually by calling
//          * UA_Server_triggerConditionEvent inside the application or call
//          * ConditionRefresh method with client to update the event notification. */
//         UA_Boolean activeStateId = false;
//         UA_Variant_setScalar(&value, &activeStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//         retval = UA_Server_setConditionVariableFieldProperty(server, conditionInstance_1,
//                                                              &value, activeStateField,
//                                                              activeStateIdField);
//         if(retval != UA_STATUSCODE_GOOD) {
//             UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                          "Setting ActiveState/Id Field failed. StatusCode %s",
//                          UA_StatusCode_name(retval));
//             return;
//         }

//         retval = UA_Server_triggerConditionEvent(server, conditionInstance_1,
//                                                  conditionSource, NULL);
//         if(retval != UA_STATUSCODE_GOOD) {
//             UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                            "Triggering condition event failed. StatusCode %s",
//                            UA_StatusCode_name(retval));
//             return;
//         }
//     }
// }

// /**
//  * The callback only changes the severity field of the condition 2. The severity
//  * field is of ConditionVariableType, so changes in it triggers an event
//  * notification automatically by the server. */
// static void
// afterWriteCallbackVariable_2(UA_Server *server, const UA_NodeId *sessionId,
//                              void *sessionContext, const UA_NodeId *nodeId,
//                              void *nodeContext, const UA_NumericRange *range,
//                              const UA_DataValue *data) {
//    /* Another way to set fields of conditions */
//     UA_Server_writeObjectProperty_scalar(server, conditionInstance_2,
//                                          UA_QUALIFIEDNAME(0, "Severity"),
//                                          (UA_UInt16 *)data->value.data,
//                                          &UA_TYPES[UA_TYPES_UINT16]);
// }

// /**
//  * RTN = return to normal.
//  *
//  * Retain will be set to false, thus no events will be generated for condition 1
//  * (although EnabledState/=true). To set Retain to true again, the disable and
//  * enable methods should be called respectively.
//  */
// static void
// afterWriteCallbackVariable_3(UA_Server *server,
//                const UA_NodeId *sessionId, void *sessionContext,
//                const UA_NodeId *nodeId, void *nodeContext,
//                const UA_NumericRange *range, const UA_DataValue *data) {

//     //UA_QualifiedName enabledStateField = UA_QUALIFIEDNAME(0,"EnabledState");
//     UA_QualifiedName ackedStateField = UA_QUALIFIEDNAME(0,"AckedState");
//     UA_QualifiedName confirmedStateField = UA_QUALIFIEDNAME(0,"ConfirmedState");
//     UA_QualifiedName activeStateField = UA_QUALIFIEDNAME(0,"ActiveState");
//     UA_QualifiedName severityField = UA_QUALIFIEDNAME(0,"Severity");
//     UA_QualifiedName messageField = UA_QUALIFIEDNAME(0,"Message");
//     UA_QualifiedName commentField = UA_QUALIFIEDNAME(0,"Comment");
//     UA_QualifiedName retainField = UA_QUALIFIEDNAME(0,"Retain");
//     UA_QualifiedName idField = UA_QUALIFIEDNAME(0,"Id");

//     UA_StatusCode retval =
//         UA_Server_writeObjectProperty_scalar(server, conditionInstance_1,
//                                              UA_QUALIFIEDNAME(0, "Time"),
//                                              &data->serverTimestamp,
//                                              &UA_TYPES[UA_TYPES_DATETIME]);
//     UA_Variant value;
//     UA_Boolean idValue = false;
//     UA_Variant_setScalar(&value, &idValue, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     retval |= UA_Server_setConditionVariableFieldProperty(server, conditionInstance_1,
//                                                           &value, activeStateField,
//                                                           idField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting ActiveState/Id Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return;
//     }

//     retval = UA_Server_setConditionVariableFieldProperty(server, conditionInstance_1,
//                                                          &value, ackedStateField,
//                                                          idField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting AckedState/Id Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return;
//     }

//     retval = UA_Server_setConditionVariableFieldProperty(server, conditionInstance_1,
//                                                          &value, confirmedStateField,
//                                                          idField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting ConfirmedState/Id Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return;
//     }

//     UA_UInt16 severityValue = 100;
//     UA_Variant_setScalar(&value, &severityValue, &UA_TYPES[UA_TYPES_UINT16]);
//     retval = UA_Server_setConditionField(server, conditionInstance_1,
//                                          &value, severityField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting Severity Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return;
//     }

//     UA_LocalizedText messageValue =
//         UA_LOCALIZEDTEXT("en", "Condition returned to normal state");
//     UA_Variant_setScalar(&value, &messageValue, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
//     retval = UA_Server_setConditionField(server, conditionInstance_1,
//                                          &value, messageField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting Message Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return;
//     }

//     UA_LocalizedText commentValue = UA_LOCALIZEDTEXT("en", "Normal State");
//     UA_Variant_setScalar(&value, &commentValue, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
//     retval = UA_Server_setConditionField(server, conditionInstance_1,
//                                          &value, commentField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting Comment Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return;
//     }

//     UA_Boolean retainValue = false;
//     UA_Variant_setScalar(&value, &retainValue, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     retval = UA_Server_setConditionField(server, conditionInstance_1,
//                                          &value, retainField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting Retain Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return;
//     }

//     retval = UA_Server_triggerConditionEvent(server, conditionInstance_1,
//                                              conditionSource, NULL);
//     if (retval != UA_STATUSCODE_GOOD) {
//      UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                     "Triggering condition event failed. StatusCode %s",
//                     UA_StatusCode_name(retval));
//      return;
//     }
// }

// static UA_StatusCode
// enteringEnabledStateCallback(UA_Server *server, const UA_NodeId *condition) {
//     UA_Boolean retain = true;
//     return UA_Server_writeObjectProperty_scalar(server, *condition,
//                                                 UA_QUALIFIEDNAME(0, "Retain"),
//                                                 &retain,
//                                                 &UA_TYPES[UA_TYPES_BOOLEAN]);
// }

// /**
//  * This is user specific function which will be called upon acknowledging an
//  * alarm notification. In this example we will set the Alarm to Inactive state.
//  * The server is responsible of setting standard fields related to Acknowledge
//  * Method and triggering the alarm notification. */
// static UA_StatusCode
// enteringAckedStateCallback(UA_Server *server, const UA_NodeId *condition) {
//     /* deactivate Alarm when acknowledging*/
//     UA_Boolean activeStateId = false;
//     UA_Variant value;
//     UA_QualifiedName activeStateField = UA_QUALIFIEDNAME(0,"ActiveState");
//     UA_QualifiedName activeStateIdField = UA_QUALIFIEDNAME(0,"Id");

//     UA_Variant_setScalar(&value, &activeStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     UA_StatusCode retval =
//         UA_Server_setConditionVariableFieldProperty(server, *condition,
//                                                     &value, activeStateField,
//                                                     activeStateIdField);

//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting ActiveState/Id Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//     }

//     return retval;
// }

// static UA_StatusCode
// enteringConfirmedStateCallback(UA_Server *server, const UA_NodeId *condition) {
//     /* Deactivate Alarm and put it out of the interesting state (by writing
//      * false to Retain field) when confirming*/
//     UA_Boolean activeStateId = false;
//     UA_Boolean retain = false;
//     UA_Variant value;
//     UA_QualifiedName activeStateField = UA_QUALIFIEDNAME(0,"ActiveState");
//     UA_QualifiedName activeStateIdField = UA_QUALIFIEDNAME(0,"Id");
//     UA_QualifiedName retainField = UA_QUALIFIEDNAME(0,"Retain");

//     UA_Variant_setScalar(&value, &activeStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     UA_StatusCode retval =
//         UA_Server_setConditionVariableFieldProperty(server, *condition,
//                                                     &value, activeStateField,
//                                                     activeStateIdField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting ActiveState/Id Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     UA_Variant_setScalar(&value, &retain, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     retval = UA_Server_setConditionField(server, *condition,
//                                          &value, retainField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting ActiveState/Id Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//     }

//     return retval;
// }

// static UA_StatusCode
// setUpEnvironment(UA_Server *server) {
//     UA_NodeId variable_1;
//     UA_NodeId variable_2;
//     UA_NodeId variable_3;
//     UA_ValueCallback callback;
//     callback.onRead = NULL;

//     /* Exposed condition 1. We will add to it user specific callbacks when
//      * entering enabled state, when acknowledging and when confirming. */
//     UA_StatusCode retval = addCondition_1(server);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "adding condition 1 failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     UA_TwoStateVariableChangeCallback userSpecificCallback = enteringEnabledStateCallback;
//     retval = UA_Server_setConditionTwoStateVariableCallback(server, conditionInstance_1,
//                                                             conditionSource, false,
//                                                             userSpecificCallback,
//                                                             UA_ENTERING_ENABLEDSTATE);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "adding entering enabled state callback failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     userSpecificCallback = enteringAckedStateCallback;
//     retval = UA_Server_setConditionTwoStateVariableCallback(server, conditionInstance_1,
//                                                             conditionSource, false,
//                                                             userSpecificCallback,
//                                                             UA_ENTERING_ACKEDSTATE);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "adding entering acked state callback failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     userSpecificCallback = enteringConfirmedStateCallback;
//     retval = UA_Server_setConditionTwoStateVariableCallback(server, conditionInstance_1,
//                                                             conditionSource, false,
//                                                             userSpecificCallback,
//                                                             UA_ENTERING_CONFIRMEDSTATE);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "adding entering confirmed state callback failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     /* Unexposed condition 2. No user specific callbacks, so the server will
//      * behave in a standard manner upon entering enabled state, acknowledging
//      * and confirming. We will set Retain field to true and enable the condition
//      * so we can receive event notifications (we cannot call enable method on
//      * unexposed condition using a client like UaExpert or Softing). */
//     retval = addCondition_2(server);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "adding condition 2 failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     UA_Boolean retain = UA_TRUE;
//     UA_Server_writeObjectProperty_scalar(server, conditionInstance_2,
//                                          UA_QUALIFIEDNAME(0, "Retain"),
//                                          &retain, &UA_TYPES[UA_TYPES_BOOLEAN]);

//     UA_Variant value;
//     UA_Boolean enabledStateId = true;
//     UA_QualifiedName enabledStateField = UA_QUALIFIEDNAME(0,"EnabledState");
//     UA_QualifiedName enabledStateIdField = UA_QUALIFIEDNAME(0,"Id");
//     UA_Variant_setScalar(&value, &enabledStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     retval = UA_Server_setConditionVariableFieldProperty(server, conditionInstance_2,
//                                                          &value, enabledStateField,
//                                                          enabledStateIdField);

//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting EnabledState/Id Field failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }


//     /* Add 3 variables to trigger condition events */
//     addVariable_1_triggerAlarmOfCondition_1(server, &variable_1);

//     callback.onWrite = afterWriteCallbackVariable_1;
//     retval = UA_Server_setVariableNode_valueCallback(server, variable_1, callback);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting variable 1 Callback failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     /* Severity can change internally also when the condition disabled and
//      * retain is false. However, in this case no events will be generated. */
//     addVariable_2_changeSeverityOfCondition_2(server, &variable_2);

//     callback.onWrite = afterWriteCallbackVariable_2;
//     retval = UA_Server_setVariableNode_valueCallback(server, variable_2, callback);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting variable 2 Callback failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     addVariable_3_returnCondition_1_toNormalState(server, &variable_3);

//     callback.onWrite = afterWriteCallbackVariable_3;
//     retval = UA_Server_setVariableNode_valueCallback(server, variable_3, callback);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting variable 3 Callback failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//     }

//     return retval;
// }


// // Somewhere in your setup, e.g., in setUpEnvironment or a dedicated function
// static UA_NodeId valvePositionNodeId; // Global or accessible NodeId

// static UA_StatusCode
// addValvePositionVariable(UA_Server *server) {
//     UA_VariableAttributes attr = UA_VariableAttributes_default;
//     attr.displayName = UA_LOCALIZEDTEXT("en", "Valve Position");
//     attr.description = UA_LOCALIZEDTEXT("en", "Current position of the valve (0-100%)");
//     attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE; // Assuming it can be written too
//     UA_Float valveValue = 50.0f; // Initial value
//     UA_Variant_setScalar(&attr.value, &valveValue, &UA_TYPES[UA_TYPES_FLOAT]);
//     attr.dataType = UA_TYPES[UA_TYPES_FLOAT].typeId; // Explicitly set data type

//     UA_QualifiedName valveName = UA_QUALIFIEDNAME(0, "ValvePosition");
//     UA_NodeId parentNodeId = UA_NS0ID(OBJECTSFOLDER);
//     UA_NodeId parentReferenceNodeId = UA_NS0ID(ORGANIZES);
//     UA_NodeId variableTypeNodeId = UA_NS0ID(BASEDATAVARIABLETYPE);

//     UA_StatusCode retval = UA_Server_addVariableNode(server, UA_NODEID_NULL, parentNodeId,
//                                       parentReferenceNodeId, valveName,
//                                       variableTypeNodeId, attr, NULL, &valvePositionNodeId);

//     if (retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Failed to add ValvePosition variable. StatusCode %s",
//                      UA_StatusCode_name(retval));
//     }
//     return retval;
// }



// static UA_NodeId valvePositionAlarmInstance; // Global NodeId for our valve alarm

// static UA_StatusCode
// addValvePositionAlarm(UA_Server *server, UA_NodeId eventNotifierSource) {
//     UA_StatusCode retval = UA_Server_createCondition(server, UA_NODEID_NULL, UA_NS0ID(OFFNORMALALARMTYPE),
//                                        UA_QUALIFIEDNAME(0, "ValvePositionAlarm"),
//                                        eventNotifierSource, // The source for this alarm's events
//                                        UA_NS0ID(HASCOMPONENT), // Or UA_NS0ID(HASALARM) if you want a specific reference type
//                                        &valvePositionAlarmInstance);

//     if (retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Failed to add ValvePositionAlarm. StatusCode %s",
//                      UA_StatusCode_name(retval));
//     }
//     return retval;
// }


// // Define your thresholds
// #define VALVE_HIGH_THRESHOLD 85.0f
// #define VALVE_LOW_THRESHOLD  15.0f

// static void
// valvePositionWriteCallback(UA_Server *server, const UA_NodeId *sessionId,
//                            void *sessionContext, const UA_NodeId *nodeId,
//                            void *nodeContext, const UA_NumericRange *range,
//                            const UA_DataValue *data) {
//     if (data->value.type->typeIndex != UA_TYPES_FLOAT) {
//         UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                        "ValvePosition value written is not a float. Ignoring.");
//         return;
//     }

//     UA_Float currentPosition = *(UA_Float *)(data->value.data);

//     // Get current ActiveState of the alarm
//     UA_Variant activeStateVariant;
//     UA_StatusCode readRetval = UA_Server_readObjectProperty(server, valvePositionAlarmInstance,
//                                                             UA_QUALIFIEDNAME(0, "ActiveState"), &activeStateVariant);
//     UA_Boolean isActive = false;
//     if (readRetval == UA_STATUSCODE_GOOD && activeStateVariant.type->typeIndex == UA_TYPES_BOOLEAN) {
//         isActive = *(UA_Boolean*)activeStateVariant.data;
//     } else {
//         UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                        "Could not read ActiveState of ValvePositionAlarm. Assuming inactive.");
//     }
//     UA_Variant_clear(&activeStateVariant); // Clear the variant after use

//     UA_StatusCode retval = UA_STATUSCODE_GOOD;
//     UA_Variant value; // For setting condition fields

//     // Check for high alarm
//     if (currentPosition > VALVE_HIGH_THRESHOLD) {
//         if (!isActive) { // Alarm just went active
//             UA_Boolean activeStateId = true;
//             UA_Variant_setScalar(&value, &activeStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//             retval |= UA_Server_setConditionVariableFieldProperty(server, valvePositionAlarmInstance,
//                                                                   &value, UA_QUALIFIEDNAME(0, "ActiveState"),
//                                                                   UA_QUALIFIEDNAME(0, "Id"));

//             UA_LocalizedText message = UA_LOCALIZEDTEXT("en", "Valve Position High!");
//             UA_Variant_setScalar(&value, &message, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
//             retval |= UA_Server_setConditionField(server, valvePositionAlarmInstance,
//                                                   &value, UA_QUALIFIEDNAME(0, "Message"));

//             UA_UInt16 severity = 800; // High severity
//             UA_Variant_setScalar(&value, &severity, &UA_TYPES[UA_TYPES_UINT16]);
//             retval |= UA_Server_setConditionField(server, valvePositionAlarmInstance,
//                                                   &value, UA_QUALIFIEDNAME(0, "Severity"));

//             if (retval == UA_STATUSCODE_GOOD) {
//                 UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Valve Position HIGH Alarm Triggered (%.2f)", currentPosition);
//             }
//         }
//         // If already active, no need to re-trigger, just ensure fields are correct
//     }
//     // Check for low alarm
//     else if (currentPosition < VALVE_LOW_THRESHOLD) {
//         if (!isActive) { // Alarm just went active
//             UA_Boolean activeStateId = true;
//             UA_Variant_setScalar(&value, &activeStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//             retval |= UA_Server_setConditionVariableFieldProperty(server, valvePositionAlarmInstance,
//                                                                   &value, UA_QUALIFIEDNAME(0, "ActiveState"),
//                                                                   UA_QUALIFIEDNAME(0, "Id"));

//             UA_LocalizedText message = UA_LOCALIZEDTEXT("en", "Valve Position Low!");
//             UA_Variant_setScalar(&value, &message, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
//             retval |= UA_Server_setConditionField(server, valvePositionAlarmInstance,
//                                                   &value, UA_QUALIFIEDNAME(0, "Message"));

//             UA_UInt16 severity = 700; // Medium-high severity
//             UA_Variant_setScalar(&value, &severity, &UA_TYPES[UA_TYPES_UINT16]);
//             retval |= UA_Server_setConditionField(server, valvePositionAlarmInstance,
//                                                   &value, UA_QUALIFIEDNAME(0, "Severity"));

//             if (retval == UA_STATUSCODE_GOOD) {
//                  UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Valve Position LOW Alarm Triggered (%.2f)", currentPosition);
//             }
//         }
//         // If already active, no need to re-trigger
//     }
//     // Return to Normal
//     else {
//         if (isActive) { // Alarm just went inactive
//             UA_Boolean activeStateId = false;
//             UA_Variant_setScalar(&value, &activeStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//             retval |= UA_Server_setConditionVariableFieldProperty(server, valvePositionAlarmInstance,
//                                                                   &value, UA_QUALIFIEDNAME(0, "ActiveState"),
//                                                                   UA_QUALIFIEDNAME(0, "Id"));

//             UA_LocalizedText message = UA_LOCALIZEDTEXT("en", "Valve Position Normal.");
//             UA_Variant_setScalar(&value, &message, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
//             retval |= UA_Server_setConditionField(server, valvePositionAlarmInstance,
//                                                   &value, UA_QUALIFIEDNAME(0, "Message"));

//             UA_UInt16 severity = 100; // Normal severity
//             UA_Variant_setScalar(&value, &severity, &UA_TYPES[UA_TYPES_UINT16]);
//             retval |= UA_Server_setConditionField(server, valvePositionAlarmInstance,
//                                                   &value, UA_QUALIFIEDNAME(0, "Severity"));

//             // When an alarm goes inactive, you usually manually trigger the event
//             // to notify clients that the condition is no longer present.
//             retval |= UA_Server_triggerConditionEvent(server, valvePositionAlarmInstance,
//                                                       valvePositionNodeId, // Or your ConditionSourceObject if that's the Notifier
//                                                       NULL);

//             if (retval == UA_STATUSCODE_GOOD) {
//                 UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Valve Position Alarm Returned to Normal (%.2f)", currentPosition);
//             }
//         }
//     }
// }


// static UA_StatusCode
// setUpEnvironment(UA_Server *server) {
//     UA_StatusCode retval = UA_STATUSCODE_GOOD;

//     // 1. Add your valve position variable
//     retval = addValvePositionVariable(server);
//     if (retval != UA_STATUSCODE_GOOD) return retval;

//     // 2. Add an Event Notifier source for the valve alarm (reusing existing one or creating new)
//     // For simplicity, let's assume we reuse the 'conditionSource' from the original example
//     // If you need a distinct source for valve alarms, create another one like addConditionSourceObject
//     // For this example, let's just make sure conditionSource is initialized if not already
//     // UA_NodeId valveAlarmEventNotifierSource = conditionSource; // Reuse the existing conditionSource
//     // If conditionSource isn't already created by addCondition_1, you'd need to create it here:
//     // retval = addConditionSourceObject(server); // Assuming this creates `conditionSource` globally
//     // if (retval != UA_STATUSCODE_GOOD) return retval;
//     // For this example, let's just use the SERVER object as the event notifier for simplicity
//     UA_NodeId valveAlarmEventNotifierSource = UA_NS0ID(SERVER);


//     // 3. Create the alarm condition instance
//     retval = addValvePositionAlarm(server, valveAlarmEventNotifierSource);
//     if (retval != UA_STATUSCODE_GOOD) return retval;

//     // 4. Attach the write callback to the Valve Position variable
//     UA_ValueCallback valveCallback;
//     valveCallback.onRead = NULL;
//     valveCallback.onWrite = valvePositionWriteCallback;
//     retval = UA_Server_setVariableNode_valueCallback(server, valvePositionNodeId, valveCallback);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting Valve Position variable Callback failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }

//     // Optional: Set Retain to true for the valve alarm so it's visible on subscription
//     // until acknowledged/confirmed or explicitly reset
//     UA_Boolean retain = UA_TRUE;
//     UA_Server_writeObjectProperty_scalar(server, valvePositionAlarmInstance,
//                                          UA_QUALIFIEDNAME(0, "Retain"),
//                                          &retain, &UA_TYPES[UA_TYPES_BOOLEAN]);

//     // Optional: Enable the alarm so it can generate events
//     // This is especially important if it's not exposed in the address space for client to enable
//     UA_Variant value;
//     UA_Boolean enabledStateId = true;
//     UA_QualifiedName enabledStateField = UA_QUALIFIEDNAME(0,"EnabledState");
//     UA_QualifiedName enabledStateIdField = UA_QUALIFIEDNAME(0,"Id");
//     UA_Variant_setScalar(&value, &enabledStateId, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     retval = UA_Server_setConditionVariableFieldProperty(server, valvePositionAlarmInstance,
//                                                          &value, enabledStateField,
//                                                          enabledStateIdField);
//     if(retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Setting EnabledState/Id Field for Valve Alarm failed. StatusCode %s",
//                      UA_StatusCode_name(retval));
//         return retval;
//     }


//     // Add other environment setup from original example if needed
//     // ... (e.g., addCondition_1, addCondition_2, other variables and callbacks)

//     return retval;
// }

// // Ensure you call setUpEnvironment(server); in main



// /**
//  * It follows the main server code, making use of the above definitions. */

// // int main (void) {
// //     UA_Server *server = UA_Server_new();

// //     setUpEnvironment(server);

// //     UA_Server_runUntilInterrupt(server); 
// //     UA_Server_delete(server);
// //     return 0;
// // }


// #include <open62541/plugin/log_stdout.h>
// #include <open62541/server.h>
// #include <open62541/server_pubsub.h> // For PubSub and more complex setups, not strictly for this example

// #include <string.h> // For strcmp
// #include <stdbool.h> // For bool

// // Global array to store information about each process node and its alarm
// typedef struct {
//     UA_NodeId processNodeId;
//     UA_NodeId alarmInstanceId;
//     UA_Float hiHiThreshold;
//     UA_Float hiThreshold;
//     UA_Float loThreshold;
//     UA_Float loLoThreshold;
//     UA_Float deadband; // Optional: for hysteresis
// } ProcessNodeAlarmInfo;

// #define NUM_PROCESS_NODES 100
// ProcessNodeAlarmInfo processAlarms[NUM_PROCESS_NODES];
// int currentProcessNodeCount = 0;

// // Forward declarations
// static UA_StatusCode addProcessNodeWithAlarmProperties(UA_Server *server, int index);
// static UA_StatusCode createAndLinkExclusiveLimitAlarm(UA_Server *server,
//                                                       const UA_NodeId *processNodeId,
//                                                       const UA_String *processNodeDisplayName,
//                                                       UA_NodeId *outAlarmInstanceId);
// static void processNodeValueChangeCallback(UA_Server *server, const UA_NodeId *sessionId,
//                                            void *sessionContext, const UA_NodeId *nodeId,
//                                            void *nodeContext, const UA_NumericRange *range,
//                                            const UA_DataValue *data);


// /* --------------------- Alarm Threshold Definitions (as child variables) --------------------- */
// static UA_StatusCode
// addAlarmThresholdProperty(UA_Server *server, const UA_NodeId *parentNodeId,
//                           const char *name, UA_Float defaultValue, UA_NodeId *outNodeId) {
//     UA_VariableAttributes attr = UA_VariableAttributes_default;
//     attr.displayName = UA_LOCALIZEDTEXT("en", name);
//     attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
//     UA_Variant_setScalar(&attr.value, &defaultValue, &UA_TYPES[UA_TYPES_FLOAT]);
//     attr.dataType = UA_TYPES[UA_TYPES_FLOAT].typeId;

//     UA_QualifiedName qName = UA_QUALIFIEDNAME(0, name);
//     // Add as a component of the parentNodeId
//     return UA_Server_addVariableNode(server, UA_NODEID_NULL, *parentNodeId,
//                                      UA_NS0ID(HASCOMPONENT), qName,
//                                      UA_NS0ID(BASEDATAVARIABLETYPE), attr, NULL, outNodeId);
// }

// /* --------------------- Process Node Creation (e.g., Motor1_Speed) --------------------- */
// static UA_StatusCode
// addProcessNodeWithAlarmProperties(UA_Server *server, int index) {
//     char displayNameBuffer[64];
//     char tagNameBuffer[64];
//     snprintf(displayNameBuffer, sizeof(displayNameBuffer), "ProcessValue_%d", index);
//     snprintf(tagNameBuffer, sizeof(tagNameBuffer), "PV_%d", index);

//     // 1. Create the main process variable (e.g., Motor1_Speed)
//     UA_VariableAttributes processVarAttr = UA_VariableAttributes_default;
//     processVarAttr.displayName = UA_LOCALIZEDTEXT("en", displayNameBuffer);
//     processVarAttr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
//     UA_Float initialValue = 50.0f + (index % 10); // Some varied initial value
//     UA_Variant_setScalar(&processVarAttr.value, &initialValue, &UA_TYPES[UA_TYPES_FLOAT]);
//     processVarAttr.dataType = UA_TYPES[UA_TYPES_FLOAT].typeId;

//     UA_QualifiedName processVarName = UA_QUALIFIEDNAME(0, tagNameBuffer);
//     UA_NodeId processVarNodeId; // Will store the NodeId of this process variable

//     UA_StatusCode retval = UA_Server_addVariableNode(server, UA_NODEID_NULL, UA_NS0ID(OBJECTSFOLDER),
//                                       UA_NS0ID(ORGANIZES), processVarName,
//                                       UA_NS0ID(BASEDATAVARIABLETYPE), processVarAttr, NULL, &processVarNodeId);
//     if (retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to add process variable %s. StatusCode %s",
//                      displayNameBuffer, UA_StatusCode_name(retval));
//         return retval;
//     }

//     // Store the process node ID
//     processAlarms[currentProcessNodeCount].processNodeId = processVarNodeId;

//     // 2. Add alarm threshold properties as children of the process variable
//     // You could also read these from a configuration file or database
//     UA_Float hiHi = 90.0f;
//     UA_Float hi = 80.0f;
//     UA_Float lo = 20.0f;
//     UA_Float loLo = 10.0f;
//     UA_Float deadband = 2.0f; // Hysteresis

//     // Store thresholds directly in our lookup struct (for easy access in callback)
//     processAlarms[currentProcessNodeCount].hiHiThreshold = hiHi;
//     processAlarms[currentProcessNodeCount].hiThreshold = hi;
//     processAlarms[currentProcessNodeCount].loThreshold = lo;
//     processAlarms[currentProcessNodeCount].loLoThreshold = loLo;
//     processAlarms[currentProcessNodeCount].deadband = deadband;

//     // Optionally, add these as actual OPC UA variables for clients to read/write
//     // UA_NodeId hiHiNodeId, hiNodeId, loNodeId, loLoNodeId, deadbandNodeId;
//     // addAlarmThresholdProperty(server, &processVarNodeId, "AlarmHiHi", hiHi, &hiHiNodeId);
//     // addAlarmThresholdProperty(server, &processVarNodeId, "AlarmHi", hi, &hiNodeId);
//     // addAlarmThresholdProperty(server, &processNodeId, "AlarmLo", lo, &loNodeId);
//     // addAlarmThresholdProperty(server, &processNodeId, "AlarmLoLo", loLo, &loLoNodeId);
//     // addAlarmThresholdProperty(server, &processNodeId, "Deadband", deadband, &deadbandNodeId);

//     // 3. Create and link the ExclusiveLimitAlarm for this process variable
//     UA_String processNodeDisplayNameUA = UA_STRING(displayNameBuffer);
//     retval = createAndLinkExclusiveLimitAlarm(server, &processVarNodeId,
//                                               &processNodeDisplayNameUA,
//                                               &processAlarms[currentProcessNodeCount].alarmInstanceId);
//     if (retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Failed to create alarm for %s. StatusCode %s",
//                      displayNameBuffer, UA_StatusCode_name(retval));
//         return retval;
//     }

//     // 4. Attach the value change callback to the process variable
//     UA_ValueCallback callback;
//     callback.onRead = NULL; // No custom read callback
//     callback.onWrite = processNodeValueChangeCallback; // Our alarm logic on write
//     retval = UA_Server_setVariableNode_valueCallback(server, processVarNodeId, callback);
//     if (retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Failed to set value callback for %s. StatusCode %s",
//                      displayNameBuffer, UA_StatusCode_name(retval));
//         return retval;
//     }

//     currentProcessNodeCount++;
//     return UA_STATUSCODE_GOOD;
// }

// /* --------------------- ExclusiveLimitAlarm Creation --------------------- */
// static UA_StatusCode
// createAndLinkExclusiveLimitAlarm(UA_Server *server, const UA_NodeId *processNodeId,
//                                   const UA_String *processNodeDisplayName,
//                                   UA_NodeId *outAlarmInstanceId) {
//     char alarmNameBuffer[128];
//     snprintf(alarmNameBuffer, sizeof(alarmNameBuffer), "%s_ExclusiveLimitAlarm",
//              (char*)processNodeDisplayName->data);

//     // Create the alarm instance
//     UA_StatusCode retval = UA_Server_createCondition(server, UA_NODEID_NULL,
//                                                      UA_NS0ID(EXCLUSIVELIMITALARMTYPE),
//                                                      UA_QUALIFIEDNAME(0, alarmNameBuffer),
//                                                      *processNodeId, // The process node itself is the event notifier source
//                                                      UA_NS0ID(HASALARM), // Link with HasAlarm reference
//                                                      outAlarmInstanceId);
//     if (retval != UA_STATUSCODE_GOOD) {
//         UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                      "Failed to create ExclusiveLimitAlarm for %s. StatusCode %s",
//                      (char*)processNodeDisplayName->data, UA_StatusCode_name(retval));
//         return retval;
//     }

//     // Set Initial values for ExclusiveLimitAlarm fields (important for state machine)
//     // ActiveState = Inactive, AckedState = Acknowledged, ConfirmedState = Confirmed
//     UA_Variant value;
//     UA_Boolean initialBool = false;
//     UA_LocalizedText initialText = UA_LOCALIZEDTEXT("en", "Normal");
//     UA_UInt16 initialSeverity = 0; // Normal severity

//     // EnabledState (should be true for the alarm to work)
//     UA_Boolean enabledState = true;
//     UA_Variant_setScalar(&value, &enabledState, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     UA_Server_setConditionVariableFieldProperty(server, *outAlarmInstanceId, &value,
//                                                 UA_QUALIFIEDNAME(0, "EnabledState"), UA_QUALIFIEDNAME(0, "Id"));

//     // ActiveState (initially false)
//     UA_Variant_setScalar(&value, &initialBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     UA_Server_setConditionVariableFieldProperty(server, *outAlarmInstanceId, &value,
//                                                 UA_QUALIFIEDNAME(0, "ActiveState"), UA_QUALIFIEDNAME(0, "Id"));

//     // AcknowledgeState (initially true, as it's not active)
//     UA_Boolean ackedState = true;
//     UA_Variant_setScalar(&value, &ackedState, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     UA_Server_setConditionVariableFieldProperty(server, *outAlarmInstanceId, &value,
//                                                 UA_QUALIFIEDNAME(0, "AckedState"), UA_QUALIFIEDNAME(0, "Id"));

//     // ConfirmedState (initially true)
//     UA_Boolean confirmedState = true;
//     UA_Variant_setScalar(&value, &confirmedState, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     UA_Server_setConditionVariableFieldProperty(server, *outAlarmInstanceId, &value,
//                                                 UA_QUALIFIEDNAME(0, "ConfirmedState"), UA_QUALIFIEDNAME(0, "Id"));

//     // Retain (true so clients see it when subscribing later)
//     UA_Boolean retain = true;
//     UA_Variant_setScalar(&value, &retain, &UA_TYPES[UA_TYPES_BOOLEAN]);
//     UA_Server_setConditionField(server, *outAlarmInstanceId, &value, UA_QUALIFIEDNAME(0, "Retain"));

//     // Other initial fields
//     UA_Variant_setScalar(&value, &initialText, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
//     UA_Server_setConditionField(server, *outAlarmInstanceId, &value, UA_QUALIFIEDNAME(0, "Message"));
//     UA_Variant_setScalar(&value, &initialSeverity, &UA_TYPES[UA_TYPES_UINT16]);
//     UA_Server_setConditionField(server, *outAlarmInstanceId, &value, UA_QUALIFIEDNAME(0, "Severity"));

//     // Set the limits for ExclusiveLimitAlarm (these are specific properties of the alarm type)
//     // This is optional if you manage limits internally, but good for conformity
//     // UA_Server_writeObjectProperty_scalar(server, *outAlarmInstanceId,
//     //                                      UA_QUALIFIEDNAME(0, "HighHighLimit"),
//     //                                      &hiHi, &UA_TYPES[UA_TYPES_FLOAT]);
//     // ... similarly for HighLimit, LowLimit, LowLowLimit

//     UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Created ExclusiveLimitAlarm %s (NodeId: ns=%d;i=%d)",
//                 alarmNameBuffer, outAlarmInstanceId->namespaceIndex, outAlarmInstanceId->identifier.numeric);
//     return UA_STATUSCODE_GOOD;
// }

// /* --------------------- Value Change Callback for Process Nodes --------------------- */
// static void
// processNodeValueChangeCallback(UA_Server *server, const UA_NodeId *sessionId,
//                                void *sessionContext, const UA_NodeId *nodeId,
//                                void *nodeContext, const UA_NumericRange *range,
//                                const UA_DataValue *data) {
//     if (data->value.type->typeIndex != UA_TYPES_FLOAT) {
//         UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                        "Process node value written is not a float. Ignoring.");
//         return;
//     }

//     UA_Float currentValue = *(UA_Float *)(data->value.data);

//     // Find the alarm info for this nodeId
//     ProcessNodeAlarmInfo *info = NULL;
//     for (int i = 0; i < currentProcessNodeCount; ++i) {
//         if (UA_NodeId_equal(nodeId, &processAlarms[i].processNodeId)) {
//             info = &processAlarms[i];
//             break;
//         }
//     }

//     if (!info) {
//         UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
//                        "Could not find alarm info for process node (ns=%d;i=%d).",
//                        nodeId->namespaceIndex, nodeId->identifier.numeric);
//         return;
//     }

//     UA_NodeId alarmInstanceId = info->alarmInstanceId;
//     UA_Float hiHi = info->hiHiThreshold;
//     UA_Float hi = info->hiThreshold;
//     UA_Float lo = info->loThreshold;
//     UA_Float loLo = info->loLoThreshold;
//     UA_Float deadband = info->deadband;

//     // Get current state of the alarm (HighHighState, HighState, LowState, LowLowState)
//     // These are specific TwoStateVariables within ExclusiveLimitAlarmType
//     bool currentHiHiState = false;
//     bool currentHiState = false;
//     bool currentLoState = false;
//     bool currentLoLoState = false;

//     UA_Variant var;
//     UA_StatusCode readStatus;

//     readStatus = UA_Server_readObjectProperty(server, alarmInstanceId,
//                                               UA_QUALIFIEDNAME(0, "HighHighState"), &var);
//     if (readStatus == UA_STATUSCODE_GOOD && var.type->typeIndex == UA_TYPES_BOOLEAN) currentHiHiState = *(UA_Boolean*)var.data; UA_Variant_clear(&var);
//     readStatus = UA_Server_readObjectProperty(server, alarmInstanceId,
//                                               UA_QUALIFIEDNAME(0, "HighState"), &var);
//     if (readStatus == UA_STATUSCODE_GOOD && var.type->typeIndex == UA_TYPES_BOOLEAN) currentHiState = *(UA_Boolean*)var.data; UA_Variant_clear(&var);
//     readStatus = UA_Server_readObjectProperty(server, alarmInstanceId,
//                                               UA_QUALIFIEDNAME(0, "LowState"), &var);
//     if (readStatus == UA_STATUSCODE_GOOD && var.type->typeIndex == UA_TYPES_BOOLEAN) currentLoState = *(UA_Boolean*)var.data; UA_Variant_clear(&var);
//     readStatus = UA_Server_readObjectProperty(server, alarmInstanceId,
//                                               UA_QUALIFIEDNAME(0, "LowLowState"), &var);
//     if (readStatus == UA_STATUSCODE_GOOD && var.type->typeIndex == UA_TYPES_BOOLEAN) currentLoLoState = *(UA_Boolean*)var.data; UA_Variant_clear(&var);

//     UA_StatusCode setStatus = UA_STATUSCODE_GOOD;
//     UA_Variant val;
//     UA_Boolean stateBool;
//     UA_LocalizedText message;
//     UA_UInt16 severity;
//     UA_Boolean activeState = false; // Will become true if any limit is violated

//     // Logic for HighHigh Alarm
//     if (currentValue >= hiHi) {
//         if (!currentHiHiState) { // Transition to HighHigh
//             stateBool = true;
//             UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//             setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                      UA_QUALIFIEDNAME(0, "HighHighState"), UA_QUALIFIEDNAME(0, "Id"));
//             message = UA_LOCALIZEDTEXT("en", "Value EXCEEDS HighHigh Limit!");
//             severity = 900;
//             UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: HighHigh Alarm (%.2f >= %.2f)",
//                         (char*)UA_NodeId_print_to_string(nodeId), currentValue, hiHi);
//         }
//         activeState = true;
//     } else if (currentValue < (hiHi - deadband) && currentHiHiState) { // Return from HighHigh
//         stateBool = false;
//         UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//         setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                  UA_QUALIFIEDNAME(0, "HighHighState"), UA_QUALIFIEDNAME(0, "Id"));
//         // Don't set message/severity here, as other states might be active
//         // This transition will trigger an event due to TwoStateVariable change
//     }

//     // Logic for High Alarm (but not HighHigh)
//     if (currentValue >= hi && currentValue < hiHi) {
//         if (!currentHiState) { // Transition to High
//             stateBool = true;
//             UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//             setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                      UA_QUALIFIEDNAME(0, "HighState"), UA_QUALIFIEDNAME(0, "Id"));
//             message = UA_LOCALIZEDTEXT("en", "Value EXCEEDS High Limit!");
//             severity = 700;
//             UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: High Alarm (%.2f >= %.2f)",
//                         (char*)UA_NodeId_print_to_string(nodeId), currentValue, hi);
//         }
//         activeState = true;
//     } else if (currentValue < (hi - deadband) && currentHiState) { // Return from High
//         stateBool = false;
//         UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//         setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                  UA_QUALIFIEDNAME(0, "HighState"), UA_QUALIFIEDNAME(0, "Id"));
//     }

//     // Logic for LowLow Alarm
//     if (currentValue <= loLo) {
//         if (!currentLoLoState) { // Transition to LowLow
//             stateBool = true;
//             UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//             setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                      UA_QUALIFIEDNAME(0, "LowLowState"), UA_QUALIFIEDNAME(0, "Id"));
//             message = UA_LOCALIZEDTEXT("en", "Value FALLS BELOW LowLow Limit!");
//             severity = 900;
//             UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: LowLow Alarm (%.2f <= %.2f)",
//                         (char*)UA_NodeId_print_to_string(nodeId), currentValue, loLo);
//         }
//         activeState = true;
//     } else if (currentValue > (loLo + deadband) && currentLoLoState) { // Return from LowLow
//         stateBool = false;
//         UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//         setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                  UA_QUALIFIEDNAME(0, "LowLowState"), UA_QUALIFIEDNAME(0, "Id"));
//     }

//     // Logic for Low Alarm (but not LowLow)
//     if (currentValue <= lo && currentValue > loLo) {
//         if (!currentLoState) { // Transition to Low
//             stateBool = true;
//             UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//             setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                      UA_QUALIFIEDNAME(0, "LowState"), UA_QUALIFIEDNAME(0, "Id"));
//             message = UA_LOCALIZEDTEXT("en", "Value FALLS BELOW Low Limit!");
//             severity = 700;
//             UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: Low Alarm (%.2f <= %.2f)",
//                         (char*)UA_NodeId_print_to_string(nodeId), currentValue, lo);
//         }
//         activeState = true;
//     } else if (currentValue > (lo + deadband) && currentLoState) { // Return from Low
//         stateBool = false;
//         UA_Variant_setScalar(&val, &stateBool, &UA_TYPES[UA_TYPES_BOOLEAN]);
//         setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                  UA_QUALIFIEDNAME(0, "LowState"), UA_QUALIFIEDNAME(0, "Id"));
//     }

//     // Update the main ActiveState, Message, and Severity of the alarm instance
//     // The ExclusiveLimitAlarmType automatically manages the ActiveState based on its limit states
//     // However, if no limit is currently violated, and the alarm was previously active,
//     // we need to set it to inactive.
//     if (!activeState) { // If no limits are currently violated
//         UA_Variant currentActiveStateVariant;
//         UA_StatusCode currentActiveReadStatus = UA_Server_readObjectProperty(server, alarmInstanceId,
//                                                                               UA_QUALIFIEDNAME(0, "ActiveState"), &currentActiveStateVariant);
//         if (currentActiveReadStatus == UA_STATUSCODE_GOOD && currentActiveStateVariant.type->typeIndex == UA_TYPES_BOOLEAN &&
//             *(UA_Boolean*)currentActiveStateVariant.data == true) { // Was previously active, now should be inactive
            
//             UA_Boolean newActiveState = false;
//             UA_Variant_setScalar(&val, &newActiveState, &UA_TYPES[UA_TYPES_BOOLEAN]);
//             setStatus |= UA_Server_setConditionVariableFieldProperty(server, alarmInstanceId, &val,
//                                                                      UA_QUALIFIEDNAME(0, "ActiveState"), UA_QUALIFIEDNAME(0, "Id"));
//             message = UA_LOCALIZEDTEXT("en", "Value is within normal limits.");
//             severity = 100;
//              UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "%s: Alarm Cleared (%.2f)",
//                         (char*)UA_NodeId_print_to_string(nodeId), currentValue);
//         }
//         UA_Variant_clear(&currentActiveStateVariant);
//     }

//     // Ensure Message and Severity are set if state changed
//     if (setStatus == UA_STATUSCODE_GOOD) {
//         UA_Variant_setScalar(&val, &message, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT]);
//         UA_Server_setConditionField(server, alarmInstanceId, &val, UA_QUALIFIEDNAME(0, "Message"));

//         UA_Variant_setScalar(&val, &severity, &UA_TYPES[UA_TYPES_UINT16]);
//         UA_Server_setConditionField(server, alarmInstanceId, &val, UA_QUALIFIEDNAME(0, "Severity"));
//     }
// }


// /* --------------------- Setup Environment --------------------- */
// static UA_StatusCode
// setUpEnvironment(UA_Server *server) {
//     UA_StatusCode retval = UA_STATUSCODE_GOOD;

//     // Create 100 process nodes, each with an associated alarm
//     for (int i = 0; i < NUM_PROCESS_NODES; ++i) {
//         retval = addProcessNodeWithAlarmProperties(server, i);
//         if (retval != UA_STATUSCODE_GOOD) {
//             UA_LOG_ERROR(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND, "Failed to setup environment for node %d", i);
//             return retval;
//         }
//     }

//     // You might also add generic callbacks for acknowledge/confirm if you want
//     // them to apply to all alarms (optional for this example)

//     return retval;
// }

// /* --------------------- Main Server Loop --------------------- */
// int main (void) {
//     UA_Server *server = UA_Server_new();

//     UA_ServerConfig_setDefault(UA_Server_getConfig(server));

//     UA_StatusCode retval = setUpEnvironment(server);
//     if (retval != UA_STATUSCODE_GOOD) {
//         UA_Server_delete(server);
//         return EXIT_FAILURE;
//     }

//     UA_Server_runUntilInterrupt(server);
//     UA_Server_delete(server);
//     return EXIT_SUCCESS;
// }

// ======================================================








// #include <vector>
// #include <string>
// #include <future> // For std::async
// #include <nlohmann/json.hpp> // For nlohmann::json
// #include <map>
// #include <algorithm> // For std::remove_if
// #include <cstdint> // For uint32_t
// #include <stdio.h>

// #include <open62541/server.h>
// #include <open62541/plugin/log_stdout.h>
// #include <open62541/types.h>
// #include <open62541/server_config_default.h>
// #include <open62541/ua_variables.h>
// #include <open62541/types_generated.h>
// #include <open62541/plugin/historydatabase.h> // For historizing
// #include <open62541/plugin/historydatabase_memory.h> // For memory history backend

// // Assuming these are defined elsewhere in your project
// // You'll need these external functions/types
// extern json getTopicList(const std::string& bearerToken);
// extern std::vector<std::string> split(const std::string& s, char delimiter);
// extern UA_NodeId getOrCreateFolder(UA_Server *server, const std::string& path, const std::string& displayName, UA_NodeId parentNodeId);
// extern void mqtt_subscribe_and_update(UA_Server *server, const std::vector<std::string>& topics);

// // Global map to store node IDs for quick lookup (if needed by writeCallback)
// std::map<std::string, UA_NodeId> nodeMap;

// Global structure to hold alarm-related data for each monitored node
// struct MonitoredNodeAlarmInfo {
//     UA_NodeId processNodeId;
//     UA_NodeId alarmInstanceId;
//     double alarmHiHi;
//     double alarmHi;
//     double alarmLo;
//     double alarmLoLo;
//     double deadband; // Add deadband for hysteresis
//     std::string displayName; // To use in alarm messages
// };

// Use a map for easy lookup by processNodeId
// std::map<UA_NodeId, MonitoredNodeAlarmInfo, UA_NodeId_less_than> monitoredAlarms;

// // --- Forward Declarations ---
// static UA_StatusCode createAndLinkExclusiveLimitAlarm(UA_Server *server,
//                                                       const UA_NodeId *processNodeId,
//                                                       const std::string& displayName,
//                                                       const nlohmann::json& item,
//                                                       UA_NodeId *outAlarmInstanceId);

// static void writeCallback(UA_Server *server, const UA_NodeId *sessionId,
//                           void *sessionContext, const UA_NodeId *nodeId,
//                           void *nodeContext, const UA_NumericRange *range,
//                           const UA_DataValue *data);

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
static UA_StatusCode createAndLinkExclusiveLimitAlarm(UA_Server *server,
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

    return UA_STATUSCODE_GOOD;
}

