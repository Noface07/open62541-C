#ifndef OPC_UA_MONITOR_H
#define OPC_UA_MONITOR_H

#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/client_subscriptions.h>
#include <functional> // Added for std::function
#include "structs.h" // Assumed to contain the definition for MyMonitorContext

using TaskScheduler = std::function<void(std::function<void()>)>;


/**
 * @brief Sets up a monitored item for data changes on a specific OPC UA node.
 * * @param client The active UA_Client instance.
 * @param response The response from a successful subscription creation.
 * @param nodeIdStr The string representation of the NodeId to monitor (e.g., "ns=2;i=123").
 * @param tagID An integer identifier for the tag being monitored.
 * @param myContext A pointer to the custom context structure containing necessary handlers 
 * (like MQTThandler) and configuration info.

 * @param attempt The current retry attempt number (default 0).
 */
/**
 * @brief Data-change callback invoked by the OPC UA stack for each monitored item update.
 * Exported so batch-registration code in client.cpp can pass it as the per-item handler.
 */
void handler_NodeValueChanged(UA_Client *client, UA_UInt32 subId, void *subContext,
                              UA_UInt32 monId, void *monContext, UA_DataValue *value);

/**
 * @brief Sets up a monitored item for data changes on a specific OPC UA node.
 */
void MonitorItem(UA_Client *client, UA_CreateSubscriptionResponse response,
                 const char *nodeIdStr, int tagID, MyMonitorContext *myContext,
                 const char *overrideNamespaceUri,
                 TaskScheduler scheduler = nullptr, int attempt = 0);


/**
 * @brief Sets up a monitored item for events from the OPC UA server.
 * * @param client The active UA_Client instance.
 * @param response The response from a successful subscription creation.
 */
void MonitorEvent(UA_Client *client, UA_CreateSubscriptionResponse response);


#endif // OPC_UA_MONITOR_H