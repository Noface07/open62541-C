#ifndef OPC_UA_MONITOR_H
#define OPC_UA_MONITOR_H

#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <functional> // Added for std::function
#include "structs.h" // Assumed to contain the definition for MyMonitorContext

using TaskScheduler = std::function<void(std::function<void()>)>;

/**
 * @brief Data-change callback invoked by the OPC UA stack for each monitored item update.
 * Exported so batch-registration code in client.cpp can pass it as the per-item handler.
 *
 * `monContext` MUST point to a MyMonitorContext that is owned by a std::shared_ptr
 * (see structs.h) — the handler takes a shared reference into the task it posts.
 */
void handler_NodeValueChanged(UA_Client *client, UA_UInt32 subId, void *subContext,
                              UA_UInt32 monId, void *monContext, UA_DataValue *value);

/**
 * @brief Sets up a monitored item for events from the OPC UA server.
 * * @param client The active UA_Client instance.
 * @param response The response from a successful subscription creation.
 */
void MonitorEvent(UA_Client *client, UA_CreateSubscriptionResponse response);


#endif // OPC_UA_MONITOR_H
