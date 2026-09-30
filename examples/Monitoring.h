#ifndef OPC_UA_MONITOR_H
#define OPC_UA_MONITOR_H

#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <functional> // Added for std::function
#include "structs.h" // Assumed to contain the definition for MyMonitorContext

#include <chrono>
#include <cstdint>
#include <memory>

using TaskScheduler = std::function<void(std::function<void()>)>;

/** Where a value came from. Both origins publish the same payload; they only
 * differ in offline handling (see CLIENT_POLLING_PLAN.md). */
enum class PublishOrigin {
    Change, // subscription data-change notification
    Poll    // periodic read of a tag that was quiet for a full poll interval
};

/** Monotonic milliseconds, the clock used for MyMonitorContext::lastPublishMs. */
inline std::int64_t
steadyNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

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
 * @brief Copy `value` and post a worker task that decodes, transforms and
 * publishes it for `ctx`. Used by the subscription handler (Change) and the
 * poller (Poll).
 *
 * Poll results are dropped instead of buffered when MQTT is down, and are
 * skipped if the tag published within the last `pollStaleMs` (a change
 * arrived while the read was in flight). `pollStaleMs` is ignored for Change.
 */
void postValueForPublish(const std::shared_ptr<MyMonitorContext> &ctx,
                         const UA_DataValue *value, PublishOrigin origin,
                         std::int64_t pollStaleMs = 0);

/**
 * @brief Sets up a monitored item for events from the OPC UA server.
 * * @param client The active UA_Client instance.
 * @param response The response from a successful subscription creation.
 */
void MonitorEvent(UA_Client *client, UA_CreateSubscriptionResponse response);


#endif // OPC_UA_MONITOR_H
