#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/plugin/log_stdout.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <signal.h>
#include <stdlib.h>
#include <string>
#include <thread>
#include <time.h>

// Define NOMINMAX to prevent windows.h definitions of min/max macros if included by MQTThandler.h
#ifndef NOMINMAX
#define NOMINMAX
#endif

// Include local headers
#include "Logger.h"
#include "MQTThandler.h" // May include Windows.h
#include "SqliteQueueService.h"
#include "alarm_enums.h"
#include "structs.h" // Includes exprtk.hpp safely
#include "Monitoring.h" // PublishOrigin, steadyNowMs, postValueForPublish
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;

#ifdef UA_ENABLE_SUBSCRIPTIONS

/**
 * @brief Scales a value from one range to another.
 * @return The scaled value. Returns scaleMin if the raw range is zero to prevent division
 * by zero.
 */
double
ScaleValue(double rawValue, double rawMin, double rawMax, double scaleMin,
           double scaleMax) {
    if(rawMax - rawMin == 0) {
        return scaleMin;
    }
    double scaleValue =
        scaleMin + (rawValue - rawMin) * (scaleMax - scaleMin) / (rawMax - rawMin);
    return scaleValue;
}

// Removed EvaluateExpression - logic moved to pre-compiled context

// Decode, transform and publish one value for `myContext`. Runs on a worker
// thread; the caller owns `value`. Shared by subscription notifications and
// poll results (CLIENT_POLLING_PLAN.md P-04) - the payload is identical for
// both; only offline handling differs.
static void
processAndPublish(MyMonitorContext *myContext, UA_DataValue *value,
                  PublishOrigin origin, std::int64_t pollStaleMs) {
    const bool isPoll = (origin == PublishOrigin::Poll);
    if(isPoll) {
        // A change was published while the read was in flight: nothing to add.
        if(pollStaleMs > 0 &&
           steadyNowMs() - myContext->lastPublishMs.load(std::memory_order_relaxed) <
               pollStaleMs)
            return;
        // Poll results are never buffered offline (P-07).
        if(!myContext->mqttHandler || !myContext->mqttHandler->isConnected())
            return;
    }

    MqttPayload p;
    // p.datapointId = myContext->infoSpace.tagId; // Assuming tagId is int? Check struct
    // definition if needed.
    p.datapointId = myContext->infoSpace.tagId;
    p.name = myContext->infoSpace.name;
    p.tagId = myContext->infoSpace.tagId;  // Redundant?
    p.tagType = myContext->infoSpace.tagType;
    p.source = static_cast<int>(AlarmSource::OPC);
    p.infoId = 1001;                                         // TODO: parameterize
    p.UpdateType = static_cast<int>(UpdateType::Telemetry);  // Simple update by default

    // Derive quality from the OPC UA StatusCode delivered by the server.
    // The top 2 bits of the StatusCode encode the severity class:
    //   0x00xxxxxx = Good, 0x40xxxxxx = Uncertain, 0x80xxxxxx = Bad
    if(value->hasStatus && UA_StatusCode_isBad(value->status)) {
        p.quality = static_cast<int>(AlarmQuality::Bad);
    } else if(value->hasStatus && UA_StatusCode_isUncertain(value->status)) {
        p.quality = static_cast<int>(AlarmQuality::Unknown);
    } else {
        p.quality = static_cast<int>(AlarmQuality::Good);
    }

    // Get timestamp

    UA_Variant *variant = &value->value;
    std::optional<double> numericValue;
    bool valueIsFinal = false;  // Flag to check if p.value has been definitively set

    // Step 1: Extract value from UA_Variant and handle non-numeric types
    // Step 1: Extract value from UA_Variant and handle non-numeric types
    if(!variant->data) {
        p.value = nullptr;
        valueIsFinal = true;
    } else if(UA_Variant_isScalar(variant)) {
        if(variant->type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
            p.value = (*static_cast<UA_Boolean *>(variant->data));
            valueIsFinal = true;
        } else if(variant->type == &UA_TYPES[UA_TYPES_SBYTE]) {
            numericValue = static_cast<double>(*static_cast<UA_SByte *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_BYTE]) {
            numericValue = static_cast<double>(*static_cast<UA_Byte *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_INT16]) {
            numericValue = static_cast<double>(*static_cast<UA_Int16 *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_UINT16]) {
            numericValue = static_cast<double>(*static_cast<UA_UInt16 *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_INT32]) {
            numericValue = static_cast<double>(*static_cast<UA_Int32 *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_UINT32]) {
            numericValue = static_cast<double>(*static_cast<UA_UInt32 *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_INT64]) {
            numericValue = static_cast<double>(*static_cast<UA_Int64 *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_UINT64]) {
            numericValue = static_cast<double>(*static_cast<UA_UInt64 *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_FLOAT]) {
            numericValue = static_cast<double>(*static_cast<UA_Float *>(variant->data));
        } else if(variant->type == &UA_TYPES[UA_TYPES_DOUBLE]) {
            numericValue = *static_cast<UA_Double *>(variant->data);
        } else if(variant->type == &UA_TYPES[UA_TYPES_STRING]) {
            UA_String str = *static_cast<UA_String *>(variant->data);
            p.value = (str.length > 0 && str.data)
                          ? std::string(reinterpret_cast<char *>(str.data), str.length)
                          : "";
            valueIsFinal = true;
        } else if(variant->type == &UA_TYPES[UA_TYPES_DATETIME]) {
            UA_DateTime dt = *static_cast<UA_DateTime *>(variant->data);
            p.value = std::to_string(dt);
            valueIsFinal = true;
        } else {
            p.value = "scalar_unsupported_type";
            valueIsFinal = true;
        }
    } else if(UA_Variant_isArray(variant)) {
        // Handle Arrays
        std::vector<nlohmann::json> jsonArray;
        size_t len = variant->arrayLength;
        jsonArray.reserve(len);

        if(variant->type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
            UA_Boolean *arr = static_cast<UA_Boolean *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_SBYTE]) {
            UA_SByte *arr = static_cast<UA_SByte *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_BYTE]) {
            UA_Byte *arr = static_cast<UA_Byte *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_INT16]) {
            UA_Int16 *arr = static_cast<UA_Int16 *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_UINT16]) {
            UA_UInt16 *arr = static_cast<UA_UInt16 *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_INT32]) {
            UA_Int32 *arr = static_cast<UA_Int32 *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_UINT32]) {
            UA_UInt32 *arr = static_cast<UA_UInt32 *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_INT64]) {
            UA_Int64 *arr = static_cast<UA_Int64 *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_UINT64]) {
            UA_UInt64 *arr = static_cast<UA_UInt64 *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_FLOAT]) {
            UA_Float *arr = static_cast<UA_Float *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_DOUBLE]) {
            UA_Double *arr = static_cast<UA_Double *>(variant->data);
            for(size_t i = 0; i < len; i++)
                jsonArray.push_back(arr[i]);
        } else if(variant->type == &UA_TYPES[UA_TYPES_STRING]) {
            UA_String *arr = static_cast<UA_String *>(variant->data);
            for(size_t i = 0; i < len; i++) {
                UA_String str = arr[i];
                jsonArray.push_back((str.length > 0 && str.data)
                                        ? std::string((char *)str.data, str.length)
                                        : "");
            }
        } else {
            // Fallback for unsupported arrays
            // jsonArray.push_back("array_unsupported_type");
        }

        p.value = jsonArray;
        valueIsFinal = true;
    }

    // Step 2: If we have a numeric value, apply transformations
    if(numericValue.has_value() && !valueIsFinal) {
        double processedValue = numericValue.value();

        // Check for INFO_STATE first, as it's a special case that results in a boolean
        // string
        if(p.tagType == "INFO_STATE") {
            if(myContext->exprContext) {
                // HOT PATH with Pre-Compiled Expression
                // Set the bound variable and evaluate as one critical section
                // (P-08: two updates for this tag may run on different workers).
                double result;
                {
                    std::lock_guard<std::mutex> lk(myContext->exprContext->evalMutex);
                    myContext->exprContext->value = processedValue;
                    result = myContext->exprContext->expression.value();
                }
                p.value = (result != 0.0);
            } else {
                // If expression is disabled or failed to compile, just use the original
                // numeric value
                p.value = processedValue;
            }
        } else if(p.tagType == "INFO_INST") {
            if(myContext->infoSpace.scaling) {
                processedValue =
                    ScaleValue(processedValue, myContext->infoSpace.rawMin,
                               myContext->infoSpace.rawMax, myContext->infoSpace.scaleMin,
                               myContext->infoSpace.scaleMax);
            }
            p.value = processedValue;
        } else if(p.tagType == "INFO_INC") {
            // Handle counter logic here if needed
            p.value = processedValue;
        } else if(p.tagType == "INFO_DCR") {
            // Handle decrement logic here if needed
            p.value = processedValue;
        } else if(p.tagType == "DT_TYP_TXT") {
            // Handle text data type logic here if needed
            // Using to_string is inevitable for text representation of number
            p.value = std::to_string(processedValue);
        } else {
            // Default case - no transformation needed
            p.value = processedValue;
        }
    }

    //// Timestamp - Optimized
    //UA_DateTime ts =
    //    value->hasSourceTimestamp
    //        ? value->sourceTimestamp
    //        : (value->hasServerTimestamp ? value->serverTimestamp : UA_DateTime_now());
    //UA_DateTimeStruct dts = UA_DateTime_toStruct(ts);

    //// Reuse stack buffer, single snprintf call
    //char buffer[32];  // 64 was overkill for 24 chars
    //// Faster formatting than snprintf? standard guarantees are safer.
    //// If ultra low latency needed, manual char filling is faster, but snprintf is robust.
    //snprintf(buffer, sizeof(buffer), "%04u-%02u-%02uT%02u:%02u:%02u.%03uZ", dts.year,
    //         dts.month, dts.day, dts.hour, dts.min, dts.sec, dts.milliSec);

    // 1. Timestamp Selection (Keep your existing logic)
    UA_DateTime ts =
        value->hasSourceTimestamp
            ? value->sourceTimestamp
            : (value->hasServerTimestamp ? value->serverTimestamp : UA_DateTime_now());

    // 2. Convert UA_DateTime (100ns ticks since 1601) to Unix Time (seconds since 1970)
    // UA_DATETIME_UNIX_EPOCH is usually defined in open62541.h,
    // if not, the magic number is 11644473600LL * 10000000LL
    long long raw_ticks = (long long)ts;
    time_t raw_time = (time_t)((raw_ticks - 116444736000000000LL) / 10000000LL);

    // 3. Extract Milliseconds (lost in conversion to time_t)
    int msec = (int)((raw_ticks % 10000000LL) / 10000LL);

    // 4. Convert to System Local Time
    struct tm t_local;
#ifdef _WIN32
    localtime_s(&t_local, &raw_time);  // Windows thread-safe
#else
    localtime_r(&raw_time, &t_local);  // POSIX thread-safe
#endif

    // 5. Get Time Zone Offset (Standard %z gives +0530, we need +05:30)
    char tz_buf[8];
    strftime(tz_buf, sizeof(tz_buf), "%z", &t_local);  // Output example: "+0530"

    // 6. Format Final String
    // We increase buffer slightly to 34 to be safe for all offsets.
    char buffer[34];

    // Trick: We print the timezone buffer in two parts (%.3s and %.2s) to inject the
    // colon
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d.%03d%.3s:%.2s",
             t_local.tm_year + 1900, t_local.tm_mon + 1, t_local.tm_mday, t_local.tm_hour,
             t_local.tm_min, t_local.tm_sec, msec,
             tz_buf,       // Prints first 3 chars (e.g., "+05")
             tz_buf + 3);  // Prints next 2 chars (e.g., "30")

    p.timeStamp = buffer;  // Allocation here is hard to avoid without changing
                           // MqttPayload to use char array

    const std::string topic = myContext->infoSpace.namespaces;
    long orgId = myContext->infoSpace.orgId > 0 ? (long)myContext->infoSpace.orgId : 1;

    json data = {{"TagId", p.tagId},
                 {"Value", p.value},
                 {"TagType", p.tagType},
                 {"TimeStamp", p.timeStamp},
                 {"Source", p.source},
                 {"DatapointId", p.datapointId},
                 {"InfoId", p.infoId},
                 {"Quality", p.quality},
                 {"UpdateType", static_cast<int>(UpdateType::Telemetry)}};

    json payload;
    payload["Data"] = json::array({data});

    /* MQTT down: persist directly to SQLite (same path as offline queue / bulk API). */
    if(myContext->mqttHandler && !myContext->mqttHandler->isConnected() &&
       myContext->sqliteService) {
        if(isPoll)
            return; // never buffered offline (P-07)
        myContext->sqliteService->EnqueueMessage(topic, p, orgId);
    } else if(myContext->asyncPublisher) {
        // Poll messages are dropped rather than spilled if MQTT drops before
        // the publisher sends them.
        myContext->asyncPublisher->enqueue(topic, payload.dump(), /*spillIfDown*/ !isPoll);
    } else if(myContext->mqttHandler && myContext->mqttHandler->isConnected()) {
        // Construct JSON manually or use library - library adds overhead but ensures
        // correctness. Assuming nlohmann::json is efficient enough. Improvement: Reuse a
        // thread-local json object to avoid repeated allocations? For now, struct
        // MqttPayload abstraction creates copies.

        // Log is potentially slow - verify log level efficiently before constructing
        // strings log(...) usually checks level inside, but string construction happens
        // before call.
        // TODO: Add macro based logging to avoid string construction if level not met.
        // Keeping existing log logic for safety.

        json dataSync = {{"TagId", p.tagId},     {"Value", p.value},
                         {"TagType", p.tagType}, {"TimeStamp", p.timeStamp},
                         {"Source", p.source},   {"DatapointId", p.datapointId},
                         {"InfoId", p.infoId},   {"Quality", p.quality},
                         {"UpdateType", 1}};

        json payloadSync;
        payloadSync["Data"] = json::array({dataSync});

        myContext->mqttHandler->publish(topic, payloadSync.dump());

    } else if(myContext->sqliteService) {
        if(isPoll)
            return; // never buffered offline (P-07)
        myContext->sqliteService->EnqueueMessage(topic, p, orgId);
    } else {
        return; // nowhere to send it
    }

    // Published or queued: the poller leaves this tag alone for an interval.
    myContext->lastPublishMs.store(steadyNowMs(), std::memory_order_relaxed);
}

void
postValueForPublish(const std::shared_ptr<MyMonitorContext> &ctx,
                    const UA_DataValue *value, PublishOrigin origin,
                    std::int64_t pollStaleMs) {
    if(!ctx || !ctx->postWorkerTask || !value)
        return;

    UA_DataValue *copiedValue = UA_DataValue_new();
    if(!copiedValue)
        return;
    UA_DataValue_copy(value, copiedValue);

    // The task holds its own reference, so the context outlives the task even
    // if the registry entry is released (reconnect / hot-reload delete) first.
    std::shared_ptr<MyMonitorContext> keepAlive = ctx;
    ctx->postWorkerTask([keepAlive, copiedValue, origin, pollStaleMs]() {
        // Cleanup struct ensures memory is freed even if exceptions occur
        struct ValueCleanup {
            UA_DataValue *ptr;
            ~ValueCleanup() {
                UA_DataValue_clear(ptr);
                UA_DataValue_delete(ptr);
            }
        } cleanup{copiedValue};

        processAndPublish(keepAlive.get(), copiedValue, origin, pollStaleMs);
    });
}

void
handler_NodeValueChanged(UA_Client *client, UA_UInt32 subId, void *subContext,
                         UA_UInt32 monId, void *monContext, UA_DataValue *value) {
    auto *myContext = static_cast<MyMonitorContext *>(monContext);
    if(!myContext) {
        log("Monitoring context is null!", LogLevel::ERRORS);
        return;
    }

    if (!myContext->postWorkerTask) {
        log("postWorkerTask is not configured!", LogLevel::ERRORS);
        return;
    }

    // Keep the context alive for as long as this task exists, even if the
    // owning registry entry is released (reconnect / hot-reload delete) before
    // a worker gets to run it. Contexts are always make_shared'd (see
    // structs.h). weak_from_this() never throws (unlike shared_from_this()),
    // which matters because this is a C callback boundary.
    std::shared_ptr<MyMonitorContext> keepAlive = myContext->weak_from_this().lock();
    if(!keepAlive) {
        log("Monitoring context is not shared-owned; skipping notification for tagId " +
                std::to_string(myContext->infoSpace.tagId),
            LogLevel::ERRORS);
        return;
    }

    postValueForPublish(keepAlive, value, PublishOrigin::Change);
}

#endif

// NOTE: The legacy single-item MonitorItem()/parseNodeId()/createMonitoredItemRequest()
// path was removed (C-33). It had no callers (client.cpp uses the batched
// UA_Client_MonitoredItems_createDataChanges path), spawned detached retry
// threads holding a raw UA_Client*, and built UA_NODEID_STRING from a
// non-NUL-terminated UA_String.
#include "Monitoring.h"

void
handler_Event(UA_Client *client, UA_UInt32 subId, void *subContext, UA_UInt32 monId,
              void *monContext, const UA_KeyValueMap eventFields) {
    log("========== EVENT NOTIFICATION ==========");
    log("Fields count: " + std::to_string(eventFields.mapSize));

    for(size_t i = 0; i < eventFields.mapSize; ++i) {
        UA_KeyValuePair *pair = &eventFields.map[i];
        UA_Variant *value = &pair->value;

        // ---- Field name ----
        std::string fieldName((char *)pair->key.name.data, pair->key.name.length);

        log("Field: " + fieldName);

        // ---- Empty value ----
        if(!value || !value->data) {
            log("  Value: <null>");
            continue;
        }

        // ---- UINT16 / Severity ----
        if(UA_Variant_hasScalarType(value, &UA_TYPES[UA_TYPES_UINT16])) {
            log("  Type: UInt16");
            log("  Value: " + std::to_string(*(UA_UInt16 *)value->data));
        }

        // ---- BOOLEAN ----
        else if(UA_Variant_hasScalarType(value, &UA_TYPES[UA_TYPES_BOOLEAN])) {
            log("  Type: Boolean");
            log(std::string("  Value: ") +
                (*(UA_Boolean *)value->data ? "true" : "false"));
        }

        // ---- STRING ----
        else if(UA_Variant_hasScalarType(value, &UA_TYPES[UA_TYPES_STRING])) {
            UA_String *s = (UA_String *)value->data;
            log("  Type: String");
            log("  Value: " + std::string((char *)s->data, s->length));
        }

        // ---- LOCALIZED TEXT ----
        else if(UA_Variant_hasScalarType(value, &UA_TYPES[UA_TYPES_LOCALIZEDTEXT])) {
            UA_LocalizedText *lt = (UA_LocalizedText *)value->data;
            log("  Type: LocalizedText");
            log("  Value: " + std::string((char *)lt->text.data, lt->text.length));
        }

        // ---- DATETIME ----
        else if(UA_Variant_hasScalarType(value, &UA_TYPES[UA_TYPES_DATETIME])) {
            UA_DateTime dt = *(UA_DateTime *)value->data;
            log("  Type: DateTime");

            if(dt == 0) {
                log("  Value: <not set>");
            } else {
                UA_Int64 unixTime = UA_DateTime_toUnixTime(dt);
                std::time_t t = (std::time_t)unixTime;

                char buf[64];
                std::tm tm{};
#ifdef _WIN32
                localtime_s(&tm, &t);
#else
                localtime_r(&t, &tm);
#endif
                std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);
                log("  Value: " + std::string(buf));
            }
        }

        // ---- NODEID ----
        else if(UA_Variant_hasScalarType(value, &UA_TYPES[UA_TYPES_NODEID])) {
            UA_NodeId *nid = (UA_NodeId *)value->data;
            log("  Type: NodeId");

            std::string nidStr = "ns=" + std::to_string(nid->namespaceIndex) + ";";
            switch(nid->identifierType) {
                case UA_NODEIDTYPE_NUMERIC:
                    nidStr += "i=" + std::to_string(nid->identifier.numeric);
                    break;
                case UA_NODEIDTYPE_STRING:
                    nidStr += "s=" + std::string((char *)nid->identifier.string.data,
                                                 nid->identifier.string.length);
                    break;
                case UA_NODEIDTYPE_GUID:
                    nidStr += "g=<GUID>";
                    break;
                case UA_NODEIDTYPE_BYTESTRING:
                    nidStr += "b=<ByteString>";
                    break;
            }
            log("  Value: " + nidStr);
        }

        // ---- BYTESTRING (EventId) ----
        else if(UA_Variant_hasScalarType(value, &UA_TYPES[UA_TYPES_BYTESTRING])) {
            UA_ByteString *bs = (UA_ByteString *)value->data;
            log("  Type: ByteString");

            std::ostringstream oss;
            for(size_t j = 0; j < bs->length; ++j)
                oss << std::hex << std::setw(2) << std::setfill('0') << (int)bs->data[j];

            log("  Value (hex): " + oss.str());
        }

        // ---- STATUS CODE ----
        else if(UA_Variant_hasScalarType(value, &UA_TYPES[UA_TYPES_STATUSCODE])) {
            log("  Type: StatusCode");
            log("  Value: " + std::to_string(*(UA_StatusCode *)value->data));
        }

        // ---- ARRAY ----
        else if(UA_Variant_isArray(value)) {
            log("  Type: Array");
            log("  Length: " + std::to_string(value->arrayLength));
        }

        // ---- UNKNOWN ----
        else {
            log("  Type: Unknown / Unsupported");
        }

        log("--------------------------------------");
    }

    log("========== END EVENT ==========\n");
}

void
MonitorEvent(UA_Client *client, UA_CreateSubscriptionResponse response) {

    UA_Byte eventNotifier = 0;
    UA_StatusCode sc = UA_Client_readEventNotifierAttribute(
        client, UA_NODEID_NUMERIC(0, 2253), &eventNotifier);
    if(sc == UA_STATUSCODE_GOOD)
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                    "Server.EventNotifier: 0x%02x", eventNotifier);
    else
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                       "Could not read EventNotifier attribute");

    /* Add a MonitoredItem */
    UA_MonitoredItemCreateRequest item;
    UA_MonitoredItemCreateRequest_init(&item);
    item.itemToMonitor.nodeId = UA_NODEID_NUMERIC(0, 2253);  // Root->Objects->Server
    item.itemToMonitor.attributeId = UA_ATTRIBUTEID_EVENTNOTIFIER;
    item.monitoringMode = UA_MONITORINGMODE_REPORTING;

    UA_EventFilter filter;
    UA_EventFilter_init(&filter);

    // Setup select clauses for Message and Severity (like in tutorial)
    const size_t nSelectClauses = 5;
    filter.selectClauses = (UA_SimpleAttributeOperand *)UA_Array_new(
        nSelectClauses, &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
    if(!filter.selectClauses) {
        log("Failed to allocate select clauses", LogLevel::ERRORS);
        return;
    }

    for(size_t i = 0; i < nSelectClauses; ++i) {
        UA_SimpleAttributeOperand_init(&filter.selectClauses[i]);
    }

    // Message field
    filter.selectClauses[0].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[0].browsePathSize = 1;
    filter.selectClauses[0].browsePath = (UA_QualifiedName *)UA_Array_new(
        filter.selectClauses[0].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[0].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses,
                        &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[0].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[0].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "Message");

    // Severity field
    filter.selectClauses[1].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[1].browsePathSize = 1;
    filter.selectClauses[1].browsePath = (UA_QualifiedName *)UA_Array_new(
        filter.selectClauses[1].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[1].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses,
                        &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[1].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[1].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "Severity");

    // SourceName
    filter.selectClauses[2].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[2].browsePathSize = 1;
    filter.selectClauses[2].browsePath = (UA_QualifiedName *)UA_Array_new(
        filter.selectClauses[2].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[2].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses,
                        &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[2].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[2].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "SourceName");

    // ReceiveTime
    filter.selectClauses[3].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[3].browsePathSize = 1;
    filter.selectClauses[3].browsePath = (UA_QualifiedName *)UA_Array_new(
        filter.selectClauses[3].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[3].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses,
                        &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[3].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[3].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "ReceiveTime");

    // Source Node
    filter.selectClauses[4].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[4].browsePathSize = 1;
    filter.selectClauses[4].browsePath = (UA_QualifiedName *)UA_Array_new(
        filter.selectClauses[4].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[4].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses,
                        &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[4].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[4].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "SourceNode");

    filter.selectClausesSize = nSelectClauses;

    item.requestedParameters.filter.encoding = UA_EXTENSIONOBJECT_DECODED;
    item.requestedParameters.filter.content.decoded.data = &filter;
    item.requestedParameters.filter.content.decoded.type =
        &UA_TYPES[UA_TYPES_EVENTFILTER];

    UA_UInt32 monId = 0;
    UA_MonitoredItemCreateResult result = UA_Client_MonitoredItems_createEvent(
        client, response.subscriptionId, UA_TIMESTAMPSTORETURN_BOTH, item, &monId,
        handler_Event, NULL);

    if(result.statusCode != UA_STATUSCODE_GOOD) {
        std::ostringstream oss;
        oss << "Could not add the MonitoredItem: 0x" << std::hex << hex << " "
            << result.statusCode;
        log(oss.str(), LogLevel ::ERRORS);
    } else {
        log("Monitoring 'Root->Objects->Server', id " +
            std::to_string(result.monitoredItemId));
    }

    // Cleanup
    UA_MonitoredItemCreateResult_clear(&result);
    UA_Array_delete(filter.selectClauses, nSelectClauses,
                    &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
}
