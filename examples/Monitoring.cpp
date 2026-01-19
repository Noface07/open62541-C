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
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::ordered_json;

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

static void
handler_NodeValueChanged(UA_Client *client, UA_UInt32 subId, void *subContext,
                         UA_UInt32 monId, void *monContext, UA_DataValue *value) {
    auto *myContext = static_cast<MyMonitorContext *>(monContext);
    if(!myContext) {
        log("Monitoring context is null!", LogLevel::ERRORS);
        return;
    }

    MqttPayload p;
    // p.datapointId = myContext->infoSpace.tagId; // Assuming tagId is int? Check struct
    // definition if needed.
    p.datapointId = myContext->infoSpace.tagId;
    p.name = myContext->infoSpace.name;
    p.tagId = myContext->infoSpace.tagId;  // Redundant?
    p.tagType = myContext->infoSpace.sourceDatatype;
    p.source = static_cast<int>(AlarmSource::OPC);           // TODO: parameterize
    p.infoId = 1001;                                         // TODO: parameterize
    p.quality = static_cast<int>(AlarmSource::OPC);          // Good quality by default
    p.UpdateType = static_cast<int>(UpdateType::Telemetry);  // Simple update by default

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
                // Update the bound variable directly
                myContext->exprContext->value = processedValue;
                // Evaluate without parsing
                double result = myContext->exprContext->expression.value();
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

    // Publish or queue
    // Direct check avoids function call overhead if simple getter
    // Publish or queue
    // Async Path (Preferred)
    if(myContext->asyncPublisher) {
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

        myContext->asyncPublisher->enqueue(myContext->infoSpace.namespaces,
                                           payload.dump());
    }
    // Fallback: Synchronous (Blocking) Path
    else if(myContext->mqttHandler && myContext->mqttHandler->isConnected()) {
        // Construct JSON manually or use library - library adds overhead but ensures
        // correctness. Assuming nlohmann::json is efficient enough. Improvement: Reuse a
        // thread-local json object to avoid repeated allocations? For now, struct
        // MqttPayload abstraction creates copies.

        // Log is potentially slow - verify log level efficiently before constructing
        // strings log(...) usually checks level inside, but string construction happens
        // before call.
        // TODO: Add macro based logging to avoid string construction if level not met.
        // Keeping existing log logic for safety.

        json data = {{"TagId", p.tagId},     {"Value", p.value},
                     {"TagType", p.tagType}, {"TimeStamp", p.timeStamp},
                     {"Source", p.source},   {"DatapointId", p.datapointId},
                     {"InfoId", p.infoId},   {"Quality", p.quality},
                     {"UpdateType", 1}};

        json payload;
        payload["Data"] = json::array({data});

        myContext->mqttHandler->publish(myContext->infoSpace.namespaces, payload.dump());

    } else if(myContext->sqliteService) {
        long orgId = 1;
        myContext->sqliteService->EnqueueMessage(myContext->infoSpace.namespaces, p,
                                                 orgId);
    }
}

#endif

static UA_MonitoredItemCreateRequest
createMonitoredItemRequest(UA_NodeId nodeId) {

    UA_MonitoredItemCreateRequest request;

    if(nodeId.identifierType == UA_NODEIDTYPE_NUMERIC) {
        UA_MonitoredItemCreateRequest_init(&request);
        request.itemToMonitor.nodeId = UA_NODEID_NUMERIC(nodeId.namespaceIndex, nodeId.identifier.numeric);
        request.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
        request.monitoringMode = UA_MONITORINGMODE_REPORTING;
    } else if(nodeId.identifierType == UA_NODEIDTYPE_STRING) {
        UA_MonitoredItemCreateRequest_init(&request);
        request.itemToMonitor.nodeId = UA_NODEID_STRING(
            nodeId.namespaceIndex, const_cast<char *>(reinterpret_cast<const char *>(
                                       nodeId.identifier.string.data)));
        request.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
        request.monitoringMode = UA_MONITORINGMODE_REPORTING;
    } else if(nodeId.identifierType == UA_NODEIDTYPE_GUID) {
        UA_MonitoredItemCreateRequest_init(&request);
        request.itemToMonitor.nodeId = UA_NODEID_GUID(nodeId.namespaceIndex, nodeId.identifier.guid);
        request.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
        request.monitoringMode = UA_MONITORINGMODE_REPORTING;
    } else if(nodeId.identifierType == UA_NODEIDTYPE_BYTESTRING) {
        UA_ByteString bs = nodeId.identifier.byteString;
        UA_MonitoredItemCreateRequest_init(&request);
        request.itemToMonitor.nodeId = UA_NODEID_BYTESTRING(nodeId.namespaceIndex, (char *)bs.data);
        request.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
        request.monitoringMode = UA_MONITORINGMODE_REPORTING;
    } else {
        // Default case - create an empty request
        UA_MonitoredItemCreateRequest_init(&request);
        request.itemToMonitor.nodeId = UA_NODEID_NULL;
        request.itemToMonitor.attributeId = UA_ATTRIBUTEID_VALUE;
        request.monitoringMode = UA_MONITORINGMODE_REPORTING;
    }

    return request;
}

static UA_NodeId
parseNodeId(UA_Client *client, const char *nodeIdStr, const char *overrideNamespaceUri) {

    // printf("Parsing NodeId string: %s\n with NamesapceURI : %s", nodeIdStr ,
    // overrideNamespaceUri);
    UA_NodeId nodeId = UA_NODEID_NULL;

    /* --------------------------------------------------
     * 1) Parse namespace from ns=<index>
     * -------------------------------------------------- */

    const char *nsStart = strstr(nodeIdStr, "ns=");
    if(!nsStart)
        return nodeId;

    const char *nsEnd = strchr(nsStart + 3, ';');
    if(!nsEnd)
        return nodeId;

    UA_UInt16 namespaceIndex = (UA_UInt16)strtoul(nsStart + 3, NULL, 10);

    /* --------------------------------------------------
     * 2) OPTIONAL override using Namespace URI
     * -------------------------------------------------- */

    if(overrideNamespaceUri && client) {
        UA_UInt16 resolvedNs;
        UA_StatusCode sc = UA_Client_getNamespaceIndex(
            client, UA_STRING((char *)overrideNamespaceUri), &resolvedNs);

        if(sc == UA_STATUSCODE_GOOD) {
            namespaceIndex = resolvedNs; /* override */
        }
        /* else: silently keep original ns */
    }

    /* --------------------------------------------------
     * 3) Parse identifier
     * -------------------------------------------------- */

    const char *idStart = nsEnd + 1;

    if(strncmp(idStart, "i=", 2) == 0) {
        UA_UInt32 numericId = (UA_UInt32)strtoul(idStart + 2, NULL, 10);
        nodeId = UA_NODEID_NUMERIC(namespaceIndex, numericId);

    } else if(strncmp(idStart, "s=", 2) == 0) {
        nodeId = UA_NODEID_STRING_ALLOC(namespaceIndex, idStart + 2);

    } else if(strncmp(idStart, "g=", 2) == 0) {
        UA_Guid guid;
        UA_String guidStr = UA_String_fromChars(idStart + 2);
        if(UA_Guid_parse(&guid, guidStr) == UA_STATUSCODE_GOOD)
            nodeId = UA_NODEID_GUID(namespaceIndex, guid);
        UA_String_clear(&guidStr);

    } else if(strncmp(idStart, "b=", 2) == 0) {
        UA_ByteString bs;
        UA_ByteString_init(&bs);
        bs.length = strlen(idStart + 2);
        bs.data = (UA_Byte *)UA_malloc(bs.length);
        if(bs.data) {
            memcpy(bs.data, idStart + 2, bs.length);
            nodeId = UA_NODEID_BYTESTRING_ALLOC(namespaceIndex, (const char *)bs.data);
        }
    }

    return nodeId;
}

// Include the header to ensure signature match
#include "Monitoring.h"

void
MonitorItem(UA_Client *client, UA_CreateSubscriptionResponse response,
            const char *nodeIdStr, int tagID, MyMonitorContext *myContext,
            const char *overrideNamespaceUri, TaskScheduler scheduler, int attempt) {

    UA_MonitoredItemCreateRequest monRequest;
    UA_MonitoredItemCreateResult monResponse;

    // global_mqttHandler = g_mqttHandler;

    // PRE-COMPILE Expression Logic
    // This is done ONCE per monitored item
    if(myContext->infoSpace.enableExpression &&
       !myContext->infoSpace.expression.empty()) {
        myContext->exprContext = std::make_shared<ExprTkContext>();

        // 1. Add variable 'value' to symbol table and bind to context value
        myContext->exprContext->symbol_table.add_variable("value",
                                                          myContext->exprContext->value);
        myContext->exprContext->symbol_table.add_constants();  // Adds pi, epsilon, etc.

        // 2. Register symbol table with expression
        myContext->exprContext->expression.register_symbol_table(
            myContext->exprContext->symbol_table);

        // 3. Compile expression using local parser
        exprtk::parser<double> parser;
        // Transform expression to lower case for consistency
        std::string exprStr = myContext->infoSpace.expression;
        std::transform(exprStr.begin(), exprStr.end(), exprStr.begin(), ::tolower);

        if(!parser.compile(exprStr, myContext->exprContext->expression)) {
            log("ExprTk compilation failed for tag " + std::to_string(tagID) + ": " +
                    parser.error(),
                LogLevel::ERRORS);
            // Verify if we should fallback or disable expression?
            myContext->exprContext.reset();  // Disable if failed to compile
        } else {
            log("ExprTk compiled successfully for tag " + std::to_string(tagID),
                LogLevel::DEBUG);
        }
    }

    UA_NodeId nodeId = parseNodeId(client, nodeIdStr, overrideNamespaceUri);
    if(UA_NodeId_isNull(&nodeId)) {
        log("Invalid node ID format: " + string(nodeIdStr), LogLevel::ERRORS);
        return;
    }

    monRequest = createMonitoredItemRequest(nodeId);

    monRequest.requestedParameters.clientHandle = 42;

    // Configure monitoring parameters for better timestamp handling
    // monRequest.requestedParameters.samplingInterval =
    // myContext->infoSpace.samplingInterval; // 1 second sampling interval
    monRequest.requestedParameters.samplingInterval = 0;  // 1 second sampling interval
    // monRequest.requestedParameters.queueSize = myContext->infoSpace.queuesize; // Queue
    // size for notifications
    monRequest.requestedParameters.queueSize = 10000;  // Queue size for notifications
    monRequest.requestedParameters.discardOldest =
        false;  // Discard oldest when queue is full

    // Configure the monitoring filter
    UA_DataChangeFilter filter;

    if(myContext->infoSpace.deadband > 0) {
        filter.deadbandType = UA_DEADBANDTYPE_PERCENT;
        filter.deadbandValue = myContext->infoSpace.deadband;
    } else {
        filter.deadbandType = UA_DEADBANDTYPE_NONE;
        filter.deadbandValue = 0.0;
    }

    filter.trigger = UA_DATACHANGETRIGGER_STATUSVALUE;
    UA_ExtensionObject filterExtObj;
    memset(&filterExtObj, 0, sizeof(filterExtObj));
    filterExtObj.encoding = UA_EXTENSIONOBJECT_DECODED;
    filterExtObj.content.decoded.type = &UA_TYPES[UA_TYPES_DATACHANGEFILTER];
    filterExtObj.content.decoded.data = &filter;
    monRequest.requestedParameters.filter = filterExtObj;

    // log("Requesting MonitoredItem for NodeId: " + string(nodeIdStr) +
    //     " | Sampling: " +
    //     std::to_string(monRequest.requestedParameters.samplingInterval) + "ms | Queue:
    //     " + std::to_string(monRequest.requestedParameters.queueSize), LogLevel::INFO);

    // Async Retry Logic
    int maxRetries = 5;

    monResponse = UA_Client_MonitoredItems_createDataChange(
        client, response.subscriptionId, UA_TIMESTAMPSTORETURN_BOTH, monRequest,
        myContext, handler_NodeValueChanged, NULL);

    if(monResponse.statusCode == UA_STATUSCODE_GOOD) {
        UA_String nodeIdStrPrint = UA_STRING_NULL;
        UA_NodeId_print(&monRequest.itemToMonitor.nodeId, &nodeIdStrPrint);
        // log("Monitoring Created Successfully for Node " + string((char
        // *)nodeIdStrPrint.data, nodeIdStrPrint.length) +
        //     " | Id: " + to_string(monResponse.monitoredItemId) + " | TagID: " +
        //     to_string(tagID), LogLevel::INFO);
        UA_String_clear(&nodeIdStrPrint);
    } else if(monResponse.statusCode == UA_STATUSCODE_BADNODEIDUNKNOWN &&
              attempt < maxRetries) {
        // log("Failed to create MonitoredItem (BadNodeIdUnknown) for TagID " +
        // to_string(tagID) +
        //     ". Retrying in 2 seconds... (Attempt " + to_string(attempt + 1) + "/" +
        //     to_string(maxRetries) + ")", LogLevel::INFO);

        if(scheduler) {
            // CRITICAL FIX: Capture strings by VALUE (std::string) to avoid dangling
            // pointers
            std::string nsUriCopy = overrideNamespaceUri ? overrideNamespaceUri : "";
            std::string nodeIdCopy = nodeIdStr ? nodeIdStr : "";

            // Spawn a thread to wait, then schedule the next attempt on the main loop
            std::thread([=]() {
                std::this_thread::sleep_for(std::chrono::seconds(2));
                // Pass copies into the inner lambda
                scheduler([=]() {
                    MonitorItem(client, response, nodeIdCopy.c_str(), tagID, myContext,
                                nsUriCopy.c_str(), scheduler, attempt + 1);
                });
            }).detach();

            return;
        } else {

            // Fallback to synchronous sleep if no scheduler provided (legacy behavior)
            log("No scheduler provided, falling back to synchronous retry",
                LogLevel::WARNING);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            MonitorItem(client, response, nodeIdStr, tagID, myContext,
                        overrideNamespaceUri, nullptr, attempt + 1);
            return;
        }

    } else {
        // Other error or max retries reached
        // log("Failed to create MonitoredItem for TagID " + to_string(tagID) +
        //" | Status: " + string(UA_StatusCode_name(monResponse.statusCode)),
        //LogLevel::ERRORS);
    }

    if(monResponse.statusCode != UA_STATUSCODE_GOOD) {
        log("Permanently failed to create MonitoredItem for TagID " + to_string(tagID),
            LogLevel::ERRORS);
        // Clean up context to avoid memory leak since it wasn't passed to a successful
        // monitor
        delete myContext;
    }
}

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
        filter.selectClauses[1].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
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
        filter.selectClauses[1].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
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
        filter.selectClauses[1].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
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
        log("Monitoring 'Root->Objects->Server', id " + result.monitoredItemId);
    }

    // Cleanup
    UA_MonitoredItemCreateResult_clear(&result);
    UA_Array_delete(filter.selectClauses, nSelectClauses,
                    &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
}
