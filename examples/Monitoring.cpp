#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#include <iomanip>
#include <iostream>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include <nlohmann/json.hpp>
#include "MQTThandler.h"

using namespace std;
using json = nlohmann::ordered_json;

#ifdef UA_ENABLE_SUBSCRIPTIONS

static void
handler_NodeValueChanged(UA_Client *client, UA_UInt32 subId, void *subContext,
                         UA_UInt32 monId, void *monContext, UA_DataValue *value) {

    auto* myContext = static_cast<MyMonitorContext*>(monContext);
    cout << "myContext->infoSpace.Namespace: " << myContext->infoSpace.namespaces << endl;
    json data;
    data["TagId"] = myContext->infoSpace.tagId;



    // Convert UA_Variant to a native type for JSON
    if (UA_Variant_isScalar(&value->value)) {
        if (value->value.type == &UA_TYPES[UA_TYPES_INT32]) {
            data["Value"] = *(UA_Int32*)value->value.data;
        } else if (value->value.type == &UA_TYPES[UA_TYPES_DOUBLE]) {
            data["Value"] = *(UA_Double*)value->value.data;
        }
        else if(value->value.type == &UA_TYPES[UA_TYPES_FLOAT])
        {data["Value"] = *(UA_Float*)value->value.data;}
        else if(value->value.type == &UA_TYPES[UA_TYPES_BOOLEAN])
        {data["Value"] = *(UA_Boolean*)value->value.data;} 
        else if(value->value.type == &UA_TYPES[UA_TYPES_STRING])
        {
            UA_String str = *(UA_String*)value->value.data;
            data["Value"] = std::string((char*)str.data, str.length);
        }
        else {
            data["Value"] = nullptr; // or a string "unsupported"
        }
    } else {
        data["Value"] = nullptr;
    }





    data["TagType"] = "INFO_DCR";
    



    // Extract and format timestamp in local time with high precision
    std::string timestamp;
    UA_DateTime utc_dt_val;

    if (value->hasSourceTimestamp) {
        utc_dt_val = value->sourceTimestamp;
    } else if (value->hasServerTimestamp) {
        utc_dt_val = value->serverTimestamp;
    } else {
        utc_dt_val = UA_DateTime_now();
    }

    UA_Int64 offset_100ns = UA_DateTime_localTimeUtcOffset();
    UA_DateTime local_dt_val = utc_dt_val + offset_100ns;
    UA_DateTimeStruct dt = UA_DateTime_toStruct(local_dt_val);

    long offset_seconds = offset_100ns / UA_DATETIME_SEC;
    char offset_sign = (offset_seconds >= 0) ? '+' : '-';
    long offset_hours = labs(offset_seconds / 3600);
    long offset_minutes = labs((offset_seconds % 3600) / 60);

    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d.%03d%03d%01d%c%02ld:%02ld",
             dt.year, dt.month, dt.day,
             dt.hour, dt.min, dt.sec,
             dt.milliSec, dt.microSec, dt.nanoSec / 100,
             offset_sign, offset_hours, offset_minutes);
    timestamp = std::string(buffer);





    
    data["TimeStamp"] = timestamp;
    data["Source"] = 4;
    data["DatapointId"] = myContext->infoSpace.tagId;
    data["InfoId"] = 1001;
    
    // Add quality information if available
    if (value->hasStatus) {
        data["Quality"] = value->status;
    } else {
        data["Quality"] = UA_STATUSCODE_GOOD; // Default to good quality
    }
    
    data["UpdateType"] = 1;

    json payload;
    payload["Data"] = json::array({data});

    // std::cout << payload.dump(4) << std::endl;
    

    std::cout << "The Monitored Item " << monId << " has changed!"<< std::endl;


    if (!myContext->mqttHandler) {
        std::cout << "MQTT handler is null!" << std::endl;
    } else {
        bool published = myContext->mqttHandler->publish(myContext->infoSpace.namespaces, payload.dump());
        if (!published) {
            std::cout << "MQTT publish failed!" << std::endl;
        } else {
            std::cout << "MQTT publish succeeded!" << std::endl;
        }
    }

    UA_Variant *variant = &value->value;

    if(variant->type == &UA_TYPES[UA_TYPES_INT32]) {
        std::cout << "New value (int32): " << *(UA_Int32 *)variant->data << std::endl;
        cout << endl;
    } else if(variant->type == &UA_TYPES[UA_TYPES_DOUBLE]) {
        std::cout << "New value (double): " << *(UA_Double *)variant->data << std::endl;
        cout << endl;
    } else if(variant->type == &UA_TYPES[UA_TYPES_FLOAT]) {
        std::cout << "New value (float): " << *(UA_Float *)variant->data << std::endl;
        cout << endl;
    } else if(variant->type == &UA_TYPES[UA_TYPES_BOOLEAN]) {
        std::cout << "New value (bool): "
                  << (*(UA_Boolean *)variant->data ? "true" : "false") << std::endl;
        cout << endl;
    } else if(variant->type == &UA_TYPES[UA_TYPES_STRING]) {
        UA_String str = *(UA_String *)variant->data;
        std::cout << "New value (string): " << std::string((char *)str.data, str.length)
                  << std::endl;
        cout << endl;
    } else {
        std::cout << "Unsupported data type: " << variant->type->typeName << std::endl;
        cout << endl;
    }
}

#endif

static UA_MonitoredItemCreateRequest
createMonitoredItemRequest(UA_NodeId nodeId) {

    UA_MonitoredItemCreateRequest request;

    if(nodeId.identifierType == UA_NODEIDTYPE_NUMERIC) {
        request = UA_MonitoredItemCreateRequest_default(
            UA_NODEID_NUMERIC(nodeId.namespaceIndex, nodeId.identifier.numeric));
    } else if(nodeId.identifierType == UA_NODEIDTYPE_STRING) {
        request = UA_MonitoredItemCreateRequest_default(UA_NODEID_STRING(
            nodeId.namespaceIndex, const_cast<char *>(reinterpret_cast<const char *>(
                                       nodeId.identifier.string.data))));
    } else if(nodeId.identifierType == UA_NODEIDTYPE_GUID) {
        request = UA_MonitoredItemCreateRequest_default(
            UA_NODEID_GUID(nodeId.namespaceIndex, nodeId.identifier.guid));
    } else if(nodeId.identifierType == UA_NODEIDTYPE_BYTESTRING) {
        UA_ByteString bs = nodeId.identifier.byteString;
        request = UA_MonitoredItemCreateRequest_default(
            UA_NODEID_BYTESTRING(nodeId.namespaceIndex, (char *)bs.data));
    } else {
        // Default case - create an empty request
        request = UA_MonitoredItemCreateRequest_default(UA_NODEID_NULL);
    }

    return request;
}

static UA_NodeId
parseNodeId(const char *nodeIdStr) {
    UA_NodeId nodeId = UA_NODEID_NULL;

    // Parse namespace
    const char *nsStart = strstr(nodeIdStr, "ns=");
    if(!nsStart)
        return nodeId;

    const char *nsEnd = strchr(nsStart + 3, ';');
    if(!nsEnd)
        return nodeId;

    // Extract namespace index
    size_t nsLen = nsEnd - (nsStart + 3);
    char *nsStr = (char *)UA_malloc(nsLen + 1);
    if(!nsStr)
        return nodeId;

    memcpy(nsStr, nsStart + 3, nsLen);
    nsStr[nsLen] = '\0';

    // Convert namespace string to index
    UA_UInt16 namespaceIndex = (UA_UInt16)strtoul(nsStr, NULL, 10);
    UA_free(nsStr);

    // Parse identifier type and value
    const char *idStart = nsEnd + 1;
    if(strncmp(idStart, "i=", 2) == 0) {
        // Numeric identifier
        UA_UInt32 numericId = (UA_UInt32)strtoul(idStart + 2, NULL, 10);
        nodeId = UA_NODEID_NUMERIC(namespaceIndex, numericId);
    } else if(strncmp(idStart, "s=", 2) == 0) {
        // String identifier
        nodeId = UA_NODEID_STRING(namespaceIndex, const_cast<char *>(idStart + 2));
    } else if(strncmp(idStart, "g=", 2) == 0) {
        // GUID identifier
        UA_Guid guid;
        if(UA_Guid_parse(&guid, UA_String_fromChars(idStart + 2)) == UA_STATUSCODE_GOOD) {
            nodeId = UA_NODEID_GUID(namespaceIndex, guid);
        }
    } else if(strncmp(idStart, "b=", 2) == 0) {
        // Base64 string identifier
        UA_ByteString bs;
        bs.length = strlen(idStart + 2);
        bs.data = (UA_Byte *)UA_malloc(bs.length);
        if(bs.data) {
            memcpy(bs.data, idStart + 2, bs.length);
            nodeId = UA_NODEID_BYTESTRING(namespaceIndex, (char *)bs.data);
            UA_ByteString_clear(&bs);
        }
    }

    return nodeId;
}




static void
MonitorItem(UA_Client *client, UA_CreateSubscriptionResponse response,
            const char *nodeIdStr, int tagID, MyMonitorContext *myContext) {

    UA_MonitoredItemCreateRequest monRequest;
    UA_MonitoredItemCreateResult monResponse;

    // global_mqttHandler = g_mqttHandler;

    UA_NodeId nodeId = parseNodeId(nodeIdStr);
    if(UA_NodeId_isNull(&nodeId)) {
        cout << "Invalid node ID format: " << nodeIdStr << endl;
        return;
    }

    monRequest = createMonitoredItemRequest(nodeId);

    monRequest.requestedParameters.clientHandle = 42;
    
    // Configure monitoring parameters for better timestamp handling
    monRequest.requestedParameters.samplingInterval = 1000.0; // 1 second sampling interval
    monRequest.requestedParameters.queueSize = 1; // Queue size for notifications
    monRequest.requestedParameters.discardOldest = true; // Discard oldest when queue is full
    
    // Configure the monitoring filter
    // UA_DataChangeFilter filter;
    // filter.deadbandType = UA_DEADBANDTYPE_NONE; // No deadband filtering
    // filter.deadbandValue = 0.0;
    // monRequest.requestedParameters.filter = (UA_ExtensionObject) {
    //     .encoding = UA_EXTENSIONOBJECT_DECODED,
    //     .content.decoded.type = &UA_TYPES[UA_TYPES_DATACHANGEFILTER],
    //     .content.decoded.data = &filter
    // };

    monResponse = UA_Client_MonitoredItems_createDataChange(
        client, response.subscriptionId, UA_TIMESTAMPSTORETURN_BOTH, monRequest, myContext,
        handler_NodeValueChanged, NULL);
    if(monResponse.statusCode == UA_STATUSCODE_GOOD) {
        UA_String nodeIdStr = UA_STRING_NULL;
        UA_NodeId_print(&monRequest.itemToMonitor.nodeId, &nodeIdStr);
        cout << "Monitoring Node " << string((char *)nodeIdStr.data, nodeIdStr.length)
             << ", id " << monResponse.monitoredItemId << tagID << endl;
        UA_String_clear(&nodeIdStr);
    }
}
