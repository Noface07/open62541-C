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
    monRequest.requestedParameters.samplingInterval = myContext->infoSpace.samplingInterval; // 1 second sampling interval
    monRequest.requestedParameters.queueSize = myContext->infoSpace.queuesize; // Queue size for notifications
    monRequest.requestedParameters.discardOldest = true; // Discard oldest when queue is full
    
    // Configure the monitoring filter
    UA_DataChangeFilter filter;
    filter.deadbandType = UA_DEADBANDTYPE_PERCENT; // No deadband filtering
    filter.deadbandValue =  myContext->infoSpace.deadband;
    filter.trigger = UA_DATACHANGETRIGGER_STATUSVALUE;
    UA_ExtensionObject filterExtObj;
    memset(&filterExtObj, 0, sizeof(filterExtObj));
    filterExtObj.encoding = UA_EXTENSIONOBJECT_DECODED;
    filterExtObj.content.decoded.type = &UA_TYPES[UA_TYPES_DATACHANGEFILTER];
    filterExtObj.content.decoded.data = &filter;
    monRequest.requestedParameters.filter = filterExtObj;

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








void handler_Event(UA_Client *client, UA_UInt32 subId, void *subContext,
                   UA_UInt32 monId, void *monContext,
                   size_t nEventFields, UA_Variant *eventFields) {
    std::cout << "Received Event Notification (" << nEventFields << " fields):" << std::endl;
    
    for(size_t i = 0; i < nEventFields; ++i) {
        if(UA_Variant_hasScalarType(&eventFields[i], &UA_TYPES[UA_TYPES_UINT16])) {
            UA_UInt16 severity = *(UA_UInt16 *)eventFields[i].data;
            std::cout << "  Severity: " << severity << std::endl;
        } else if (UA_Variant_hasScalarType(&eventFields[i], &UA_TYPES[UA_TYPES_LOCALIZEDTEXT])) {
            UA_LocalizedText *lt = (UA_LocalizedText *)eventFields[i].data;
            std::cout << "  Message: " << std::string((char*)lt->text.data, lt->text.length) << std::endl;
        }
        else if (UA_Variant_hasScalarType(&eventFields[i], &UA_TYPES[UA_TYPES_STRING])) {
            UA_String *s = (UA_String *)eventFields[i].data;
            std::cout << "  Source Name: " << std::string((char*)s->data, s->length) << std::endl;
        }

        else if (UA_Variant_hasScalarType(&eventFields[i], &UA_TYPES[UA_TYPES_DATETIME])) {
            UA_DateTime dt = *(UA_DateTime *)eventFields[i].data;
            UA_Int64 UnixTime = UA_DateTime_toUnixTime(dt);

            // Convert to time_t (seconds since epoch)
            std::time_t t = static_cast<std::time_t>(UnixTime);

            // Convert to local time and print
            char buf[64];
            std::tm *tm_info = std::localtime(&t);
            std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm_info);

            std::cout << "  Time: " << buf << std::endl;
        }

        else if (UA_Variant_hasScalarType(&eventFields[i], &UA_TYPES[UA_TYPES_NODEID])) {
            UA_NodeId *nid = (UA_NodeId *)eventFields[i].data;
            std::cout << "  NODE ID: ns=" << nid->namespaceIndex << ";";
            switch (nid->identifierType) {
                case UA_NODEIDTYPE_NUMERIC:
                    std::cout << "i=" << nid->identifier.numeric;
                    break;
                case UA_NODEIDTYPE_STRING:
                    std::cout << "s=" << std::string((char*)nid->identifier.string.data, nid->identifier.string.length);
                    break;
                case UA_NODEIDTYPE_GUID:
                    std::cout << "g=GUID";
                    break;
                case UA_NODEIDTYPE_BYTESTRING:
                    std::cout << "b=ByteString";
                    break;
            }
            std::cout << std::endl;
        }

         else {
            std::cout << "  Unknown field type" << std::endl;
        }
    }
    std::cout << std::endl;

    // payload
            // {
            // "type": "alarm",
            // "timestamp": "2025-06-25T15:40:12Z",
            // "tagId": 128,
            // "message": "Level exceeded",
            // "severity": 500
            // }

}

static void MonitorEvent(UA_Client *client, UA_CreateSubscriptionResponse response) {

    UA_Byte eventNotifier = 0;
    UA_StatusCode sc = UA_Client_readEventNotifierAttribute(client,
                            UA_NODEID_NUMERIC(0, 2253), &eventNotifier);
    if(sc == UA_STATUSCODE_GOOD)
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                    "Server.EventNotifier: 0x%02x", eventNotifier);
    else
        UA_LOG_WARNING(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                    "Could not read EventNotifier attribute");

    /* Add a MonitoredItem */
    UA_MonitoredItemCreateRequest item;
    UA_MonitoredItemCreateRequest_init(&item);
    item.itemToMonitor.nodeId = UA_NODEID_NUMERIC(0, 2253); // Root->Objects->Server
    item.itemToMonitor.attributeId = UA_ATTRIBUTEID_EVENTNOTIFIER;
    item.monitoringMode = UA_MONITORINGMODE_REPORTING;

    UA_EventFilter filter;
    UA_EventFilter_init(&filter);
    
    // Setup select clauses for Message and Severity (like in tutorial)
    const size_t nSelectClauses = 5;
    filter.selectClauses = (UA_SimpleAttributeOperand*)
        UA_Array_new(nSelectClauses, &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
    if(!filter.selectClauses) {
        std::cout << "Failed to allocate select clauses" << std::endl;
        return;
    }
    
    for(size_t i = 0; i < nSelectClauses; ++i) {
        UA_SimpleAttributeOperand_init(&filter.selectClauses[i]);
    }

    // Message field
    filter.selectClauses[0].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[0].browsePathSize = 1;
    filter.selectClauses[0].browsePath = (UA_QualifiedName*)
        UA_Array_new(filter.selectClauses[0].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[0].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses, &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[0].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[0].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "Message");

    // Severity field
    filter.selectClauses[1].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[1].browsePathSize = 1;
    filter.selectClauses[1].browsePath = (UA_QualifiedName*)
        UA_Array_new(filter.selectClauses[1].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[1].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses, &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[1].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[1].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "Severity");



        // SourceName
    filter.selectClauses[2].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[2].browsePathSize = 1;
    filter.selectClauses[2].browsePath = (UA_QualifiedName*)
        UA_Array_new(filter.selectClauses[1].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[2].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses, &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[2].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[2].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "SourceName");



    // ReceiveTime
    filter.selectClauses[3].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[3].browsePathSize = 1;
    filter.selectClauses[3].browsePath = (UA_QualifiedName*)
        UA_Array_new(filter.selectClauses[1].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[3].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses, &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[3].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[3].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "ReceiveTime");

    // Source Node
    filter.selectClauses[4].typeDefinitionId = UA_NS0ID(BASEEVENTTYPE);
    filter.selectClauses[4].browsePathSize = 1;
    filter.selectClauses[4].browsePath = (UA_QualifiedName*)
        UA_Array_new(filter.selectClauses[1].browsePathSize, &UA_TYPES[UA_TYPES_QUALIFIEDNAME]);
    if(!filter.selectClauses[4].browsePath) {
        UA_Array_delete(filter.selectClauses, nSelectClauses, &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
        return;
    }
    filter.selectClauses[4].attributeId = UA_ATTRIBUTEID_VALUE;
    filter.selectClauses[4].browsePath[0] = UA_QUALIFIEDNAME_ALLOC(0, "SourceNode");


    filter.selectClausesSize = nSelectClauses;

    item.requestedParameters.filter.encoding = UA_EXTENSIONOBJECT_DECODED;
    item.requestedParameters.filter.content.decoded.data = &filter;
    item.requestedParameters.filter.content.decoded.type = &UA_TYPES[UA_TYPES_EVENTFILTER];

    UA_UInt32 monId = 0;
    UA_MonitoredItemCreateResult result =
        UA_Client_MonitoredItems_createEvent(client, response.subscriptionId,
                                             UA_TIMESTAMPSTORETURN_BOTH, item,
                                             &monId, handler_Event, NULL);

    if(result.statusCode != UA_STATUSCODE_GOOD) {
        std::cout << "Could not add the MonitoredItem: 0x" << std::hex << result.statusCode << std::endl;
    } else {
        std::cout << "Monitoring 'Root->Objects->Server', id " << result.monitoredItemId << std::endl;
    }

    // Cleanup
    UA_MonitoredItemCreateResult_clear(&result);
    UA_Array_delete(filter.selectClauses, nSelectClauses, &UA_TYPES[UA_TYPES_SIMPLEATTRIBUTEOPERAND]);
}

