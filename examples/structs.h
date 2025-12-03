#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <optional>
#include "SqliteQueueService.h"

using namespace std;

class MQTTHandler; // Forward declaration


struct MappedInfospaceTag{
    int id;
    int tagId;
    string name;
    string namespaces;
    int samplingInterval;
    int deadband;
    int queuesize;
    int orgId;


    bool scaling;
    double rawMin;
    double rawMax;
    double scaleMin;
    double scaleMax;
    string sourceDatatype;
    bool enableExpression;
    string expression;


    // bool isSimulationProfile;
    // bool isLogging;
    // bool isVirtual;
};

struct TagInfo{
    bool scaling;
    double rawMin;
    double rawMax;
    double scaleMin;
    double scaleMax;
    string sourceDatatype;
    bool enableExpression;
    int samplingInterval;
    int deadband;
    int queuesize;
    string rdWtOpt;
    string expression;
    optional<string> namespaceNodeID;
    optional<vector<MappedInfospaceTag>> mappedInfospaceTags;
    int dataPointId;
    optional<string> name;
    optional<string> nodeId;
    optional<string> typeId;
    optional<string> parentId;
};

struct GroupInfo{
    vector<TagInfo> tags;
    int dataPointId;
    string typeId;
    string parentId;
    string name;
    string nodeId;
    int publishingInterval;
    int lifetimeCount;
    int maxKeepAliveCount;
    int priority;
    int maxNotificationsPerPublish;
};

struct ServerInfoO{
    string cfgName;
    string endpointUrl;
    string securityPolicy;
    string msgSecurityMode;
    string authType;
    vector<GroupInfo> groups;
    // vector<TagInfo> tags;
    int dataPointId;
    string name;
    string nodeId;
    string typeId;
    string parentId;
};

struct MyMonitorContext {
    MappedInfospaceTag infoSpace;
    MQTTHandler* mqttHandler;
    SqliteQueueService* sqliteService;
    // unordered_map<int, string> TopicMapping;
    // json payload;
    // Add more fields as needed
};


