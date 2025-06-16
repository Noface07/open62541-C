#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <optional>

using namespace std;




struct MappedInfospaceTag{
    int id;
    int tagId;
    string name;
    string namespaces;
    bool isSimulationProfile;
    bool isLogging;
    bool isVirtual;
};

struct TagInfo{
    bool scaling;
    double rawMin;
    double rawMax;
    double scaleMin;
    double scaleMax;
    bool enableExpression;
    string expression;
    optional<vector<MappedInfospaceTag>> mappedInfospaceTags;
    optional<int> dataPointId;
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
};

struct ServerInfoO{
    string cfgName;
    string endpointUrl;
    string securityPolicy;
    string msgSecurityMode;
    string authType;
    vector<GroupInfo> groups;
    vector<TagInfo> tags;
    int dataPointId;
    string name;
    string nodeId;
    string typeId;
    string parentId;
};
