#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <optional>

using namespace std;


struct orgMappings{
    int id;
    int hierarchyId;
    int mapOrgId;
    string orgShortCode;
};



struct ServerConfig{
    int id;
    int dataPointId;
    string name;
    string ip;
    int port;
    string nodeId;

    vector<orgMappings> orgMappings;
};

