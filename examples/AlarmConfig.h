#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <optional>

using namespace std;



            
struct AlarmTrigger{
    int id;
    string tagType;
    double hiHi;
    double hi;
    double lo;
    double loLo;
    string state;
    int activationDelay;
    int resetDelayInCounter;
    string evaluatedOn;
    string evaluationInterval;
    string triggerType;
    int tagId;
    string nameSpace;
    string topic;
    int infoId;
};

struct AlarmEmitter{
    int id;
    string alarmShortcode;
    int emitterNode;
    string emitterNodeName;
    string alarmTagNameSpace;
    int orgId;
    // string createdBy;
    // string createdOn;
    // string updatedBy;
    // string updatedOn;
    // string isDeleted;
    // string deletedBy;
    // string deletedOn;
};

struct AlarmConfig{
    int id;
    string name;
    string shortCode;
    string description;
    string priority;
    int severityOffset;
    string category;
    string subCategory;
    string alarmSourceType;
    string areaDeviceName;
    string message;
    // string state;
    string sopProcedure;
    string annunciation;
    string ackType;
    bool bedgeNotification;
    bool smsNotification;
    bool emailNotification;
    // string whatsapNotification;
    string reset;
    bool escalationWF;
    bool suppression;
    // string enableLogging;
    string loggingFreq;
    string purging;
    int orgId;
    bool enable;
    // string removeIds;
    optional<vector<AlarmTrigger>> alarmTriggers;
    optional<vector<AlarmEmitter>> alarmEmitters;
};
