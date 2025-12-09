#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <optional>

using namespace std;

struct AlarmTrigger{
    int id;
    // string alarmShortCode;
    string applicableTagId;
    string applicableTagName;
    string tagType;
    double hiHi;
    double hi;
    double lo;
    double loLo;
    string state;
    int activationDelay;
    int hysteresisOrResetDelay;
    string evaluatedOn;
    string evaluationInterval;
    // string activationType;s
    // double distance;
    double value;
};

struct AlarmEmitter{
    int id;
    string alarmShortcode;
    int emitterNode;
    string emitterNodeName;
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
    string bedgeNotification;
    string smsNotification;
    string emailNotification;
    // string whatsapNotification;
    string reset;
    string escalationWF;
    string suppression;
    // string enableLogging;
    string loggingFreq;
    string purging;
    int orgId;
    string enable;
    // string removeIds;
    optional<vector<AlarmTrigger>> alarmTriggers;
    optional<vector<AlarmEmitter>> alarmEmitters;
};
