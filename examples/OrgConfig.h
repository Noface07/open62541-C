#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <optional>

using namespace std;


struct OrgConfig{
    int id;
    string name;
    string shortCode;
    string emailPrimary;
    string emailSecondary;
    string contactNoPrimary;
    string contactNoSecondary;
    string remark;
    string displayName;
    string organisationType;
    string tenancyType;
    int profileId;
    int orgId;
    bool isCopyProfile;
};
