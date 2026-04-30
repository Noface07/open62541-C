#include "TelemetrySpill.h"

#include "SqliteQueueService.h"
#include "alarm_enums.h"

#include <nlohmann/json.hpp>

bool
TryEnqueueTelemetryWrapper(const std::string &topic, const std::string &wrapperJson,
                           SqliteQueueService *sqlite,
                           const std::map<int, long> &datapointToOrgId) {
    if(!sqlite || wrapperJson.empty())
        return false;

    try {
        nlohmann::json payload = nlohmann::json::parse(wrapperJson);
        if(!payload.contains("Data") || !payload["Data"].is_array() ||
           payload["Data"].empty())
            return false;

        const auto &data = payload["Data"][0];
        MqttPayload p;
        p.datapointId = data.value("DatapointId", 0);
        p.name = "";
        p.tagId = data.value("TagId", 0);
        p.tagType = data.value("TagType", "INFO_DCR");
        p.source = data.value("Source", static_cast<int>(AlarmSource::AEEngine));
        p.infoId = data.value("InfoId", 1001);
        if(data.contains("Value"))
            p.value = data["Value"];
        else
            p.value = nullptr;
        p.timeStamp = data.value("TimeStamp", "");
        p.quality = data.value("Quality", static_cast<int>(AlarmQuality::Good));
        p.UpdateType =
            data.value("UpdateType", static_cast<int>(UpdateType::Telemetry));

        long orgId = 1;
        if(p.datapointId > 0) {
            auto it = datapointToOrgId.find(p.datapointId);
            if(it != datapointToOrgId.end())
                orgId = it->second;
        }

        sqlite->EnqueueMessage(topic, p, orgId);
        return true;
    } catch(...) {
        return false;
    }
}
