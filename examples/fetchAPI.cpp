#include "fetchAPI.h"

#include <open62541/plugin/log_stdout.h>

#include <iostream>
#include <stdio.h>
#include <stdlib.h>

#include "AlarmConfig.h"
#include "Logger.h"
#include "OrgConfig.h"
#include "ServerConfig.h"
#include "UserProfile.h"
#include "structs.h"
#include <async_mqtt/all.hpp>
#include <async_mqtt/asio_bind/predefined_layer/mqtts.hpp>
#include <async_mqtt/asio_bind/predefined_layer/ws.hpp>
#include <async_mqtt/asio_bind/predefined_layer/wss.hpp>
#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <nlohmann/json.hpp>
#include <unordered_map>

using json = nlohmann::ordered_json;

using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;

as::io_context http_ioc;
unordered_map<int, pair<string, string>> Mapping;
unordered_map<int, string> TopicMapping;

// URL-encode helper
static std::string
url_encode(const std::string &value) {
    std::ostringstream escaped;
    escaped.fill('0');
    escaped << std::hex;
    for(auto c : value) {
        // Unreserved characters (RFC 3986)
        if(('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') ||
           c == '-' || c == '_' || c == '.' || c == '~') {
            escaped << c;
        } else {
            escaped << '%' << std::setw(2) << std::uppercase << int((unsigned char)c);
            escaped << std::nouppercase;  // reset flag
        }
    }
    return escaped.str();
}

json
getBearerToken(string host, string port, string username, string password) {
    try {
        std::string target = "/api/SignIn";
        int version = 11;

        // JSON body built from provided credentials
        // json jbody;
        // jbody["Username"] = username;
        // jbody["password"] = password;
        // std::string json_body = jbody.dump();
        //
        // Build form-encoded body (keys are lowercase per your screenshot)
        std::string form_body =
            "grant_type=" + url_encode("password") + "&username=" + url_encode(username) +
            "&password=" + url_encode(password) + "&client_id=" + url_encode("roclient");

        // Set up I/O context and resolver
        // as::io_context ioc;
        tcp::resolver resolver(http_ioc);
        beast::tcp_stream stream(http_ioc);

        // Resolve domain name
        auto const results = resolver.resolve(host, port);

        // Connect to host
        stream.connect(results);

        // Create HTTP POST request
        beast::http::request<beast::http::string_body> req{beast::http::verb::post,
                                                           target, version};
        req.set(beast::http::field::host, host);
        req.set(beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
        req.set(beast::http::field::content_type, "application/x-www-form-urlencoded");
        req.body() = form_body;
        req.prepare_payload();

        // Send request
        beast::http::write(stream, req);

        // Read response
        beast::flat_buffer buffer;
        beast::http::response<beast::http::string_body> res;
        beast::http::read(stream, buffer, res);

        json result = json::parse(res.body());
        // log(res.body().c_str(),LogLevel::DEBUG);

        // Gracefully close the connection
        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        if(ec && ec != beast::errc::not_connected)
            throw beast::system_error{ec};

        return result;
    } catch(const std::exception &e) {
        log("getBearerToken Error: " + string(e.what()), LogLevel::ERRORS);
        throw;  // Re-throw to allow retry logic to handle it
    }
}

json
getResponse(string host, string port, string bearerToken, string json_body,
            string target) {

    try {
        int version = 11;

        tcp::resolver resolver(http_ioc);
        beast::tcp_stream stream(http_ioc);

        auto const results = resolver.resolve(host, port);
        stream.connect(results);

        // Build HTTP POST request
        beast::http::request<beast::http::string_body> req{beast::http::verb::post,
                                                           target, version};
        req.set(beast::http::field::host, host);
        req.set(beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
        req.set(beast::http::field::content_type, "application/json");
        req.set(beast::http::field::authorization, "Bearer " + bearerToken);

        req.body() = json_body;
        req.prepare_payload();

        // Send request
        beast::http::write(stream, req);

        // ------------------------------
        // 🔥 IMPORTANT PART (BODY LIMIT)
        // ------------------------------
        beast::flat_buffer buffer;

        beast::http::response_parser<beast::http::dynamic_body> parser;
        parser.body_limit(100 * 1024 * 1024);  // 100 MB (adjust as needed)

        beast::http::read(stream, buffer, parser);

        beast::http::response<beast::http::dynamic_body> res = parser.release();
        // ------------------------------

        auto bodyStr = boost::beast::buffers_to_string(res.body().data());
        json result = json::parse(bodyStr);

        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);

        return result;

    } catch(const std::exception &e) {
        log("getResponse Error: " + string(e.what()), LogLevel::ERRORS);
        throw;
    }
}

vector<ServerInfoO>
ParseServerHierarchyFromJson(const json &response) {
    vector<ServerInfoO> servers;

    // Default global OrgId from root if available
    int globalOrgId = 1;
    if(response.contains("orgId") && response["orgId"].is_number()) {
        globalOrgId = response["orgId"].get<int>();
    }

    // Check for 'servers' (Simulated) or 'data' (API)
    if(response.contains("data") && response["data"].is_array()) {
        for(const auto &itemd : response["data"]) {
            if(itemd.contains("servers") && itemd["servers"].is_array()) {
                for(const auto &item : itemd["servers"]) {
                    ServerInfoO serverInfo;

                    // Safe parsing using .value() to avoid crashes if fields are missing
                    serverInfo.cfgName = item.value("cfgName", "");
                    serverInfo.endpointUrl = item.value("endpointUrl", "");
                    serverInfo.securityPolicy = item.value("securityPolicy", "NONE");
                    serverInfo.msgSecurityMode = item.value("msgSecurityMode", "NONE");
                    serverInfo.authType = item.value("authType", "AUTH_STG_ANYMS");
                    // Normalize authType
                    if(serverInfo.authType == "anonymus" || serverInfo.authType == "anonymous") {
                        serverInfo.authType = "AUTH_STG_ANYMS";
                    }
                    serverInfo.dataPointId = item.value("dataPointId", 0);
                    serverInfo.name = item.value("name", "");
                    serverInfo.nodeId = item.value("nodeId", "");
                    serverInfo.typeId = item.value("typeId", "");
                    serverInfo.parentId = item.value("parentId", "");

                    // Parse credentials
                    serverInfo.username = item.value("username", "");
                    serverInfo.password = item.value("userPwd", ""); // Map userPwd to password
                    serverInfo.certificate = item.value("certificate", "");
                    serverInfo.privateKey = item.value("privateKey", "");
                    serverInfo.sessionName = item.value("sessionName", "");

                    if(item.contains("groups") && item["groups"].is_array()) {
                        vector<GroupInfo> groups;
                        for(const auto &groupItem : item["groups"]) {
                            GroupInfo groupInfo;

                            groupInfo.dataPointId = groupItem.value("dataPointId", 0);
                            groupInfo.name = groupItem.value("name", "");
                            groupInfo.nodeId = groupItem.value("nodeId", "");
                            groupInfo.typeId = groupItem.value("typeId", "");
                            groupInfo.parentId = groupItem.value("parentId", "");
                            groupInfo.publishingInterval =
                                groupItem.value("publishingInterval", 0);
                            groupInfo.lifetimeCount =
                                groupItem.value("lifeTimeCount", 0);
                            groupInfo.maxKeepAliveCount =
                                groupItem.value("maxKeepAlive", 0);
                            groupInfo.priority = groupItem.value("priority", 0);
                            groupInfo.maxNotificationsPerPublish =
                                groupItem.value("maxNotificationsPublish", 0);

                            if(groupItem.contains("tags") &&
                               groupItem["tags"].is_array()) {
                                vector<TagInfo> groupTags;
                                for(const auto &tagItem : groupItem["tags"]) {
                                    TagInfo tagInfo;

                                    tagInfo.scaling = tagItem.value("scaling", false);
                                    tagInfo.rawMin = tagItem.value("rawMin", 0.0);
                                    tagInfo.rawMax = tagItem.value("rawMax", 0.0);
                                    tagInfo.scaleMin = tagItem.value("scaleMin", 0.0);
                                    tagInfo.scaleMax = tagItem.value("scaleMax", 0.0);
                                    tagInfo.enableExpression =
                                        tagItem.value("enableExpression", false);
                                    tagInfo.expression = tagItem.value("expression", "");
                                    tagInfo.dataPointId =
                                        tagItem.value("dataPointId", 0);
                                    tagInfo.name = tagItem.value("name", "");
                                    tagInfo.nodeId = tagItem.value("nodeId", "");
                                    tagInfo.typeId = tagItem.value("typeId", "");
                                    tagInfo.parentId = tagItem.value("parentId", "");
                                    tagInfo.samplingInterval =
                                        tagItem.value("samplingInterval", 0);
                                    tagInfo.deadband = tagItem.value("deadband", 0);
                                    tagInfo.queuesize = tagItem.value("queueSize", 0);
                                    tagInfo.rdWtOpt = tagItem.value("rdWtOpt", "");
                                    tagInfo.sourceDatatype =
                                        tagItem.value("sourceDatatype", "");

                                    // Extract the OPC UA namespace field from the tag
                                    string opcUaNamespace = "";
                                    if(tagItem.contains("namespace") &&
                                       tagItem["namespace"].is_string()) {
                                        tagInfo.namespaceNodeID =
                                            tagItem["namespace"].get<string>();
                                        opcUaNamespace =
                                            tagItem["namespace"].get<string>();
                                    };

                                    if(tagItem.contains("mappedInfospaceTags") &&
                                       tagItem["mappedInfospaceTags"].is_array()) {
                                        vector<MappedInfospaceTag> mappedTags;
                                        for(const auto &mappedTag :
                                            tagItem["mappedInfospaceTags"]) {
                                            MappedInfospaceTag mappedInfo;
                                            mappedInfo.id = mappedTag.value("id", 0);
                                            mappedInfo.tagId =
                                                mappedTag.value("tagId", 0);
                                            mappedInfo.name =
                                                mappedTag.value("name", "");
                                            mappedInfo.namespaces =
                                                mappedTag.value("namespace", "");

                                            mappedInfo.scaling = tagInfo.scaling;
                                            mappedInfo.rawMin = tagInfo.rawMin;
                                            mappedInfo.rawMax = tagInfo.rawMax;
                                            mappedInfo.scaleMin = tagInfo.scaleMin;
                                            mappedInfo.scaleMax = tagInfo.scaleMax;
                                            mappedInfo.sourceDatatype =
                                                tagInfo.sourceDatatype;
                                            mappedInfo.enableExpression =
                                                tagInfo.enableExpression;
                                            mappedInfo.expression = tagInfo.expression;
                                           
                                            mappedInfo.samplingInterval =
                                                mappedTag.value("samplingInterval", 0);
                                            mappedInfo.deadband =
                                                mappedTag.value("deadband", 0);
                                            mappedInfo.queuesize =
                                                mappedTag.value("queueSize", 0);

                                            // Extract orgId if available in the JSON, else use global
                                            if(mappedTag.contains("orgId")) {
                                                mappedInfo.orgId =
                                                    mappedTag["orgId"].get<int>();
                                            } else {
                                                mappedInfo.orgId = globalOrgId;
                                            }

                                            // CRITICAL FIX: Use the OPC UA namespace from
                                            // the tag, not the MQTT namespace from
                                            // mappedInfo opcUaNamespace contains the OPC
                                            // UA node path like "Node2/ns=3;i=1002"
                                            // mappedInfo.namespaces contains the MQTT
                                            // topic like "TDSPL/NNNN/TEST/TEST"
                                            pair<string, string> NodePair = {
                                                opcUaNamespace, serverInfo.endpointUrl};
                                            Mapping[mappedInfo.tagId] = NodePair;
                                           
                                            // Store MQTT topic mapping for alarm fallback
                                            TopicMapping[mappedInfo.tagId] =
                                                mappedInfo.namespaces;

                                            mappedTags.push_back(mappedInfo);
                                        }
                                        tagInfo.mappedInfospaceTags = mappedTags;
                                    }

                                    groupTags.push_back(tagInfo);
                                }
                                groupInfo.tags = groupTags;
                            }

                            groups.push_back(groupInfo);
                        }
                        serverInfo.groups = groups;
                    }
                    servers.push_back(serverInfo);
                }
            }
        }
    }
    // log("Mapping: ");
    // for(const auto &mapping : Mapping) {
    //     log(std::to_string(mapping.first) + " " + mapping.second.first + " " +
    //     mapping.second.second);
    // }
    // cout << endl;

    return servers;
}

vector<ServerInfoO>
ParseServerHierarchy(string host, string port, string bearerToken, string json_body,
                     string target) {
    if(!bearerToken.empty()) {
        auto futureResponse = std::async(std::launch::async, getResponse, host, port,
                                         bearerToken, json_body, target);
        json response = futureResponse.get();
        return ParseServerHierarchyFromJson(response);
    }
    return {};
}

vector<AlarmConfig>
ParseAlarmConfigFromJson(const json &response) {
    vector<AlarmConfig> alarms;
    try {

        // Check for 'data' field (actual API response format)
        if(response.contains("data") && response["data"].is_array()) {
            for(const auto &item : response["data"]) {
                try {
                    AlarmConfig alarmConfig;
                    alarmConfig.id = item.contains("id") ? item["id"].get<int>() : 0;
                    alarmConfig.name =
                        item.contains("name") ? item["name"].get<string>() : "";
                    alarmConfig.shortCode =
                        item.contains("shortCode") ? item["shortCode"].get<string>() : "";
                    alarmConfig.description = item.contains("description")
                                                  ? item["description"].get<string>()
                                                  : "";
                    alarmConfig.priority =
                        item.contains("priority") ? item["priority"].get<string>() : "";
                    alarmConfig.severityOffset = (item.contains("severityOffset") &&
                                                  !item["severityOffset"].is_null())
                                                     ? item["severityOffset"].get<int>()
                                                     : 0;
                    alarmConfig.category =
                        (item.contains("category") && !item["category"].is_null())
                            ? item["category"].get<string>()
                            : "";
                    alarmConfig.subCategory =
                        (item.contains("subCategory") && !item["subCategory"].is_null())
                            ? item["subCategory"].get<string>()
                            : "";
                    alarmConfig.alarmSourceType =
                        item.contains("alarmSourceType")
                            ? item["alarmSourceType"].get<string>()
                            : "";
                    alarmConfig.areaDeviceName =
                        item.contains("areaDeviceName")
                            ? item["areaDeviceName"].get<string>()
                            : "";
                    alarmConfig.message =
                        item.contains("message") ? item["message"].get<string>() : "";
                    // alarmConfig.state = item["state"].get<string>();
                    alarmConfig.sopProcedure = item.contains("sopProcedure")
                                                   ? item["sopProcedure"].get<string>()
                                                   : "";
                    alarmConfig.annunciation = item.contains("annunciation")
                                                   ? item["annunciation"].get<string>()
                                                   : "";
                    alarmConfig.ackType =
                        item.contains("ackType") ? item["ackType"].get<string>() : "";
                    alarmConfig.enable =
                        item.contains("enable") ? item["enable"].get<bool>() : false;

                    // API returns booleans, but struct stores as strings - check
                    // contains() first
                    if(item.contains("bedgeNotification")) {
                        alarmConfig.bedgeNotification =
                            item["bedgeNotification"].is_boolean()
                                ? item["bedgeNotification"].get<bool>()
                                : (item["bedgeNotification"].get<string>() == "true");
                    } else {
                        alarmConfig.bedgeNotification = false;
                    }
                    if(item.contains("smsNotification")) {
                        alarmConfig.smsNotification =
                            item["smsNotification"].is_boolean()
                                ? item["smsNotification"].get<bool>()
                                : (item["smsNotification"].get<string>() == "true");
                    } else {
                        alarmConfig.smsNotification = false;
                    }
                    if(item.contains("emailNotification")) {
                        alarmConfig.emailNotification =
                            item["emailNotification"].is_boolean()
                                ? item["emailNotification"].get<bool>()
                                : (item["emailNotification"].get<string>() == "true");
                    } else {
                        alarmConfig.emailNotification = false;
                    }
                    // alarmConfig.whatsapNotification =
                    // item["whatsapNotification"].get<string>();
                    alarmConfig.reset =
                        item.contains("reset") ? item["reset"].get<string>() : "";
                    alarmConfig.escalationWF = item.contains("escalationWF")
                                                   ? item["escalationWF"].get<bool>()
                                                   : false;
                    alarmConfig.suppression = item.contains("suppression")
                                                  ? item["suppression"].get<bool>()
                                                  : false;
                    // alarmConfig.enableLogging = item["enableLogging"].get<string>();
                    alarmConfig.loggingFreq = item.contains("loggingFreq")
                                                  ? item["loggingFreq"].get<string>()
                                                  : "";
                    alarmConfig.purging =
                        item.contains("purging") ? item["purging"].get<string>() : "";
                    alarmConfig.orgId =
                        item.contains("orgId") ? item["orgId"].get<int>() : 0;
                    // alarmConfig.removeIds = item["removeIds"].get<string>();

                    // API returns "alarmTriggers" as an OBJECT, not array
                    if(item.contains("alarmTriggers") &&
                       item["alarmTriggers"].is_object()) {
                        vector<AlarmTrigger> alarmTriggers;
                        const auto &triggerItem = item["alarmTriggers"];
                        try {
                            AlarmTrigger alarmTrigger;
                            // Check contains() FIRST to avoid assertion failures
                            alarmTrigger.id = (!triggerItem.contains("id") ||
                                               triggerItem["id"].is_null())
                                                  ? 0
                                                  : triggerItem["id"].get<int>();

                            alarmTrigger.tagId = (!triggerItem.contains("tagId") ||
                                                  triggerItem["tagId"].is_null())
                                                     ? 0
                                                     : triggerItem["tagId"].get<int>();

                            alarmTrigger.tagType =
                                (!triggerItem.contains("tagType") ||
                                 triggerItem["tagType"].is_null())
                                    ? ""
                                    : triggerItem["tagType"].get<string>();

                            // Handle potentially null double values - check contains()
                            // FIRST
                            alarmTrigger.hiHi = (!triggerItem.contains("hiHi") ||
                                                 triggerItem["hiHi"].is_null())
                                                    ? 0.0
                                                    : triggerItem["hiHi"].get<double>();
                            alarmTrigger.hi = (!triggerItem.contains("hi") ||
                                               triggerItem["hi"].is_null())
                                                  ? 0.0
                                                  : triggerItem["hi"].get<double>();
                            alarmTrigger.lo = (!triggerItem.contains("lo") ||
                                               triggerItem["lo"].is_null())
                                                  ? 0.0
                                                  : triggerItem["lo"].get<double>();
                            alarmTrigger.loLo = (!triggerItem.contains("loLo") ||
                                                 triggerItem["loLo"].is_null())
                                                    ? 0.0
                                                    : triggerItem["loLo"].get<double>();

                            // Handle potentially null or missing string/int values -
                            // check contains() FIRST
                            alarmTrigger.state = (!triggerItem.contains("state") ||
                                                  triggerItem["state"].is_null())
                                                     ? ""
                                                     : triggerItem["state"].get<string>();
                            alarmTrigger.activationDelay =
                                (!triggerItem.contains("activationDelay") ||
                                 triggerItem["activationDelay"].is_null())
                                    ? 0
                                    : triggerItem["activationDelay"].get<int>();

                            alarmTrigger.resetDelayInCounter =
                                (!triggerItem.contains("resetDelayInCounter") ||
                                 triggerItem["resetDelayInCounter"].is_null())
                                    ? 0
                                    : triggerItem["resetDelayInCounter"].get<int>();

                            alarmTrigger.evaluatedOn =
                                (!triggerItem.contains("evaluatedOn") ||
                                 triggerItem["evaluatedOn"].is_null())
                                    ? ""
                                    : triggerItem["evaluatedOn"].get<string>();
                            alarmTrigger.evaluationInterval =
                                (!triggerItem.contains("evaluationInterval") ||
                                 triggerItem["evaluationInterval"].is_null())
                                    ? ""
                                    : triggerItem["evaluationInterval"].get<string>();

                            alarmTrigger.triggerType =
                                (!triggerItem.contains("triggerType") ||
                                 triggerItem["triggerType"].is_null())
                                    ? ""
                                    : triggerItem["triggerType"].get<string>();

                            alarmTrigger.nameSpace =
                                (!triggerItem.contains("nameSpace") ||
                                 triggerItem["nameSpace"].is_null())
                                    ? ""
                                    : triggerItem["nameSpace"].get<string>();

                            alarmTrigger.topic = (!triggerItem.contains("topic") ||
                                                  triggerItem["topic"].is_null())
                                                     ? ""
                                                     : triggerItem["topic"].get<string>();

                            alarmTrigger.infoId = (!triggerItem.contains("infoId") ||
                                                   triggerItem["infoId"].is_null())
                                                      ? 0
                                                      : triggerItem["infoId"].get<int>();

                            alarmTriggers.push_back(alarmTrigger);
                        } catch(const std::exception &e) {
                            log("Error parsing alarm trigger: " + std::string(e.what()),
                                LogLevel::ERRORS);
                        }
                        alarmConfig.alarmTriggers = alarmTriggers;
                    }

                    if(item.contains("alarmEmitter") && item["alarmEmitter"].is_array()) {
                        vector<AlarmEmitter> alarmEmitters;
                        for(const auto &emitterItem : item["alarmEmitter"]) {
                            try {
                                AlarmEmitter alarmEmitter;
                                // Check contains() FIRST to avoid assertion failures
                                alarmEmitter.id = (!emitterItem.contains("id") ||
                                                   emitterItem["id"].is_null())
                                                      ? 0
                                                      : emitterItem["id"].get<int>();
                                alarmEmitter.alarmShortcode =
                                    (!emitterItem.contains("alarmShortcode") ||
                                     emitterItem["alarmShortcode"].is_null())
                                        ? ""
                                        : emitterItem["alarmShortcode"].get<string>();
                                alarmEmitter.emitterNode =
                                    (!emitterItem.contains("emitterNode") ||
                                     emitterItem["emitterNode"].is_null())
                                        ? 0
                                        : emitterItem["emitterNode"].get<int>();
                                alarmEmitter.emitterNodeName =
                                    (!emitterItem.contains("emitterNodeName") ||
                                     emitterItem["emitterNodeName"].is_null())
                                        ? ""
                                        : emitterItem["emitterNodeName"].get<string>();
                                alarmEmitter.alarmTagNameSpace =
                                    (!emitterItem.contains("alarmTagNameSpace") ||
                                     emitterItem["alarmTagNameSpace"].is_null())
                                        ? ""
                                        : emitterItem["alarmTagNameSpace"].get<string>();
                                alarmEmitter.orgId =
                                    (!emitterItem.contains("orgId") ||
                                     emitterItem["orgId"].is_null())
                                        ? 0
                                        : emitterItem["orgId"].get<int>();
                                alarmEmitters.push_back(alarmEmitter);
                            } catch(const std::exception &e) {
                                log("Error parsing alarm emitter: " +
                                        std::string(e.what()),
                                    LogLevel::ERRORS);
                                // Skip this emitter and continue with the next
                                continue;
                            }
                        }
                        alarmConfig.alarmEmitters = alarmEmitters;
                    } else {
                        log("Alarm '" + alarmConfig.name + "' has no emitters",
                            LogLevel::ERRORS);
                    }
                    alarms.push_back(alarmConfig);
                } catch(const std::exception &e) {
                    log("Error parsing alarm config item: " + std::string(e.what()),
                        LogLevel::ERRORS);
                    // Skip this alarm and continue with the next
                    continue;
                }
            }
        }
        // else if(response.contains("alarmConfigs") &&
        // response["alarmConfigs"].is_array()) {
        //     // Fallback for old API format
        //     log("Found 'alarmConfigs' field (old format) with " +
        //     std::to_string(response["alarmConfigs"].size()) + " alarm(s)",
        //     LogLevel::INFO);
        // }
        else {
            log("ERROR: No 'data' or 'alarmConfigs' field found in API response!",
                LogLevel::ERRORS);
            // log("Response keys: " + response.dump(), LogLevel::DEBUG);
        }
    } catch(const std::exception &e) {
        log("Error in ParseAlarmConfig: " + std::string(e.what()), LogLevel::ERRORS);
        // Return empty vector on error rather than crashing
    }
    return alarms;
}

vector<AlarmConfig>
ParseAlarmConfig(string host, string port, string bearerToken, string json_body,
                 string target) {
    if(bearerToken.empty())
        return {};
    try {
        auto futureResponse = std::async(std::launch::async, getResponse, host, port,
                                         bearerToken, json_body, target);
        json response = futureResponse.get();
        return ParseAlarmConfigFromJson(response);
    } catch(const std::exception &e) {
        log("Error in ParseAlarmConfig: " + std::string(e.what()), LogLevel::ERRORS);
        return {};
    }
}

vector<OrgConfig>
ParseOrgConfig(string host, string port, string bearerToken, string json_body,
               string target) {
    vector<OrgConfig> orgConfig;

    if(!bearerToken.empty()) {
        try {
            auto futureResponse = std::async(std::launch::async, getResponse, host, port,
                                             bearerToken, json_body, target);
            json response = futureResponse.get();

            // Check for 'data' field (actual API response format)
            if(response.contains("data") && response["data"].is_array()) {
                for(const auto &item : response["data"]) {
                    try {
                        OrgConfig orgs;
                        orgs.id = item.contains("id") ? item["id"].get<int>() : 0;
                        orgs.name =
                            item.contains("name") ? item["name"].get<string>() : "";
                        orgs.shortCode = item.contains("shortCode")
                                             ? item["shortCode"].get<string>()
                                             : "";
                        orgs.emailPrimary = item.contains("emailPrimary")
                                                ? item["emailPrimary"].get<string>()
                                                : "";
                        orgs.emailSecondary = item.contains("emailSecondary")
                                                  ? item["emailSecondary"].get<string>()
                                                  : "";
                        orgs.contactNoPrimary =
                            item.contains("contactNoPrimary")
                                ? item["contactNoPrimary"].get<string>()
                                : "";
                        orgs.contactNoSecondary =
                            item.contains("contactNoSecondary")
                                ? item["contactNoSecondary"].get<string>()
                                : "";
                        orgs.remark =
                            item.contains("remark") ? item["remark"].get<string>() : "";
                        orgs.displayName = item.contains("displayName")
                                               ? item["displayName"].get<string>()
                                               : "";
                        orgs.organisationType =
                            item.contains("organisationType")
                                ? item["organisationType"].get<string>()
                                : "";
                        orgs.tenancyType = item.contains("tenancyType")
                                               ? item["tenancyType"].get<string>()
                                               : "";
                        orgs.profileId =
                            item.contains("profileId") ? item["profileId"].get<int>() : 0;
                        orgs.orgId =
                            item.contains("orgId") ? item["orgId"].get<int>() : 0;
                        orgs.isCopyProfile = item.contains("isCopyProfile")
                                                 ? item["isCopyProfile"].get<bool>()
                                                 : false;

                        orgConfig.push_back(orgs);
                    } catch(const std::exception &e) {
                        log("Error parsing org config item: " + std::string(e.what()),
                            LogLevel::ERRORS);
                        // Skip this alarm and continue with the next
                        continue;
                    }
                }
            } else {
                log("ERROR: No 'data' or 'orgs' field found in API response!",
                    LogLevel::ERRORS);
                // log("Response keys: " + response.dump(), LogLevel::DEBUG);
            }
        } catch(const std::exception &e) {
            log("Error in ParseAlarmConfig: " + std::string(e.what()), LogLevel::ERRORS);
            // Return empty vector on error rather than crashing
        }
    }
    return orgConfig;
}

UserProfile
ParseUserProfileFromJson(const json &response) {
    UserProfile profile;  // Default empty profile

    // Check for 'data' field (actual API response format)
    if(response.contains("data") && response["data"].is_array() &&
       !response["data"].empty()) {
        const auto &item = response["data"][0];  // Get first user profile

        try {
            profile.userId = item.value("userId", 0);
            profile.displayName = item.value("displayName", "");

            // Helper lambda to convert number or string to string
            auto getAsString = [&item](const std::string &key) -> std::string {
                if(!item.contains(key))
                    return "";
                if(item[key].is_number())
                    return std::to_string(item[key].get<int>());
                if(item[key].is_string())
                    return item[key].get<std::string>();
                return "";
            };

            profile.districtId = getAsString("districtId");
            profile.stateId = getAsString("stateId");
            profile.countryId = getAsString("countryId");
            profile.emailId = item.value("emailId", "");
            profile.defaultLangaugeId = getAsString("defaultLangaugeId");
            profile.defaultLangauge = item.value("defaultLangauge", "");
            profile.defaultLangaugeCode = item.value("defaultLangaugeCode", "");
            profile.currentLangaugeId = getAsString("currentLangaugeId");
            profile.currentLangauge = item.value("currentLangauge", "");
            profile.currentLangaugeCode = item.value("currentLangaugeCode", "");
            profile.defaultOrgId = getAsString("defaultOrgId");
            profile.defaultOrgName = item.value("defaultOrgName", "");
            profile.currentOrgId = getAsString("currentOrgId");
            profile.currentOrgName = item.value("currentOrgName", "");
            profile.currentOrgProfileId = getAsString("currentOrgProfileId");
            profile.currentOrgProductName =
                item.value("currentOrgProductName", "");                // String
            profile.currentOrgCode = item.value("currentOrgCode", "");  // String
            profile.currentOrganisationType =
                item.value("currentOrganisationType", "");              // String
            profile.currentOrgLogo = item.value("currentOrgLogo", "");  // String

            log("✓ Parsed user profile: " + profile.displayName +
                    " (OrgID: " + profile.currentOrgId + ")",
                LogLevel::DEBUG);
        } catch(const std::exception &e) {
            log("Error parsing user profile item: " + std::string(e.what()),
                LogLevel::ERRORS);
        }
    } else {
        log("ERROR: No 'data' field found in user profile response or data is empty!",
            LogLevel::ERRORS);
    }
    return profile;
}

UserProfile
ParseUserProfile(string host, string port, string bearerToken, string json_body,
                 string target) {
    if(!bearerToken.empty()) {
        try {
            auto futureResponse = std::async(std::launch::async, getResponse, host, port,
                                             bearerToken, json_body, target);
            json response = futureResponse.get();
            return ParseUserProfileFromJson(response);

        } catch(const std::exception &e) {
            log("Error in ParseUserProfile: " + std::string(e.what()), LogLevel::ERRORS);
        }
    }
    return UserProfile();
}

// Assumes `json` is nlohmann::json and types ServerConfig, orgMappings, log, LogLevel
// exist.

ServerConfig
ServerConfigFromJSON(const json &item) {
    ServerConfig serverConfig;
    // Defaults
    serverConfig.id = 0;
    serverConfig.dataPointId = 0;
    serverConfig.port = 0;

    try {
        // Use value() which returns default if key missing or not convertible
        serverConfig.id = item.value("id", 0);
        serverConfig.name = item.value("name", std::string{});
        serverConfig.ip = item.value("ip", std::string{});
        serverConfig.port = item.value("port", 0);
        serverConfig.nodeId = item.value("nodeId", std::string{});

        // Handle orgMappings whether it's an object (single) or array (multiple)
        if(item.contains("orgMappings")) {
            const auto &om = item["orgMappings"];

            if(om.is_array()) {
                for(const auto &entry : om) {
                    try {
                        orgMappings mapping;
                        mapping.id = entry.value("id", 0);
                        mapping.hierarchyId = entry.value("hierarchyId", 0);
                        mapping.mapOrgId = entry.value("mapOrgId", 0);
                        mapping.orgShortCode = entry.value("orgShortCode", std::string{});
                        serverConfig.orgMappings.push_back(std::move(mapping));
                        log("DEBUG: Parsed OrgMapping - ShortCode: " +
                                serverConfig.orgMappings.back().orgShortCode + ", ID: " +
                                std::to_string(serverConfig.orgMappings.back().id) +
                                ", MapOrgID: " +
                                std::to_string(serverConfig.orgMappings.back().mapOrgId),
                            LogLevel::INFO);
                    } catch(const std::exception &e) {
                        log(std::string("Error parsing orgMappings array entry: ") +
                                e.what(),
                            LogLevel::ERRORS);
                    }
                }
            } else if(om.is_object()) {
                try {
                    orgMappings mapping;
                    mapping.id = om.value("id", 0);
                    mapping.hierarchyId = om.value("hierarchyId", 0);
                    mapping.mapOrgId = om.value("mapOrgId", 0);
                    mapping.orgShortCode = om.value("orgShortCode", std::string{});
                    serverConfig.orgMappings.push_back(std::move(mapping));
                    log("DEBUG: Parsed OrgMapping - ShortCode: " +
                            serverConfig.orgMappings.back().orgShortCode + ", ID: " +
                            std::to_string(serverConfig.orgMappings.back().id) +
                            ", MapOrgID: " +
                            std::to_string(serverConfig.orgMappings.back().mapOrgId),
                        LogLevel::INFO);
                } catch(const std::exception &e) {
                    log(std::string("Error parsing orgMappings object: ") + e.what(),
                        LogLevel::ERRORS);
                }
            } else {
                log("orgMappings present but not array/object; ignoring", LogLevel::INFO);
            }
        }
    } catch(const std::exception &e) {
        log(std::string("Error in SevrverConfigFromJSON: ") + e.what(), LogLevel::ERRORS);
    }
    return serverConfig;
}

std::vector<ServerConfig>
ParseServerConfig(const std::string &host, const std::string &port,
                  const std::string &bearerToken, const std::string &json_body,
                  const std::string &target) {
    std::vector<ServerConfig> serverConfigs;

    if(bearerToken.empty()) {
        log("ParseServerConfig: empty bearerToken; returning empty list", LogLevel::INFO);
        return serverConfigs;
    }

    try {
        auto futureResponse = std::async(std::launch::async, getResponse, host, port,
                                         bearerToken, json_body, target);
        json response = futureResponse.get();

        if(!response.contains("data") || !response["data"].is_array() ||
           response["data"].empty()) {
            log("ERROR: No 'data' field found in server config response or data is "
                "empty!",
                LogLevel::ERRORS);
            return serverConfigs;
        }

        for(const auto &item : response["data"]) {
            serverConfigs.push_back(ServerConfigFromJSON(item));
        }

    } catch(const std::exception &e) {
        log(std::string("Error in ParseServerConfig: ") + e.what(), LogLevel::ERRORS);
    }

    return serverConfigs;
}

std::pair<int, std::string>
extractNsAndValue(const std::string &input) {
    std::size_t nsPos = input.find("ns=");
    std::size_t semiPos = input.find(';');
    std::size_t equalPos = input.find('=', semiPos);  // '=' after the semicolon

    if(nsPos == std::string::npos || semiPos == std::string::npos ||
       equalPos == std::string::npos) {
        return {0, ""};
    }

    int ns = std::stoi(
        input.substr(nsPos + 3, semiPos - (nsPos + 3)));  // Extract between "ns=" and ';'
    std::string value = input.substr(equalPos + 1);       // Extract after '='

    return {ns, value};
}
