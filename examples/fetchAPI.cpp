#include <async_mqtt/all.hpp>
#include <async_mqtt/asio_bind/predefined_layer/mqtts.hpp>
#include <async_mqtt/asio_bind/predefined_layer/ws.hpp> 
#include <async_mqtt/asio_bind/predefined_layer/wss.hpp>
#include <boost/asio.hpp>

#include <open62541/plugin/log_stdout.h>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>


#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <unordered_map>


#include <nlohmann/json.hpp>

#include "structs.h"
#include "Logger.h"
#include "AlarmConfig.h"
#include "OrgConfig.h"
#include "UserProfile.h"
#include "ServerConfig.h"


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




json getBearerToken(string host, string port , string username, string password) {
    try {
        std::string target = "/api/SignIn";
        int version = 11;

        // JSON body built from provided credentials
        //json jbody;
        //jbody["Username"] = username;
        //jbody["password"] = password;
        //std::string json_body = jbody.dump();
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
        beast::http::request<beast::http::string_body> req{beast::http::verb::post, target, version};
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
        if (ec && ec != beast::errc::not_connected)
            throw beast::system_error{ec};

        return result;
    } catch (const std::exception &e) {
        log("getBearerToken Error: " + string(e.what()),LogLevel::ERRORS);
        throw; // Re-throw to allow retry logic to handle it
    }
}



json getHierarchy(string host , string port , string bearerToken , string json_body , string target) {

try{
    // std::string host = host;
    // std::string port = port;
    int version = 11;

    // Set up I/O context and connection
    tcp::resolver resolver(http_ioc);
    beast::tcp_stream stream(http_ioc);

    // Resolve and connect
    auto const results = resolver.resolve(host, port);
    stream.connect(results);

    // Build HTTP POST request
    beast::http::request<beast::http::string_body> req{beast::http::verb::post, target, version};
    req.set(beast::http::field::host, host);
    req.set(beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(beast::http::field::content_type, "application/json");

    // Set Bearer Authorization header
    req.set(beast::http::field::authorization, "Bearer " + bearerToken);

    req.body() = json_body;
    req.prepare_payload();

    // Send request
    beast::http::write(stream, req);

    // Get response
    beast::flat_buffer buffer;
    beast::http::response<beast::http::string_body> res;
    beast::http::read(stream, buffer, res);

    
    // Output response

    // log(res.body().c_str(),LogLevel::DEBUG);
    json result = json::parse(res.body());
    // Shutdown connection
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);

    return result;

    } catch (const std::exception &e) {
    log("getHierarchy Error: " + string(e.what()),LogLevel::ERRORS);
        throw; // Re-throw to allow retry logic to handle it
    }
    
}


json
GetAllOrganizationList(string host, string port, string bearerToken, string json_body,
             string target) {

    try {
        int version = 11;

        // Set up I/O context and connection
        tcp::resolver resolver(http_ioc);
        beast::tcp_stream stream(http_ioc);

        // Resolve and connect
        auto const results = resolver.resolve(host, port);
        stream.connect(results);

        // Build HTTP POST request
        beast::http::request<beast::http::string_body> req{beast::http::verb::post,
                                                           target, version};
        req.set(beast::http::field::host, host);
        req.set(beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
        req.set(beast::http::field::content_type, "application/json");

        // Set Bearer Authorization header
        req.set(beast::http::field::authorization, "Bearer " + bearerToken);

        req.body() = json_body;
        req.prepare_payload();

        // Send request
        beast::http::write(stream, req);

        // Get response
        beast::flat_buffer buffer;
        beast::http::response<beast::http::string_body> res;
        beast::http::read(stream, buffer, res);

        // Output response

        // log(res.body().c_str(), LogLevel::DEBUG);
        json result = json::parse(res.body());
        // Shutdown connection
        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);

        return result;

    } catch(const std::exception &e) {
        log("GetAllOrganizationList Error: " + string(e.what()), LogLevel::ERRORS);
        throw;  // Re-throw to allow retry logic to handle it
    }
}


json
GetUserProfile(string host, string port, string bearerToken, string json_body,
             string target) {

    try {
        int version = 11;

        // Set up I/O context and connection
        tcp::resolver resolver(http_ioc);
        beast::tcp_stream stream(http_ioc);

        // Resolve and connect
        auto const results = resolver.resolve(host, port);
        stream.connect(results);

        // Build HTTP POST request
        beast::http::request<beast::http::string_body> req{beast::http::verb::post,
                                                           target, version};
        req.set(beast::http::field::host, host);
        req.set(beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
        req.set(beast::http::field::content_type, "application/json");

        // Set Bearer Authorization header
        req.set(beast::http::field::authorization, "Bearer " + bearerToken);

        req.body() = json_body;
        req.prepare_payload();

        // Send request
        beast::http::write(stream, req);

        // Get response
        beast::flat_buffer buffer;
        beast::http::response<beast::http::string_body> res;
        beast::http::read(stream, buffer, res);

        // Output response

        // log(res.body().c_str(), LogLevel::DEBUG);
        json result = json::parse(res.body());
        // Shutdown connection
        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);

        return result;

    } catch(const std::exception &e) {
        log("GetUserProfile Error: " + string(e.what()), LogLevel::ERRORS);
        throw;  // Re-throw to allow retry logic to handle it
    }
}

vector<ServerInfoO> ParseServerHierarchy(string host, string port, string bearerToken, string json_body, string target) {

    vector<ServerInfoO> servers;
    if(!bearerToken.empty()) {
        auto futureResponse = std::async(std::launch::async, getHierarchy, host, port,bearerToken, json_body, target);
        json response = futureResponse.get();

        if(response.contains("servers") && response["servers"].is_array()) {
            for(const auto &item : response["servers"]) {
                ServerInfoO serverInfo;

                serverInfo.cfgName = item["cfgName"].get<string>();
                serverInfo.endpointUrl = item["endpointUrl"].get<string>();
                serverInfo.securityPolicy = item["securityPolicy"].get<string>();
                serverInfo.msgSecurityMode = item["msgSecurityMode"].get<string>();
                serverInfo.authType = item["authType"].get<string>();
                serverInfo.dataPointId = item["dataPointId"].get<int>();
                serverInfo.name = item["name"].get<string>();
                serverInfo.nodeId = item["nodeId"].get<string>();
                serverInfo.typeId = item["typeId"].get<string>();
                serverInfo.parentId = item["parentId"].get<string>();

                if(item.contains("groups") && item["groups"].is_array()) {
                    vector<GroupInfo> groups;
                    for(const auto &groupItem : item["groups"]) {
                        GroupInfo groupInfo;

                        groupInfo.dataPointId = groupItem["dataPointId"].get<int>();
                        groupInfo.name = groupItem["name"].get<string>();
                        groupInfo.nodeId = groupItem["nodeId"].get<string>();
                        groupInfo.typeId = groupItem["typeId"].get<string>();
                        groupInfo.parentId = groupItem["parentId"].get<string>();
                        groupInfo.publishingInterval = groupItem["publishingInterval"].get<int>();
                        groupInfo.lifetimeCount = groupItem["lifeTimeCount"].get<int>();
                        groupInfo.maxKeepAliveCount = groupItem["maxKeepAlive"].get<int>();
                        groupInfo.priority = groupItem["priority"].get<int>();
                        groupInfo.maxNotificationsPerPublish = groupItem["maxNotificationsPublish"].get<int>();

                        if(groupItem.contains("tags") && groupItem["tags"].is_array()) {
                            vector<TagInfo> groupTags;
                            for(const auto &tagItem : groupItem["tags"]) {
                                TagInfo tagInfo;

                                tagInfo.scaling = tagItem["scaling"].get<bool>();
                                tagInfo.rawMin = tagItem["rawMin"].get<double>();
                                tagInfo.rawMax = tagItem["rawMax"].get<double>();
                                tagInfo.scaleMin = tagItem["scaleMin"].get<double>();
                                tagInfo.scaleMax = tagItem["scaleMax"].get<double>();
                                tagInfo.enableExpression =
                                    tagItem["enableExpression"].get<bool>();
                                tagInfo.expression = tagItem["expression"].get<string>();
                                tagInfo.dataPointId = tagItem["dataPointId"].get<int>();
                                tagInfo.name = tagItem["name"].get<string>();
                                tagInfo.nodeId = tagItem["nodeId"].get<string>();
                                tagInfo.typeId = tagItem["typeId"].get<string>();
                                tagInfo.parentId = tagItem["parentId"].get<string>();
                                tagInfo.samplingInterval = tagItem["samplingInterval"].get<int>();
                                tagInfo.deadband = tagItem["deadband"].get<int>();
                                tagInfo.queuesize = tagItem["queueSize"].get<int>();
                                tagInfo.rdWtOpt = tagItem["rdWtOpt"].get<string>();

                                // Extract the OPC UA namespace field from the tag
                                string opcUaNamespace = "";
                                if(tagItem.contains("namespace") && tagItem["namespace"].is_string()) {
                                    opcUaNamespace = tagItem["namespace"].get<string>();
                                }

                                if(tagItem.contains("mappedInfospaceTags") &&
                                   tagItem["mappedInfospaceTags"].is_array()) {
                                    vector<MappedInfospaceTag> mappedTags;
                                    for(const auto &mappedTag :
                                        tagItem["mappedInfospaceTags"]) {
                                        MappedInfospaceTag mappedInfo;
                                        mappedInfo.id = mappedTag["id"].get<int>();
                                        mappedInfo.tagId = mappedTag["tagId"].get<int>();
                                        mappedInfo.name = mappedTag["name"].get<string>();
                                        mappedInfo.namespaces =
                                            mappedTag["namespace"].get<string>();


                                        mappedInfo.scaling = tagInfo.scaling;
                                        mappedInfo.rawMin = tagInfo.rawMin;
                                        mappedInfo.rawMax = tagInfo.rawMax;
                                        mappedInfo.scaleMin = tagInfo.scaleMin;
                                        mappedInfo.scaleMax = tagInfo.scaleMax;
                                        mappedInfo.sourceDatatype = tagInfo.sourceDatatype;
                                        mappedInfo.enableExpression = tagInfo.enableExpression;
                                        mappedInfo.expression = tagInfo.expression;
                                        //mappedInfo.isSimulationProfile =
                                        //    mappedTag["isSimulationProfile"].get<bool>();
                                        //mappedInfo.isLogging =
                                        //    mappedTag["isLogging"].get<bool>();
                                        //mappedInfo.isVirtual =
                                        //    mappedTag["isVirtual"].get<bool>();
                                        mappedInfo.samplingInterval = mappedTag["samplingInterval"].get<int>();
                                        mappedInfo.deadband = mappedTag["deadband"].get<int>();
                                        mappedInfo.queuesize = mappedTag["queueSize"].get<int>();

                                        // Extract orgId if available in the JSON
                                        if(mappedTag.contains("orgId")) {
                                            mappedInfo.orgId = mappedTag["orgId"].get<int>();
                                        } else {
                                            mappedInfo.orgId = 1; // Default fallback
                                        }

                                        // CRITICAL FIX: Use the OPC UA namespace from the tag, not the MQTT namespace from mappedInfo
                                        // opcUaNamespace contains the OPC UA node path like "Node2/ns=3;i=1002"
                                        // mappedInfo.namespaces contains the MQTT topic like "TDSPL/NNNN/TEST/TEST"
                                        pair<string, string> NodePair = {
                                            opcUaNamespace, serverInfo.endpointUrl};
                                        Mapping[mappedInfo.tagId] = NodePair;
                                        // Store MQTT topic mapping for alarm fallback
                                        TopicMapping[mappedInfo.tagId] = mappedInfo.namespaces;

                                        // }
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

    log("Mapping: ");
    for(const auto &mapping : Mapping) {
        log(std::to_string(mapping.first) + " " + mapping.second.first + " " + mapping.second.second);
    }
    cout << endl;

    return servers;
}













vector<AlarmConfig> ParseAlarmConfig(string host, string port, string bearerToken, string json_body, string target) {
    vector<AlarmConfig> alarms;
    if(!bearerToken.empty()) {
        try {
        auto futureResponse = std::async(std::launch::async, getHierarchy, host, port,bearerToken, json_body, target);
        json response = futureResponse.get();

        // Check for 'data' field (actual API response format)
        if(response.contains("data") && response["data"].is_array()) {
            for(const auto &item : response["data"]) {
                try {
                AlarmConfig alarmConfig;
                alarmConfig.id = item.contains("id") ? item["id"].get<int>() : 0;
                alarmConfig.name = item.contains("name") ? item["name"].get<string>() : "";
                alarmConfig.shortCode = item.contains("shortCode") ? item["shortCode"].get<string>() : "";
                alarmConfig.description = item.contains("description") ? item["description"].get<string>() : "";
                alarmConfig.priority = item.contains("priority") ? item["priority"].get<string>() : "";
                alarmConfig.severityOffset = (item.contains("severityOffset") && !item["severityOffset"].is_null())
                    ? item["severityOffset"].get<int>() : 0;
                alarmConfig.category = (item.contains("category") && !item["category"].is_null())
                    ? item["category"].get<string>() : "";
                alarmConfig.subCategory = (item.contains("subCategory") && !item["subCategory"].is_null())
                    ? item["subCategory"].get<string>() : "";
                alarmConfig.alarmSourceType = item.contains("alarmSourceType") ? item["alarmSourceType"].get<string>() : "";
                alarmConfig.areaDeviceName = item.contains("areaDeviceName") ? item["areaDeviceName"].get<string>() : "";
                alarmConfig.message = item.contains("message") ? item["message"].get<string>() : "";
                // alarmConfig.state = item["state"].get<string>();
                alarmConfig.sopProcedure = item.contains("sopProcedure") ? item["sopProcedure"].get<string>() : "";
                alarmConfig.annunciation = item.contains("annunciation") ? item["annunciation"].get<string>() : "";
                alarmConfig.ackType = item.contains("ackType") ? item["ackType"].get<string>() : "";
                alarmConfig.enable = item.contains("enable") ? item["enable"].get<string>() : "";

                
                // API returns booleans, but struct stores as strings - check contains() first
                if(item.contains("bedgeNotification")) {
                    alarmConfig.bedgeNotification = item["bedgeNotification"].is_boolean() 
                        ? (item["bedgeNotification"].get<bool>() ? "true" : "false")
                        : item["bedgeNotification"].get<string>();
                } else {
                    alarmConfig.bedgeNotification = "false";
                }
                if(item.contains("smsNotification")) {
                    alarmConfig.smsNotification = item["smsNotification"].is_boolean()
                        ? (item["smsNotification"].get<bool>() ? "true" : "false")
                        : item["smsNotification"].get<string>();
                } else {
                    alarmConfig.smsNotification = "false";
                }
                if(item.contains("emailNotification")) {
                    alarmConfig.emailNotification = item["emailNotification"].is_boolean()
                        ? (item["emailNotification"].get<bool>() ? "true" : "false")
                        : item["emailNotification"].get<string>();
                } else {
                    alarmConfig.emailNotification = "false";
                }
                // alarmConfig.whatsapNotification = item["whatsapNotification"].get<string>();
                alarmConfig.reset = item.contains("reset") ? item["reset"].get<string>() : "";
                alarmConfig.escalationWF = item.contains("escalationWF") ? item["escalationWF"].get<string>() : "";
                alarmConfig.suppression = item.contains("suppression") ? item["suppression"].get<string>() : "";
                // alarmConfig.enableLogging = item["enableLogging"].get<string>();
                alarmConfig.loggingFreq = item.contains("loggingFreq") ? item["loggingFreq"].get<string>() : "";
                alarmConfig.purging = item.contains("purging") ? item["purging"].get<string>() : "";
                alarmConfig.orgId = item.contains("orgId") ? item["orgId"].get<int>() : 0;
                // alarmConfig.removeIds = item["removeIds"].get<string>();
                
                // API returns "alarmTriggers" as an OBJECT, not array
                if(item.contains("alarmTriggers") && item["alarmTriggers"].is_object()) {
                    vector<AlarmTrigger> alarmTriggers;
                    const auto &triggerItem = item["alarmTriggers"];
                    try {
                        AlarmTrigger alarmTrigger;
                        // Check contains() FIRST to avoid assertion failures
                        alarmTrigger.id = (!triggerItem.contains("id") || triggerItem["id"].is_null()) 
                            ? 0 : triggerItem["id"].get<int>();
                        // alarmTrigger.alarmShortCode = triggerItem["alarmShortCode"].get<string>();
                        alarmTrigger.applicableTagId = (!triggerItem.contains("applicableTagId") || triggerItem["applicableTagId"].is_null()) 
                            ? "" : triggerItem["applicableTagId"].get<string>();
                        alarmTrigger.applicableTagName = (!triggerItem.contains("applicableTagName") || triggerItem["applicableTagName"].is_null()) 
                            ? "" : triggerItem["applicableTagName"].get<string>();
                        alarmTrigger.tagType = (!triggerItem.contains("tagType") || triggerItem["tagType"].is_null()) 
                            ? "" : triggerItem["tagType"].get<string>();
                        
                        // Handle potentially null double values - check contains() FIRST
                        alarmTrigger.hiHi = (!triggerItem.contains("hiHi") || triggerItem["hiHi"].is_null()) 
                            ? 0.0 : triggerItem["hiHi"].get<double>();
                        alarmTrigger.hi = (!triggerItem.contains("hi") || triggerItem["hi"].is_null()) 
                            ? 0.0 : triggerItem["hi"].get<double>();
                        alarmTrigger.lo = (!triggerItem.contains("lo") || triggerItem["lo"].is_null()) 
                            ? 0.0 : triggerItem["lo"].get<double>();
                        alarmTrigger.loLo = (!triggerItem.contains("loLo") || triggerItem["loLo"].is_null()) 
                            ? 0.0 : triggerItem["loLo"].get<double>();
                        
                        // Handle potentially null or missing string/int values - check contains() FIRST
                        alarmTrigger.state = (!triggerItem.contains("state") || triggerItem["state"].is_null()) 
                            ? "" : triggerItem["state"].get<string>();
                        alarmTrigger.activationDelay = (!triggerItem.contains("activationDelay") || triggerItem["activationDelay"].is_null()) 
                            ? 0 : triggerItem["activationDelay"].get<int>();
                        alarmTrigger.hysteresisOrResetDelay = (!triggerItem.contains("hysteresisOrResetDelay") || triggerItem["hysteresisOrResetDelay"].is_null()) 
                            ? 0 : triggerItem["hysteresisOrResetDelay"].get<int>();
                        alarmTrigger.evaluatedOn = (!triggerItem.contains("evaluatedOn") || triggerItem["evaluatedOn"].is_null()) 
                            ? "" : triggerItem["evaluatedOn"].get<string>();
                        alarmTrigger.evaluationInterval = (!triggerItem.contains("evaluationInterval") || triggerItem["evaluationInterval"].is_null()) 
                            ? "" : triggerItem["evaluationInterval"].get<string>();
                        // alarmTrigger.activationType = triggerItem["activationType"].get<string>();
                        // alarmTrigger.distance = triggerItem["distance"].get<double>();
                        
                        // Handle 'value' field - can be missing, null, empty string, or number
                        if(triggerItem.contains("value") && !triggerItem["value"].is_null()) {
                            if(triggerItem["value"].is_string()) {
                                std::string valStr = triggerItem["value"].get<std::string>();
                                if(valStr.empty()) {
                                    alarmTrigger.value = 0.0;
                                } else {
                                    try {
                                        alarmTrigger.value = std::stod(valStr);
                                    } catch(...) {
                                        alarmTrigger.value = 0.0;
                                    }
                                }
                            } else if(triggerItem["value"].is_number()) {
                                alarmTrigger.value = triggerItem["value"].get<double>();
                            } else {
                                alarmTrigger.value = 0.0;
                            }
                        } else {
                            alarmTrigger.value = 0.0;
                        }
                        alarmTriggers.push_back(alarmTrigger);
                    } catch(const std::exception &e) {
                        log("Error parsing alarm trigger: " + std::string(e.what()), LogLevel::ERRORS);
                    }
                    alarmConfig.alarmTriggers = alarmTriggers;
                }
                
                if(item.contains("alarmEmitter") && item["alarmEmitter"].is_array()) {
                    vector<AlarmEmitter> alarmEmitters;
                    for(const auto &emitterItem : item["alarmEmitter"]) {
                        try {
                        AlarmEmitter alarmEmitter;
                        // Check contains() FIRST to avoid assertion failures
                        alarmEmitter.id = (!emitterItem.contains("id") || emitterItem["id"].is_null()) 
                            ? 0 : emitterItem["id"].get<int>();
                        alarmEmitter.alarmShortcode = (!emitterItem.contains("alarmShortcode") || emitterItem["alarmShortcode"].is_null()) 
                            ? "" : emitterItem["alarmShortcode"].get<string>();
                        alarmEmitter.emitterNode = (!emitterItem.contains("emitterNode") || emitterItem["emitterNode"].is_null()) 
                            ? 0 : emitterItem["emitterNode"].get<int>();
                        alarmEmitter.emitterNodeName = (!emitterItem.contains("emitterNodeName") || emitterItem["emitterNodeName"].is_null()) 
                            ? "" : emitterItem["emitterNodeName"].get<string>();
                        alarmEmitter.orgId = (!emitterItem.contains("orgId") || emitterItem["orgId"].is_null()) 
                            ? 0 : emitterItem["orgId"].get<int>();
                        // alarmEmitter.createdBy = emitterItem["createdBy"].get<string>();
                        // alarmEmitter.createdOn = emitterItem["createdOn"].get<string>();
                        // alarmEmitter.updatedBy = emitterItem["updatedBy"].get<string>();
                        // alarmEmitter.updatedOn = emitterItem["updatedOn"].get<string>();
                        // alarmEmitter.isDeleted = emitterItem["isDeleted"].get<string>();
                        // alarmEmitter.deletedBy = emitterItem["deletedBy"].get<string>();
                        // alarmEmitter.deletedOn = emitterItem["deletedOn"].get<string>();
                        alarmEmitters.push_back(alarmEmitter);
                        } catch(const std::exception &e) {
                            log("Error parsing alarm emitter: " + std::string(e.what()), LogLevel::ERRORS);
                            // Skip this emitter and continue with the next
                            continue;
                        }
                    }
                    alarmConfig.alarmEmitters = alarmEmitters;
                } else {
                    log("Alarm '" + alarmConfig.name + "' has no emitters", LogLevel::ERRORS);
                }
                alarms.push_back(alarmConfig);
                } catch(const std::exception &e) {
                    log("Error parsing alarm config item: " + std::string(e.what()), LogLevel::ERRORS);
                    // Skip this alarm and continue with the next
                    continue;
                }
            }
        } 
        // else if(response.contains("alarmConfigs") && response["alarmConfigs"].is_array()) {
        //     // Fallback for old API format
        //     log("Found 'alarmConfigs' field (old format) with " + std::to_string(response["alarmConfigs"].size()) + " alarm(s)", LogLevel::INFO);
        // } 
        else {
            log("ERROR: No 'data' or 'alarmConfigs' field found in API response!", LogLevel::ERRORS);
            // log("Response keys: " + response.dump(), LogLevel::DEBUG);
        }
        } catch(const std::exception &e) {
            log("Error in ParseAlarmConfig: " + std::string(e.what()), LogLevel::ERRORS);
            // Return empty vector on error rather than crashing
        }
    }
    return alarms;
}





vector<OrgConfig>
ParseOrgConfig(string host, string port, string bearerToken, string json_body,
    string target) {
    vector<OrgConfig> orgConfig;

    if(!bearerToken.empty()) {
        try {
            auto futureResponse = std::async(std::launch::async, GetAllOrganizationList,
                                             host, port,
                                             bearerToken, json_body, target);
            json response = futureResponse.get();

            // Check for 'data' field (actual API response format)
            if(response.contains("data") && response["data"].is_array()) {
                for(const auto &item : response["data"]) {
                    try {
                        OrgConfig orgs;
                        orgs.id = item.contains("id") ? item["id"].get<int>() : 0;
                        orgs.name = item.contains("name") ? item["name"].get<string>() : "";
                        orgs.shortCode = item.contains("shortCode") ? item["shortCode"].get<string>() : "";
                        orgs.emailPrimary = item.contains("emailPrimary") ? item["emailPrimary"].get<string>() : "";
                        orgs.emailSecondary = item.contains("emailSecondary") ? item["emailSecondary"].get<string>() : "";
                        orgs.contactNoPrimary = item.contains("contactNoPrimary") ? item["contactNoPrimary"].get<string>() : "";
                        orgs.contactNoSecondary = item.contains("contactNoSecondary") ? item["contactNoSecondary"].get<string>() : "";
                        orgs.remark = item.contains("remark") ? item["remark"].get<string>() : "";
                        orgs.displayName = item.contains("displayName") ? item["displayName"].get<string>() : "";
                        orgs.organisationType = item.contains("organisationType") ? item["organisationType"].get<string>() : "";
                        orgs.tenancyType = item.contains("tenancyType") ? item["tenancyType"].get<string>() : "";
                        orgs.profileId = item.contains("profileId") ? item["profileId"].get<int>() : 0;
                        orgs.orgId = item.contains("orgId") ? item["orgId"].get<int>() : 0;
                        orgs.isCopyProfile = item.contains("isCopyProfile") ? item["isCopyProfile"].get<bool>() : false;

                       
                        orgConfig.push_back(orgs);
                    } catch(const std::exception &e) {
                        log("Error parsing org config item: " + std::string(e.what()),
                            LogLevel::ERRORS);
                        // Skip this alarm and continue with the next
                        continue;
                    }
                }
            }
            else {
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


UserProfile ParseUserProfile(string host, string port, string bearerToken, 
                            string json_body, string target) {
    UserProfile profile; // Default empty profile

    if(!bearerToken.empty()) {
        try {
            auto futureResponse = std::async(std::launch::async, GetUserProfile,
                                             host, port,
                                             bearerToken, json_body, target);
            json response = futureResponse.get();

            // Check for 'data' field (actual API response format)
            if(response.contains("data") && response["data"].is_array() && !response["data"].empty()) {
                const auto &item = response["data"][0]; // Get first user profile
                
                try {
                    profile.userId = item.value("userId", 0);
                    profile.displayName = item.value("displayName", "");
                    
                    // Helper lambda to convert number or string to string
                    auto getAsString = [&item](const std::string& key) -> std::string {
                        if(!item.contains(key)) return "";
                        if(item[key].is_number()) return std::to_string(item[key].get<int>());
                        if(item[key].is_string()) return item[key].get<std::string>();
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
                    profile.currentOrgProfileId = item.value("currentOrgProfileId", "");
                    profile.currentOrgProductName = item.value("currentOrgProductName", "");
                    profile.currentOrgCode = item.value("currentOrgCode", "");
                    profile.currentOrganisationType = item.value("currentOrganisationType", "");
                    profile.currentOrgLogo = item.value("currentOrgLogo", "");
                    
                    log("✓ Parsed user profile: " + profile.displayName + 
                        " (OrgID: " + profile.currentOrgId + ")", LogLevel::DEBUG);
                } catch(const std::exception &e) {
                    log("Error parsing user profile item: " + std::string(e.what()),
                        LogLevel::ERRORS);
                }
            }
            else {
                log("ERROR: No 'data' field found in user profile response or data is empty!",
                    LogLevel::ERRORS);
                // log("Response: " + response.dump(), LogLevel::DEBUG);
            }
        } catch(const std::exception &e) {
            log("Error in ParseUserProfile: " + std::string(e.what()), LogLevel::ERRORS);
        }
    }
    
    return profile;
}

// Assumes `json` is nlohmann::json and types ServerConfig, orgMappings, log, LogLevel exist.

ServerConfig ServerConfigFromJSON(const json& item) {
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
        if (item.contains("orgMappings")) {
            const auto &om = item["orgMappings"];

            if (om.is_array()) {
                for (const auto &entry : om) {
                    try {
                        orgMappings mapping;
                        mapping.id = entry.value("id", 0);
                        mapping.hierarchyId = entry.value("hierarchyId", 0);
                        mapping.mapOrgId = entry.value("mapOrgId", 0);
                        mapping.orgShortCode = entry.value("orgShortCode", std::string{});
                        serverConfig.orgMappings.push_back(std::move(mapping));
                        log("DEBUG: Parsed OrgMapping - ShortCode: " + serverConfig.orgMappings.back().orgShortCode + 
                            ", ID: " + std::to_string(serverConfig.orgMappings.back().id) + 
                            ", MapOrgID: " + std::to_string(serverConfig.orgMappings.back().mapOrgId), LogLevel::INFO);
                    } catch (const std::exception &e) {
                        log(std::string("Error parsing orgMappings array entry: ") + e.what(), LogLevel::ERRORS);
                    }
                }
            } else if (om.is_object()) {
                try {
                    orgMappings mapping;
                    mapping.id = om.value("id", 0);
                    mapping.hierarchyId = om.value("hierarchyId", 0);
                    mapping.mapOrgId = om.value("mapOrgId", 0);
                    mapping.orgShortCode = om.value("orgShortCode", std::string{});
                    serverConfig.orgMappings.push_back(std::move(mapping));
                    log("DEBUG: Parsed OrgMapping - ShortCode: " + serverConfig.orgMappings.back().orgShortCode + 
                        ", ID: " + std::to_string(serverConfig.orgMappings.back().id) + 
                        ", MapOrgID: " + std::to_string(serverConfig.orgMappings.back().mapOrgId), LogLevel::INFO);
                } catch (const std::exception &e) {
                    log(std::string("Error parsing orgMappings object: ") + e.what(), LogLevel::ERRORS);
                }
            } else {
                log("orgMappings present but not array/object; ignoring", LogLevel::INFO);
            }
        }
    } catch (const std::exception &e) {
        log(std::string("Error in SevrverConfigFromJSON: ") + e.what(), LogLevel::ERRORS);
    }
    return serverConfig;
}

std::vector<ServerConfig> ParseServerConfig(const std::string &host,
                               const std::string &port,
                               const std::string &bearerToken,
                               const std::string &json_body,
                               const std::string &target)
{
    std::vector<ServerConfig> serverConfigs;

    if (bearerToken.empty()) {
        log("ParseServerConfig: empty bearerToken; returning empty list", LogLevel::INFO);
        return serverConfigs;
    }

    try {
        auto futureResponse = std::async(std::launch::async,
                                         getHierarchy,
                                         host, port, bearerToken, json_body, target);
        json response = futureResponse.get();

        if (!response.contains("data") || !response["data"].is_array() || response["data"].empty()) {
            log("ERROR: No 'data' field found in server config response or data is empty!", LogLevel::ERRORS);
            return serverConfigs;
        }

        for (const auto &item : response["data"]) {
           serverConfigs.push_back(ServerConfigFromJSON(item));
        }

    } catch (const std::exception &e) {
        log(std::string("Error in ParseServerConfig: ") + e.what(), LogLevel::ERRORS);
    }

    return serverConfigs;
}


std::pair<int, std::string> extractNsAndValue(const std::string& input) {
    std::size_t nsPos = input.find("ns=");
    std::size_t semiPos = input.find(';');
    std::size_t equalPos = input.find('=', semiPos);  // '=' after the semicolon

    if (nsPos == std::string::npos || semiPos == std::string::npos || equalPos == std::string::npos) {
        return {0, ""};
    }

    int ns = std::stoi(input.substr(nsPos + 3, semiPos - (nsPos + 3))); // Extract between "ns=" and ';'
    std::string value = input.substr(equalPos + 1); // Extract after '='

    return {ns, value};
}
