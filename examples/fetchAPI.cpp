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


using json = nlohmann::ordered_json;


using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;       
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;  

as::io_context ioc;
unordered_map<int, pair<string, string>> Mapping;
unordered_map<int, string> TopicMapping;

json getBearerToken(string host, string port , string username, string password) {
    try {
        // std::string host = "164.52.221.177";
        // std::string port = "5128";
        std::string target = "/api/Login";
        int version = 11;

        // JSON body built from provided credentials
        json jbody;
        jbody["Username"] = username;
        jbody["password"] = password;
        std::string json_body = jbody.dump();

        // Set up I/O context and resolver
        // as::io_context ioc;
        tcp::resolver resolver(ioc);
        beast::tcp_stream stream(ioc);

        // Resolve domain name
        auto const results = resolver.resolve(host, port);

        // Connect to host
        stream.connect(results);

        // Create HTTP POST request
        beast::http::request<beast::http::string_body> req{beast::http::verb::post, target, version};
        req.set(beast::http::field::host, host);
        req.set(beast::http::field::user_agent, BOOST_BEAST_VERSION_STRING);
        req.set(beast::http::field::content_type, "application/json");
        req.body() = json_body;
        req.prepare_payload();

        // Send request
        beast::http::write(stream, req);

        // Read response
        beast::flat_buffer buffer;
        beast::http::response<beast::http::string_body> res;
        beast::http::read(stream, buffer, res);

        json result = json::parse(res.body());
        log(res.body().c_str(),LogLevel::DEBUG);

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



json getHierarchy(string host , string port , string bearerToken) {

try{
    // std::string host = host;
    // std::string port = port;
    std::string target = "/api/GetOpcUaHierarchy";
    int version = 11;

    // JSON body
    std::string json_body = R"(
    {
   
        "orgId": 0,
        "roleId": "",
        "userId": 0,
        "moduleId": 0,
        "userType": "",
        "requestDateTime": "2024-12-26T08:16:05.629Z",
        "ipAddress": "",
        "originName": "",
        "filterModel": {
            
            "customValue": "all"
        },

        "data": {
            "nodeId": "ND02"
        }
    }
    )";

    // Set up I/O context and connection
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);

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

    log(res.body().c_str(),LogLevel::DEBUG);
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

vector<ServerInfoO> ParseServerHierarchy(string host, string port, string bearerToken) {

    vector<ServerInfoO> servers;
    if(!bearerToken.empty()) {
        auto futureResponse = std::async(std::launch::async, getHierarchy, host, port,bearerToken);
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

                // if(item.contains("tags") && item["tags"].is_array()) {
                //     vector<TagInfo> serverTags;
                //     for(const auto &tagItem : item["tags"]) {
                //         TagInfo tagInfo;

                //         tagInfo.scaling = tagItem["scaling"].get<bool>();
                //         tagInfo.rawMin = tagItem["rawMin"].get<double>();
                //         tagInfo.rawMax = tagItem["rawMax"].get<double>();
                //         tagInfo.scaleMin = tagItem["scaleMin"].get<double>();
                //         tagInfo.scaleMax = tagItem["scaleMax"].get<double>();
                //         tagInfo.enableExpression =
                //         tagItem["enableExpression"].get<bool>();
                //         tagInfo.expression = tagItem["expression"].get<string>();

                //         if(tagItem.contains("namespaceNodeID") && tagItem["namespaceNodeID"].is_string()) {
                //             tagInfo.namespaceNodeID = tagItem["namespaceNodeID"].get<string>();
                //         }
                //         if(tagItem.contains("dataPointId") && tagItem["dataPointId"].is_number_integer()) {
                //             tagInfo.dataPointId = tagItem["dataPointId"].get<int>();
                //         }
                //         if(tagItem.contains("name") && tagItem["name"].is_string()) {
                //         tagInfo.name = tagItem["name"].get<string>();
                //         }
                //         if(tagItem.contains("nodeId") && tagItem["nodeId"].is_string()) {
                //             tagInfo.nodeId = tagItem["nodeId"].get<string>();
                //         }
                //         if(tagItem.contains("typeId") && tagItem["typeId"].is_string()) {
                //             tagInfo.typeId = tagItem["typeId"].get<string>();
                //         }
                //         if(tagItem.contains("parentId") && tagItem["parentId"].is_string()) {
                //             tagInfo.parentId = tagItem["parentId"].get<string>();
                //         }

                //         if(tagItem.contains("mappedInfospaceTags") &&
                //            tagItem["mappedInfospaceTags"].is_array()) {
                //             vector<MappedInfospaceTag> mappedTags;
                //             for(const auto &mappedTag : tagItem["mappedInfospaceTags"]) {
                //                 MappedInfospaceTag mappedInfo;
                //                 mappedInfo.id = mappedTag["id"].get<int>();
                //                 mappedInfo.tagId = mappedTag["tagId"].get<int>();                                
                //                 mappedInfo.name = mappedTag["name"].get<string>();
                //                 mappedInfo.namespaces =
                //                     mappedTag["namespace"].get<string>();
                //                 mappedInfo.isSimulationProfile =
                //                     mappedTag["isSimulationProfile"].get<bool>();
                //                 mappedInfo.isLogging = mappedTag["isLogging"].get<bool>();
                //                 mappedInfo.isVirtual = mappedTag["isVirtual"].get<bool>();
                                
                //                 //name to namespace 
                //                 // if(tagItem.contains("namespaceNodeID") && tagItem["namespaceNodeID"].is_string() && tagInfo.namespaceNodeID.has_value()) {
                //                     pair<string, string> NodePair = {tagInfo.name.value(), serverInfo.endpointUrl};
                //                     Mapping[mappedInfo.tagId] = NodePair;

                //                 // }
                //                 mappedTags.push_back(mappedInfo);
                //             }
                //             tagInfo.mappedInfospaceTags = mappedTags;
                //         }

                //         serverTags.push_back(tagInfo);
                //     }
                //     serverInfo.tags = serverTags;
                // }


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
