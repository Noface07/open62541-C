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


using json = nlohmann::json;


using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;       
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;  

as::io_context ioc;
unordered_map<int, pair<string, string>> Mapping;

json getBearerToken() {
    try {
        std::string host = "164.52.221.177";
        std::string port = "5128";
        std::string target = "/api/Login";
        int version = 11;

        // JSON body
        std::string json_body = R"({
        "Username":"ajay.sharma@techondater.co.in",
        "password":"VvvQVRdH7JheYR7lLgbPCp4fcNEslXnKqhR59bdFMK8="
        })";

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
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, res.body().c_str());

        // Gracefully close the connection
        beast::error_code ec;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec);
        if (ec && ec != beast::errc::not_connected)
            throw beast::system_error{ec};

        return result;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return json{};
    }
}



json getHierarchy(string bearerToken) {

try{
    std::string host = "164.52.221.177";
    std::string port = "5128";
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
            "nodeId": "ND01"
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

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, res.body().c_str());
    json result = json::parse(res.body());
    // Shutdown connection
    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);

    return result;

    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return json{};
    }
    
}

vector<ServerInfoO> ParseServerHierarchy(string bearerToken) {

    vector<ServerInfoO> servers;
    if(!bearerToken.empty()) {
        auto futureResponse = std::async(std::launch::async, getHierarchy, bearerToken);
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

                // if(item.contains("groups") && item["groups"].is_array()) {
                //     vector<GroupInfo> groups;
                //     for(const auto &groupItem : item["groups"]) {
                //         GroupInfo groupInfo;

                //         groupInfo.dataPointId = groupItem["dataPointId"].get<int>();
                //         groupInfo.name = groupItem["name"].get<string>();
                //         groupInfo.nodeId = groupItem["nodeId"].get<string>();
                //         groupInfo.typeId = groupItem["typeId"].get<string>();
                //         groupInfo.parentId = groupItem["parentId"].get<string>();

                //         if(groupItem.contains("tags") && groupItem["tags"].is_array()) {
                //             vector<TagInfo> groupTags;
                //             for(const auto &tagItem : groupItem["tags"]) {
                //                 TagInfo tagInfo;

                //                 tagInfo.scaling = tagItem["scaling"].get<bool>();
                //                 tagInfo.rawMin = tagItem["rawMin"].get<double>();
                //                 tagInfo.rawMax = tagItem["rawMax"].get<double>();
                //                 tagInfo.scaleMin = tagItem["scaleMin"].get<double>();
                //                 tagInfo.scaleMax = tagItem["scaleMax"].get<double>();
                //                 tagInfo.enableExpression =
                //                     tagItem["enableExpression"].get<bool>();
                //                 tagInfo.expression = tagItem["expression"].get<string>();
                //                 tagInfo.dataPointId = tagItem["dataPointId"].get<int>();
                //                 tagInfo.name = tagItem["name"].get<string>();
                //                 tagInfo.nodeId = tagItem["nodeId"].get<string>();
                //                 tagInfo.typeId = tagItem["typeId"].get<string>();
                //                 tagInfo.parentId = tagItem["parentId"].get<string>();

                //                 if(tagItem.contains("mappedInfospaceTags") &&
                //                    tagItem["mappedInfospaceTags"].is_array()) {
                //                     vector<MappedInfospaceTag> mappedTags;
                //                     for(const auto &mappedTag :
                //                         tagItem["mappedInfospaceTags"]) {
                //                         MappedInfospaceTag mappedInfo;
                //                         mappedInfo.id = mappedTag["id"].get<int>();
                //                         mappedInfo.tagId = mappedTag["tagId"].get<int>();
                //                         mappedInfo.name = mappedTag["name"].get<string>();
                //                         mappedInfo.namespaces =
                //                             mappedTag["namespace"].get<string>();
                //                         mappedInfo.isSimulationProfile =
                //                             mappedTag["isSimulationProfile"].get<bool>();
                //                         mappedInfo.isLogging =
                //                             mappedTag["isLogging"].get<bool>();
                //                         mappedInfo.isVirtual =
                //                             mappedTag["isVirtual"].get<bool>();
                //                         mappedTags.push_back(mappedInfo);
                //                     }
                //                     tagInfo.mappedInfospaceTags = mappedTags;
                //                 }

                //                 groupTags.push_back(tagInfo);
                //             }
                //             groupInfo.tags = groupTags;
                //         }

                //         groups.push_back(groupInfo);
                //     }
                //     serverInfo.groups = groups;
                // }

                if(item.contains("tags") && item["tags"].is_array()) {
                    vector<TagInfo> serverTags;
                    for(const auto &tagItem : item["tags"]) {
                        TagInfo tagInfo;

                        tagInfo.scaling = tagItem["scaling"].get<bool>();
                        tagInfo.rawMin = tagItem["rawMin"].get<double>();
                        tagInfo.rawMax = tagItem["rawMax"].get<double>();
                        tagInfo.scaleMin = tagItem["scaleMin"].get<double>();
                        tagInfo.scaleMax = tagItem["scaleMax"].get<double>();
                        tagInfo.enableExpression =
                        tagItem["enableExpression"].get<bool>();
                        tagInfo.expression = tagItem["expression"].get<string>();

                        if(tagItem.contains("namespaceNodeID") && tagItem["namespaceNodeID"].is_string()) {
                            tagInfo.namespaceNodeID = tagItem["namespaceNodeID"].get<string>();
                        }
                        if(tagItem.contains("dataPointId") && tagItem["dataPointId"].is_number_integer()) {
                            tagInfo.dataPointId = tagItem["dataPointId"].get<int>();
                        }
                        if(tagItem.contains("name") && tagItem["name"].is_string()) {
                        tagInfo.name = tagItem["name"].get<string>();
                        }
                        if(tagItem.contains("nodeId") && tagItem["nodeId"].is_string()) {
                            tagInfo.nodeId = tagItem["nodeId"].get<string>();
                        }
                        if(tagItem.contains("typeId") && tagItem["typeId"].is_string()) {
                            tagInfo.typeId = tagItem["typeId"].get<string>();
                        }
                        if(tagItem.contains("parentId") && tagItem["parentId"].is_string()) {
                            tagInfo.parentId = tagItem["parentId"].get<string>();
                        }

                        if(tagItem.contains("mappedInfospaceTags") &&
                           tagItem["mappedInfospaceTags"].is_array()) {
                            vector<MappedInfospaceTag> mappedTags;
                            for(const auto &mappedTag : tagItem["mappedInfospaceTags"]) {
                                MappedInfospaceTag mappedInfo;
                                mappedInfo.id = mappedTag["id"].get<int>();
                                mappedInfo.tagId = mappedTag["tagId"].get<int>();                                
                                mappedInfo.name = mappedTag["name"].get<string>();
                                mappedInfo.namespaces =
                                    mappedTag["namespace"].get<string>();
                                mappedInfo.isSimulationProfile =
                                    mappedTag["isSimulationProfile"].get<bool>();
                                mappedInfo.isLogging = mappedTag["isLogging"].get<bool>();
                                mappedInfo.isVirtual = mappedTag["isVirtual"].get<bool>();
                                
                                
                                if(tagItem.contains("namespaceNodeID") && tagItem["namespaceNodeID"].is_string() && tagInfo.namespaceNodeID.has_value()) {
                                    pair<string, string> NodePair = {tagInfo.namespaceNodeID.value(), serverInfo.endpointUrl};
                                    Mapping[mappedInfo.tagId] = NodePair;
                                }

                                mappedTags.push_back(mappedInfo);
                            }
                            tagInfo.mappedInfospaceTags = mappedTags;
                        }

                        serverTags.push_back(tagInfo);
                    }
                    serverInfo.tags = serverTags;
                }

                servers.push_back(serverInfo);
            }
        }
    }

    cout << "Mapping: " << endl;
    for(const auto &mapping : Mapping) {
        cout << mapping.first << " " << mapping.second.first << " " << mapping.second.second << endl;
    }
    cout << endl;

    return servers;
}
