#ifndef API_HANDLER_H
#define API_HANDLER_H

#include <string>
#include <vector>
#include <utility>
#include <unordered_map>
#include <boost/asio/io_context.hpp>
#include <nlohmann/json.hpp>
#include "structs.h" 
#include "AlarmConfig.h" 
#include "OrgConfig.h"
#include "UserProfile.h"
#include "ServerConfig.h"

// Forward declare a dedicated io_context for HTTP/beast operations
namespace as = boost::asio;
extern as::io_context http_ioc;

// Declare global mappings as extern to be accessible from other files.
// These variables must be defined in one .cpp file (e.g., your existing one).
extern std::unordered_map<int, std::pair<std::string, std::string>> Mapping;
extern std::unordered_map<int, std::string> TopicMapping;

using json = nlohmann::ordered_json;

/**
 * @brief Authenticates with the API and retrieves a bearer token.
 *
 * @param host The hostname or IP address of the API server.
 * @param port The port number for the API service.
 * @param username The username for authentication.
 * @param password The password for authentication.
 * @return A JSON object containing the API response, including the bearer token.
 * @throws std::exception on connection or HTTP errors.
 */
json getBearerToken(std::string host, std::string port, std::string username, std::string password);

/**
 * @brief Fetches the OPC UA hierarchy from the API using a bearer token.
 *
 * @param host The hostname or IP address of the API server.
 * @param port The port number for the API service.
 * @param bearerToken The authentication token obtained from getBearerToken.
 * @return A JSON object representing the complete server and tag hierarchy.
 * @throws std::exception on connection or HTTP errors.
 */
json getResponse(std::string host, std::string port, std::string bearerToken,
            std::string json_body, std::string target);

/**
 * @brief Parses the JSON hierarchy into a vector of C++ structs.
 *
 * This function calls getResponse internally and processes the JSON response,
 * populating the global `Mapping` variable as a side effect.
 *
 * @param host The hostname or IP address of the API server.
 * @param port The port number for the API service.
 * @param bearerToken The authentication token.
 * @return A std::vector of ServerInfoO objects, representing the parsed server configurations.
 */
std::vector<ServerInfoO> ParseServerHierarchy(std::string host, std::string port, std::string bearerToken , std::string json_body, std::string target);

std::vector<ServerInfoO> ParseServerHierarchyFromJson(const nlohmann::ordered_json& response);

std::vector<AlarmConfig> ParseAlarmConfig(std::string host, std::string port, std::string bearerToken, std::string json_body, std::string target);

std::vector<AlarmConfig> ParseAlarmConfigFromJson(const nlohmann::ordered_json& response);
UserProfile ParseUserProfileFromJson(const nlohmann::ordered_json& response);

std::vector<OrgConfig> ParseOrgConfig(std::string host, std::string port, std::string bearerToken, std::string json_body, std::string target);

UserProfile ParseUserProfile(std::string host, std::string port, std::string bearerToken, 
                            std::string json_body, std::string target);

std::vector<ServerConfig> ParseServerConfig(const std::string &host, const std::string &port, const std::string &bearerToken, const std::string &json_body, const std::string &target);

/**
 * @brief A utility function to extract the namespace index and identifier from an OPC UA node string.
 *
 * @param input A string in the format "ns=<index>;<type>=<value>" (e.g., "ns=2;i=1001").
 * @return A std::pair containing the namespace index (int) and the identifier (std::string).
 */
std::pair<int, std::string> extractNsAndValue(const std::string& input);



ServerConfig ServerConfigFromJSON(const json& item);

// ─────────────────────────────────────────────────────────────────────────────
// Hot reload (MQTT topic HTRLD/Edgents → /api/EdgentHotReloading)
// ─────────────────────────────────────────────────────────────────────────────

// One mapped tag option (mappedTagOptions[i]) inside a HotReloadItem. Each
// option corresponds to one MQTT topic ↔ tagId pair that should drive a
// monitored item on the OPC UA server side.
struct HotReloadMappedTagOption {
    int id = 0;
    int tagId = 0;          // key used in Mapping / TopicMapping
    int orgId = 0;
    std::string name;
    std::string namespaces; // MQTT topic (e.g. "TDSPL/UAv1/UAv21/UA_15001")
};

// One tag/datapoint entry from the EdgentHotReloading API response payload.
// The API may return either a short identifier in `nodeId` (e.g. "ND011") or
// a full OPC UA path in `namespacePath` (e.g. "ns=2;i=32081"); callers
// prefer `namespacePath` and fall back to constructing one from
// `nodeId` + the connected context's resolved namespace index.
struct HotReloadItem {
    int id = 0;            // hierarchy id of the item
    int dataPointId = 0;   // hint for tagId fallback
    std::string name;
    std::string typeId;
    std::string nodeId;        // raw identifier (e.g. "ND011")
    std::string namespacePath; // full OPC UA node path (e.g. "ns=2;i=32081")
    std::string parentId;
    std::string rdWtOpt;       // from dataPointsModel.rdWtOpt (e.g. "RD_WRT_RW")
    int orgId = 0;
    double deadbandPercent = 0.0;
    int sampling = 0;
    int queueSize = 0;

    // Group-level fields. Only populated when the API row describes an
    // OPC_HI_GROUP and consumed by applyGroupUpdate to call
    // UA_Client_Subscriptions_modify. -1 indicates "not present" so the
    // dispatcher can preserve existing values it cannot override.
    int publishingInterval = -1;
    int maxKeepAliveCount = -1;
    int lifetimeCount = -1;
    int priority = -1;
    int maxNotificationsPerPublish = -1;

    std::vector<HotReloadMappedTagOption> mappedTagOptions;
};

/**
 * @brief Issue a POST to /api/EdgentHotReloading with the original MQTT
 *        command body and return the raw JSON response.
 *
 * @param outHttpStatus If non-null, receives the HTTP status code (e.g. 401)
 *        on a completed response; 0 on transport / pre-response failure.
 */
json callHotReloadAPI(const std::string &host, const std::string &port,
                      const std::string &bearerToken,
                      const json &commandBody,
                      unsigned *outHttpStatus = nullptr);

/**
 * @brief Issue a POST to /api/EventsHotReloading with the original MQTT
 *        command body and return the raw JSON response.
 *
 * Used by the UA Server when a HTRLD/Events MQTT message is received.
 * Mirrors callHotReloadAPI but targets the alarm/events endpoint.
 *
 * @param outHttpStatus If non-null, receives the HTTP status code on a
 *        completed response; 0 on transport / pre-response failure.
 */
json callEventsHotReloadAPI(const std::string &host, const std::string &port,
                            const std::string &bearerToken,
                            const json &commandBody,
                            unsigned *outHttpStatus = nullptr);

/**
 * @brief Extract the doubly-nested data array from the EdgentHotReloading
 *        response envelope into a flat list of HotReloadItem.
 *
 * Returns an empty vector on any field-missing / bad-type condition rather
 * than throwing.
 */
std::vector<HotReloadItem> parseHotReloadResponse(const json &response);

/**
 * @brief Walk the EdgentHotReloading response envelope and pull out any
 *        OPC UA server descriptors (typeId == "OPC_HI_SERVER" or items that
 *        carry an endpointUrl). Each entry is parsed into a fully populated
 *        ServerInfoO including its groups/tags/mappedInfospaceTags so the
 *        caller can spin up a brand-new ClientContext at runtime.
 *
 * Side effect: populates the global Mapping / TopicMapping for every newly
 * discovered tag, mirroring the startup parser.
 *
 * Returns an empty vector on any field-missing / bad-type condition rather
 * than throwing.
 */
std::vector<ServerInfoO> parseHotReloadServerEntries(const json &response);

#endif // API_HANDLER_H
