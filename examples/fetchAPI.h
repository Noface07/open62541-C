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
json getHierarchy(std::string host, std::string port, std::string bearerToken, std::string json_body, std::string target);

/**
 * @brief Parses the JSON hierarchy into a vector of C++ structs.
 *
 * This function calls getHierarchy internally and processes the JSON response,
 * populating the global `Mapping` variable as a side effect.
 *
 * @param host The hostname or IP address of the API server.
 * @param port The port number for the API service.
 * @param bearerToken The authentication token.
 * @return A std::vector of ServerInfoO objects, representing the parsed server configurations.
 */
std::vector<ServerInfoO> ParseServerHierarchy(std::string host, std::string port, std::string bearerToken , std::string json_body, std::string target);

std::vector<AlarmConfig> ParseAlarmConfig(std::string host, std::string port, std::string bearerToken, std::string json_body, std::string target);

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

#endif // API_HANDLER_H
