#pragma once
#include <string>

/**
 * @brief Holds the decrypted configuration from an EdgeConfig_*.txt file.
 *
 * JSON structure after decryption:
 * {
 *   "Id":43,
 *   "Name":"Node_Test1",
 *   "ShortCode":"NODE_TEST1",
 *   "IP":"127.0.0.1",
 *   "AuthenticationType":"AUTH_TYP_BSC",
 *   "UserName":"NODE_TEST1",
 *   "Password":"<base64>",
 *   "ClientId":"XEE-MOD-NODE_TEST1-168856",
 *   "EdgentType":"MOD"
 * }
 */
struct EdgeConfigData {
    int         id           = 0;
    std::string name;
    std::string shortCode;      // Used as NodeID throughout the application
    std::string ip;
    std::string authType;
    std::string username;       // API / OPC UA sign-in username
    std::string password;       // API / OPC UA sign-in password (as received, base64)
    std::string clientId;       // MQTT ClientID
    std::string edgentType;

    // Derived MQTT fields — populated by SetEdgeConfigMqttPassword() after bearer token is known
    std::string mqttUsername;   // = username
    std::string mqttPassword;   // = JSON string: {"token":"<bearerToken>"}
};

/**
 * @brief Scans the working directory for a file matching "<prefix>*.txt",
 *        decrypts its contents using AES-256-CBC (key: "TechDC0nf!g"),
 *        and parses the resulting JSON into EdgeConfigData.
 *
 * @param prefix  Filename prefix to search for.
 *                Client: default "EdgeConfig_"
 *                Server: "EdgeConfig_ND07_Server_"
 *
 * @throws std::runtime_error if no matching file is found, decryption fails,
 *         or JSON is malformed / missing required fields.
 */
EdgeConfigData LoadEdgeConfig(const std::string& prefix = "EdgeConfig_");

/**
 * @brief Populates mqttUsername and mqttPassword in the EdgeConfigData after
 *        the bearer token is available.
 *
 * mqttUsername = cfg.username
 * mqttPassword = UTF-8 string of: {"token":"<bearerToken>"}
 *
 * @param cfg         The EdgeConfigData to update (modified in-place).
 * @param bearerToken The bearer token obtained from the API.
 */
void SetEdgeConfigMqttPassword(EdgeConfigData& cfg, const std::string& bearerToken);
