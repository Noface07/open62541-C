#define _WIN32_WINNT 0x0601 // Prevent boost/asio warning
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>

#include "RedisClient.h"
#include "fetchAPI.h"
#include "Encryption.h"
#include "Logger.h" // For Logger definitions
#include <nlohmann/json.hpp>

using json = nlohmann::ordered_json;

// Global settings (Populated from appsettings.json)
std::string REDIS_HOST = "127.0.0.1";
int REDIS_PORT = 6379;
std::string REDIS_PASS = "";
int REDIS_DB = 0;
std::string REDIS_PREFIX = "OPCUA_SERVER:";

std::string API_HOST = "";
std::string API_PORT = "";
std::string DEFAULT_API_USER = "";
std::string DEFAULT_API_PASS = "";

// ----------------------------------------------------------------------------
// Logger Stub
// ----------------------------------------------------------------------------

void setupLogging() {
    g_logging_enabled = false; // No file log for utility
    g_debug = true;            // Print to console
}

void printUsage(const char* progName) {
    std::cout << "Usage: " << progName << " [command] [args...]\n"
              << "Interactive Mode:\n"
              << "  (Run without arguments to start interactive login)\n"
              << "\n"
              << "Commands:\n"
              << "  login [user] [pass]                Interactive Login -> Fetch All Data\n"
              << "  update_user <user> [pass]          Fetch Profile -> Fetch Org Data (CLI)\n"
              << "  update_org <orgId>                 Fetch Org Data Only (Using Admin Token)\n"
              << "  set <key_suffix> <value>           Manual Set\n"
              << "  setfile <key_suffix> <filepath>    Manual Set from File\n";
}

// ----------------------------------------------------------------------------
// Configuration Loading
// ----------------------------------------------------------------------------

bool loadConfiguration() {
    std::cout << "[Config] Loading appsettings.json...\n";
    std::ifstream file("appsettings.json");
    if (!file.is_open()) {
        std::cerr << "❌ Error: Could not open appsettings.json. Ensure it is in the same directory.\n";
        return false;
    }
    
    try {
        json config;
        file >> config;
        
        // Defaults matching server.cpp if not overridden
        REDIS_HOST = "216.48.184.131"; 
        REDIS_PORT = 6379;
        REDIS_PASS = "xeeredis@techd";
        
        // Extract API Params
        API_HOST = config["AppSettings"]["ApplicationEndURLHost"].get<std::string>();
        int port = config["AppSettings"]["ApplicationEndURLPort"].get<int>();
        API_PORT = std::to_string(port);
        
        // Extract Auth
        if (config.contains("Authorization")) {
            DEFAULT_API_USER = config["Authorization"]["Username"].get<std::string>();
            DEFAULT_API_PASS = config["Authorization"]["Password"].get<std::string>();
        } else {
            DEFAULT_API_USER = "admin";
            DEFAULT_API_PASS = "123456";
        }
        
        std::cout << "✓ Config Loaded. API Host: " << API_HOST << ":" << API_PORT << "\n";
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Error parsing appsettings.json: " << e.what() << "\n";
        return false;
    }
}

// ----------------------------------------------------------------------------
// Logic Helpers
// ----------------------------------------------------------------------------

// Helper to update Org Data (Topics + Alarms)
void updateOrgData(int orgId, const std::string& token) {
    std::cout << "\n   [API] Fetching Data for OrgID " << orgId << "...\n";
    
    // 1. Topic List
    try {
        json body;
        body["orgId"] = orgId;
        body["roleId"] = ""; 
        body["userId"] = 0;
        body["filterModel"]["customValue"] = "all";
        body["data"]["isLogging"] = true;
        
        auto response = getResponse(API_HOST, API_PORT, token, body.dump(), "/api/GetTopicList");
        
        std::string key = "TOPIC_LIST_" + std::to_string(orgId);
        g_redisClient.setCompressed(key, response.dump(), 0);
        std::cout << "   [Redis] Set " << key << " (Size: " << response.dump().size() << ")\n";
    } catch (const std::exception& e) {
        std::cerr << "   [Error] GetTopicList Failed: " << e.what() << "\n";
    }

    // 2. Alarms
    try {
        json body;
        body["orgId"] = orgId;
        body["filterModel"]["currentPage"] = 1;
        body["filterModel"]["pageSize"] = 100;

        auto response = getResponse(API_HOST, API_PORT, token, body.dump(), "/api/GetAlarmsConfigDetailList");
        
        std::string key = "ALARMS_" + std::to_string(orgId);
        g_redisClient.setCompressed(key, response.dump(), 0);
        std::cout << "   [Redis] Set " << key << " (Size: " << response.dump().size() << ")\n";
    } catch (const std::exception& e) {
        std::cerr << "   [Error] GetAlarmsConfigDetailList Failed: " << e.what() << "\n";
    }
}

// Main Flow: Login -> Profile -> Org Data
void updateAllForUser(const std::string& username, const std::string& password) {
    try {
        // 1. Authenticate
        std::cout << "\n[1/3] Authenticating as '" << username << "'...\n";
        std::string encryptedPass = GetEncryptedString("", password, 0);
        json authResponse = getBearerToken(API_HOST, API_PORT, username, encryptedPass);
        
        if (!authResponse.contains("access_token")) {
            throw std::runtime_error("Authentication failed. Response: " + authResponse.dump());
        }
        std::string token = authResponse["access_token"].get<std::string>();
        std::cout << "✓ Authenticated.\n";

        // 2. Fetch & Cache User Profile
        std::cout << "\n[2/3] Fetching User Profile...\n";
        auto profileResponse = getResponse(API_HOST, API_PORT, token, "{}", "/api/GetUserProfile");
        
        // Cache Profile
        std::string profileKey = "USER_PROFILE_" + username;
        g_redisClient.setCompressed(profileKey, profileResponse.dump(), 0);
        std::cout << "✓ Cached User Profile to Redis (Key: " << profileKey << ")\n";

        // 3. Extract Org ID and Fetch Org Data
        std::string currentOrgIdStr;
        
        // Logic matched to fetchAPI.cpp ParseUserProfileFromJson
        if (profileResponse.contains("data") && profileResponse["data"].is_array() && !profileResponse["data"].empty()) {
            auto& item = profileResponse["data"][0];
            if (item.contains("currentOrgId")) {
                if (item["currentOrgId"].is_number()) {
                    currentOrgIdStr = std::to_string(item["currentOrgId"].get<int>());
                } else if (item["currentOrgId"].is_string()) {
                    currentOrgIdStr = item["currentOrgId"].get<std::string>();
                }
            }
        } 
        
        if (!currentOrgIdStr.empty()) {
            int orgId = std::stoi(currentOrgIdStr);
            std::cout << "\n[3/3] Identified Organization ID: " << orgId << ". Fetching Org Data...\n";
            updateOrgData(orgId, token);
            std::cout << "\n✓ Success! All data cached for user '" << username << "'.\n";
        } else {
            std::cout << "\n[Warning] 'currentOrgId' not found in profile 'data[0]'. Skipping Org Data fetch.\n";
        }

    } catch (const std::exception& e) {
        std::cerr << "❌ Error: " << e.what() << "\n";
    }
}

// Helper to get Admin Token
std::string getAdminToken() {
    std::string encryptedPass = GetEncryptedString("", DEFAULT_API_PASS, 0);
    json authResponse = getBearerToken(API_HOST, API_PORT, DEFAULT_API_USER, encryptedPass);
    if (authResponse.contains("access_token")) {
        return authResponse["access_token"].get<std::string>();
    }
    throw std::runtime_error("Admin Auth Failed");
}

// ----------------------------------------------------------------------------
// Main
// ----------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    setupLogging();
    
    if (!loadConfiguration()) {
        std::cout << "Using hardcoded defaults as fallback (Risk of mismatch)...\n";
    }

    // Initialize Redis Client
    g_redisClient.init(REDIS_HOST, REDIS_PORT, REDIS_PASS, REDIS_DB, REDIS_PREFIX);
    if (!g_redisClient.connect()) {
        std::cerr << "Failed to connect to Redis server.\n";
        // Continue? The API calls don't strictly need Redis connected to succeed, but caching will fail.
        // We will return 1.
        return 1;
    }

    std::string command = "login"; // Default to interactive login
    if (argc > 1) command = argv[1];

    try {
        if (command == "login" || command == "update_user") {
            std::string username;
            std::string password;

            // 1. Get Username
            if (argc >= 3) {
                username = argv[2];
            } else {
                std::cout << "Enter Username: ";
                std::getline(std::cin, username);
            }

            // 2. Get Password
            if (argc >= 4) {
                password = argv[3];
            } else {
                std::cout << "Enter Password: ";
                std::getline(std::cin, password);
            }

            if (username.empty() || password.empty()) {
                std::cerr << "Username and password are required.\n";
                return 1;
            }

            updateAllForUser(username, password);
        }
        else if (command == "update_org") {
            if (argc < 3) {
                std::cerr << "Usage: update_org <orgId>\n";
                return 1;
            }
            int orgId = std::stoi(argv[2]);
            std::string token = getAdminToken(); 
            updateOrgData(orgId, token);
        }
        else if (command == "set") {
             if (argc < 4) return 1;
             g_redisClient.set(argv[2], argv[3], 0);
             std::cout << "OK\n";
        }
        else if (command == "setfile") {
             if (argc < 4) return 1;
             std::ifstream t(argv[3]);
             std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
             g_redisClient.set(argv[2], str, 0);
             std::cout << "OK\n";
        } 
        else {
            printUsage(argv[0]);
            return 1;
        }

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
