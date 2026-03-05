#ifdef _WIN32
#ifndef _WIN32_WINNT
#define _WIN32_WINNT 0x0601 // Prevent boost/asio warning
#endif
#endif
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
#include "SqliteQueueService.h"
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

// SQLite service for database sync
SqliteQueueService *g_sqliteUpdater = nullptr;
std::string DB_PATH = "OfflineData.db";

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
        
        // Extract DB Path (same as server.cpp)
        if (config.contains("Payload") && config["Payload"].contains("OfflineQueueOptions") 
            && config["Payload"]["OfflineQueueOptions"].contains("DbPath")) {
            DB_PATH = config["Payload"]["OfflineQueueOptions"]["DbPath"].get<std::string>();
        }
        
        std::cout << "✓ Config Loaded. API Host: " << API_HOST << ":" << API_PORT << "\n";
        std::cout << "   DB Path: " << DB_PATH << "\n";
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
        // Also save to local database
        if (g_sqliteUpdater) {
            try {
                g_sqliteUpdater->SetConfig(key, response.dump());
                std::cout << "   [DB] Set " << key << "\n";
            } catch (const std::exception& ex) {
                std::cerr << "   [DB Error] " << ex.what() << "\n";
            }
        }
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
        // Also save to local database
        if (g_sqliteUpdater) {
            try {
                g_sqliteUpdater->SetConfig(key, response.dump());
                std::cout << "   [DB] Set " << key << "\n";
            } catch (const std::exception& ex) {
                std::cerr << "   [DB Error] " << ex.what() << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "   [Error] GetAlarmsConfigDetailList Failed: " << e.what() << "\n";
    }
}

// Helper to get Token only
std::string loginUser(const std::string& username, const std::string& password) {
    std::cout << "\n   [Auth] Authenticating as '" << username << "'...\n";
    std::string encryptedPass = GetEncryptedString("", password, 0);
    json authResponse = getBearerToken(API_HOST, API_PORT, username, encryptedPass);
    
    if (authResponse.contains("access_token")) {
        std::cout << "   [Auth] Success.\n";
        return authResponse["access_token"].get<std::string>();
    }
    throw std::runtime_error("Authentication failed. Response: " + authResponse.dump());
}

// Fetch Profile and Update Org (Assuming Token is valid)
void updateUserProfileAndOrg(const std::string& username, const std::string& token) {
     try {
        // 1. Fetch & Cache User Profile
        std::cout << "\n[1/2] Fetching User Profile...\n";
        auto profileResponse = getResponse(API_HOST, API_PORT, token, "{}", "/api/GetUserProfile");
        
        // Cache Profile
        std::string profileKey = "USER_PROFILE_" + username;
        g_redisClient.setCompressed(profileKey, profileResponse.dump(), 0);
        std::cout << "✓ Cached User Profile to Redis (Key: " << profileKey << ")\n";

        // 2. Extract Org ID and Fetch Org Data
        std::string currentOrgIdStr;
        
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
            std::cout << "\n[2/2] Identified Organization ID: " << orgId << ". Fetching Org Data...\n";
            updateOrgData(orgId, token);
            std::cout << "\n✓ Success! All data cached for user '" << username << "'.\n";
        } else {
            std::cout << "\n[Warning] 'currentOrgId' not found in profile 'data[0]'. Skipping Org Data fetch.\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "❌ Error: " << e.what() << "\n";
    }
}

// Kept for backward compatibility with CLI arg usage
void updateAllForUser(const std::string& username, const std::string& password) {
    std::string token = loginUser(username, password);
    updateUserProfileAndOrg(username, token);
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

// Helper to update Hierarchy Data
void updateHierarchy(int orgId, const std::string& nodeId, const std::string& token) {
    std::cout << "\n   [API] Fetching Hierarchy for NodeID " << nodeId << " (Org " << orgId << ")...\n";
    
    try {
        json body;
        body["orgId"] = 0; 
        body["roleId"] = ""; 
        body["userId"] = 0;
        body["moduleId"] = 0;
        body["userType"] = "";
        body["requestDateTime"] = "2024-12-26T08:16:05.629Z";
        body["ipAddress"] = "";
        body["originName"] = "";
        body["filterModel"]["customValue"] = nodeId;
        
        // Use /api/GetOpcUaHierarchy
        auto response = getResponse(API_HOST, API_PORT, token, body.dump(), "/api/GetOpcUaHierarchy");
        
        std::string key = "OPCUA_HIERARCHY_" + nodeId;
        g_redisClient.setCompressed(key, response.dump(), 0);
        std::cout << "   [Redis] Set " << key << " (Size: " << response.dump().size() << ")\n";
        // Also save to local database
        if (g_sqliteUpdater) {
            try {
                g_sqliteUpdater->SetConfig(key, response.dump());
                std::cout << "   [DB] Set " << key << "\n";
            } catch (const std::exception& ex) {
                std::cerr << "   [DB Error] " << ex.what() << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "   [Error] GetOpcUaHierarchy Failed: " << e.what() << "\n";
    }
}

// Helper to update Server Configs (GetOpcUaServersWithOrgMappings)
void updateServerConfigs(const std::string& nodeId, const std::string& token) {
    std::cout << "\n   [API] Fetching Server Configs for NodeID " << nodeId << "...\n";
    
    try {
        json body;
        body["data"]["nodeId"] = nodeId;
        
        auto response = getResponse(API_HOST, API_PORT, token, body.dump(), "/api/GetOpcUaServersWithOrgMappings");
        
        std::string key = "SERVER_CONFIGS_" + nodeId;
        g_redisClient.setCompressed(key, response.dump(), 0);
        std::cout << "   [Redis] Set " << key << " (Size: " << response.dump().size() << ")\n";

        // Cache the raw API response to local database
        if (g_sqliteUpdater) {
            try {
                // Store the full API response - server.cpp will parse it with ServerConfigFromJSON on load
                if(response.contains("data") && response["data"].is_array()) {
                    g_sqliteUpdater->SetConfig(key, response["data"].dump());
                } else {
                    g_sqliteUpdater->SetConfig(key, response.dump());
                }
                std::cout << "   [DB] Set " << key << "\n";
            } catch (const std::exception& ex) {
                std::cerr << "   [DB Error] " << ex.what() << "\n";
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "   [Error] GetOpcUaServersWithOrgMappings Failed: " << e.what() << "\n";
    }
}

int main(int argc, char* argv[]) {
    setupLogging();
    
    if (!loadConfiguration()) {
        std::cout << "Using hardcoded defaults as fallback (Risk of mismatch)...\n";
    }

    // Initialize Redis Client
    g_redisClient.init(REDIS_HOST, REDIS_PORT, REDIS_PASS, REDIS_DB, REDIS_PREFIX);
    if (!g_redisClient.connect()) {
        std::cerr << "Failed to connect to Redis server.\n";
        return 1;
    }

    // Initialize SQLite for database sync
    try {
        OfflineQueueOptions opts;
        opts.batchSize = 100;
        opts.uploadIntervalSeconds = 15;
        g_sqliteUpdater = new SqliteQueueService(DB_PATH, opts);
        std::cout << "[DB] SQLite database initialized at " << DB_PATH << "\n";
    } catch (const std::exception& e) {
        std::cerr << "[DB Warning] Could not initialize SQLite: " << e.what() << ". DB sync will be disabled.\n";
    }

    // CLI Arguments Handling
    if (argc > 1) {
        std::string command = argv[1];
        try {
            if (command == "login" || command == "update_user") {
                if (argc < 4) { std::cerr << "Usage: update_user <user> <pass>\n"; return 1; }
                updateAllForUser(argv[2], argv[3]);
            }
            else if (command == "update_org") {
                if (argc < 3) { std::cerr << "Usage: update_org <orgId>\n"; return 1; }
                updateOrgData(std::stoi(argv[2]), getAdminToken());
            }
            else if (command == "update_hierarchy") {
                if (argc < 4) { std::cerr << "Usage: update_hierarchy <orgId> <nodeId>\n"; return 1; }
                updateHierarchy(std::stoi(argv[2]), argv[3], getAdminToken());
            }
            else if (command == "set") {
                if (argc < 4) return 1;
                g_redisClient.set(argv[2], argv[3], 0);
            }
            else if (command == "setfile") {
                if (argc < 4) return 1;
                std::ifstream t(argv[3]);
                std::string str((std::istreambuf_iterator<char>(t)), std::istreambuf_iterator<char>());
                g_redisClient.set(argv[2], str, 0);
            }
        } catch (const std::exception& e) {
            std::cerr << "CLI Error: " << e.what() << "\n";
            return 1;
        }
        return 0;
    }

    // Interactive Mode (Linear Flow)
    std::string sessionUser;
    std::string sessionPass;
    std::string sessionNodeID;

    std::cout << "\n=== RedisUpdater: Update All Cache ===\n";
    
    // 1. Get Username
    std::cout << "1) Enter Username: ";
    std::getline(std::cin, sessionUser);
    if (sessionUser.empty()) return 0;

    // 2. Get Password
    std::cout << "2) Enter Password: ";
    std::getline(std::cin, sessionPass);
    if (sessionPass.empty()) return 0;

    // 3. Get Node ID
    std::cout << "3) Enter Node ID: ";
    std::getline(std::cin, sessionNodeID);
    if (sessionNodeID.empty()) return 0;

    try {
        std::cout << "\n--- Starting Update Process ---\n";

        // Step 1: Login
        std::string token = loginUser(sessionUser, sessionPass);

        // Step 2: Update Profile -> Get Org ID -> Update Org Data
        // We need to modify updateUserProfileAndOrg or do it manually here to capture the OrgID
        // Let's refactor inline for clarity since we need the OrgID for Step 3
        
        // 2a. Fetch Profile
        std::cout << "\n[Step 2] Fetching User Profile...\n";
        auto profileResponse = getResponse(API_HOST, API_PORT, token, "{}", "/api/GetUserProfile");
        
        std::string profileKey = "USER_PROFILE_" + sessionUser;
        g_redisClient.setCompressed(profileKey, profileResponse.dump(), 0);
        std::cout << "   [Redis] Set " << profileKey << "\n";
        // Also save to local database
        if (g_sqliteUpdater) {
            try {
                g_sqliteUpdater->SetConfig(profileKey, profileResponse.dump());
                std::cout << "   [DB] Set " << profileKey << "\n";
            } catch (const std::exception& ex) {
                std::cerr << "   [DB Error] " << ex.what() << "\n";
            }
        }

        // 2b. Extract Org ID
        int orgId = 0;
        if (profileResponse.contains("data") && profileResponse["data"].is_array() && !profileResponse["data"].empty()) {
            auto& item = profileResponse["data"][0];
            if (item.contains("currentOrgId")) {
                if (item["currentOrgId"].is_number()) orgId = item["currentOrgId"].get<int>();
                else if (item["currentOrgId"].is_string()) orgId = std::stoi(item["currentOrgId"].get<std::string>());
            }
        }

        if (orgId == 0) {
            throw std::runtime_error("Could not determine Organization ID from User Profile.");
        }
        std::cout << "   [Info] Identified Organization ID: " << orgId << "\n";

        // Step 3: Update Org Data (Topics, Alarms)
        std::cout << "\n[Step 3] Fetching Organization Data (Topics & Alarms)...\n";
        updateOrgData(orgId, token);

        // Step 4: Update Hierarchy
        std::cout << "\n[Step 4] Fetching Hierarchy Data...\n";
        updateHierarchy(orgId, sessionNodeID, token);

        // Step 5: Update Server Configs
        std::cout << "\n[Step 5] Fetching Server Configs...\n";
        updateServerConfigs(sessionNodeID, token);

        std::cout << "\n=== SUCCESS: All data updated in Redis & DB! ==="  << "\n";

    } catch (const std::exception& e) {
        std::cerr << "\n❌ FAIL: " << e.what() << "\n";
    }

    std::cout << "\nPress Enter to exit...";
    std::string dummy;
    std::getline(std::cin, dummy);

    // Cleanup
    delete g_sqliteUpdater;
    g_sqliteUpdater = nullptr;
    
    return 0;
}
