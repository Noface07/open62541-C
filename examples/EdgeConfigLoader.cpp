#include "EdgeConfigLoader.h"
#include "Encryption.h"
#include "Logger.h"

#include <nlohmann/json.hpp>

#include <fstream>
#include <stdexcept>
#include <string>
#include <filesystem>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <dirent.h>
#include <unistd.h>
#endif

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// Return the directory containing the running executable
// ---------------------------------------------------------------------------
static std::string GetExeDir() {
#ifdef _WIN32
    char path[MAX_PATH];
    DWORD len = GetModuleFileNameA(nullptr, path, MAX_PATH);
    if (len == 0) return ".";
    std::string p(path, len);
    auto pos = p.find_last_of("\\/");
    return (pos != std::string::npos) ? p.substr(0, pos) : ".";
#else
    char path[4096];
    ssize_t len = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (len <= 0) return ".";
    path[len] = '\0';
    std::string p(path);
    auto pos = p.rfind('/');
    return (pos != std::string::npos) ? p.substr(0, pos) : ".";
#endif
}

// ---------------------------------------------------------------------------
// Internal helper: find the first file whose name starts with "EdgeConfig_"
// Searches: (1) current working directory, (2) executable directory
// ---------------------------------------------------------------------------
static std::string FindEdgeConfigFile(const std::string& prefix = "EdgeConfig_") {
    // Directories to search in order
    std::vector<std::string> searchDirs = {".", GetExeDir()};

    for (const auto& dir : searchDirs) {
#ifdef _WIN32
        std::string pattern = dir + "\\" + prefix + "*";
        WIN32_FIND_DATAA findData;
        HANDLE hFind = FindFirstFileA(pattern.c_str(), &findData);
        if (hFind != INVALID_HANDLE_VALUE) {
            std::string result = dir + "\\" + findData.cFileName;
            FindClose(hFind);
            return result;
        }
#else
        DIR* d = opendir(dir.c_str());
        if (!d) continue;
        struct dirent* entry;
        while ((entry = readdir(d)) != nullptr) {
            std::string name = entry->d_name;
            if (name.rfind(prefix, 0) == 0) {
                closedir(d);
                return dir + "/" + name;
            }
        }
        closedir(d);
#endif
    }
    return "";
}

// ---------------------------------------------------------------------------
// LoadEdgeConfig
// ---------------------------------------------------------------------------
EdgeConfigData LoadEdgeConfig(const std::string& prefix) {
    std::string exeDir = GetExeDir();
    std::cerr  << "[EdgeConfig] Executable directory : " << exeDir << std::endl;
    log("EdgeConfigLoader: Executable directory: " + exeDir, LogLevel::INFO);

    // 1. Find the EdgeConfig file
    std::cerr  << "[EdgeConfig] Searching for " << prefix << "* file ..." << std::endl;
    log("EdgeConfigLoader: Searching for " + prefix + "* file in CWD and exe directory ...", LogLevel::INFO);

    std::string fileName = FindEdgeConfigFile(prefix);
    if (fileName.empty()) {
        std::string msg =
            "EdgeConfigLoader: FAILED — No file matching 'EdgeConfig_*' found.\n"
            "  Searched: current working directory AND " + exeDir + "\n"
            "  Place the EdgeConfig_*.txt file in the same folder as server_cpp.exe.";
        std::cerr  << "[EdgeConfig] ERROR: " << msg << std::endl;
        log(msg, LogLevel::ERRORS);
        throw std::runtime_error(msg);
    }

    std::cerr  << "[EdgeConfig] Found file: " << fileName << std::endl;
    log("EdgeConfigLoader: Found config file: " + fileName, LogLevel::INFO);

    // 2. Read the encrypted content (single-line base64 ciphertext)
    std::cerr  << "[EdgeConfig] Opening file ..." << std::endl;
    std::ifstream ifs(fileName);
    if (!ifs.is_open()) {
        std::string msg = "EdgeConfigLoader: FAILED — Cannot open file: " + fileName;
        std::cerr  << "[EdgeConfig] ERROR: " << msg << std::endl;
        log(msg, LogLevel::ERRORS);
        throw std::runtime_error(msg);
    }

    std::string ciphertext;
    std::getline(ifs, ciphertext);
    ifs.close();

    // Strip any trailing whitespace / CR
    while (!ciphertext.empty() &&
           (ciphertext.back() == '\r' || ciphertext.back() == '\n' ||
            ciphertext.back() == ' ')) {
        ciphertext.pop_back();
    }

    std::cerr  << "[EdgeConfig] Read " << ciphertext.size() << " bytes of ciphertext." << std::endl;
    log("EdgeConfigLoader: Read " + std::to_string(ciphertext.size()) + " bytes of ciphertext from " + fileName, LogLevel::INFO);

    if (ciphertext.empty()) {
        std::string msg = "EdgeConfigLoader: FAILED — File is empty: " + fileName;
        std::cerr  << "[EdgeConfig] ERROR: " << msg << std::endl;
        log(msg, LogLevel::ERRORS);
        throw std::runtime_error(msg);
    }

    // 3. Decrypt — default key "TechDC0nf!g" is baked into GetDecryptedString
    std::cerr  << "[EdgeConfig] Decrypting content ..." << std::endl;
    log("EdgeConfigLoader: Calling GetDecryptedString ...", LogLevel::INFO);

    std::string plaintext = GetDecryptedString("", ciphertext, 0);

    if (plaintext.empty()) {
        std::string msg =
            "EdgeConfigLoader: FAILED — Decryption returned empty string.\n"
            "  File: " + fileName + "\n"
            "  Ciphertext length: " + std::to_string(ciphertext.size()) + "\n"
            "  Check that the encryption key and Base64 ciphertext are correct.";
        std::cerr  << "[EdgeConfig] ERROR: " << msg << std::endl;
        log(msg, LogLevel::ERRORS);
        throw std::runtime_error(msg);
    }

    std::cerr  << "[EdgeConfig] Decrypted OK (" << plaintext.size() << " chars). Parsing JSON ..." << std::endl;
    log("EdgeConfigLoader: Decrypted config successfully (" +
        std::to_string(plaintext.size()) + " chars). Raw JSON: " + plaintext.substr(0, 120),
        LogLevel::INFO);

    // 4. Parse JSON
    json j;
    try {
        j = json::parse(plaintext);
    } catch (const std::exception& e) {
        std::string msg =
            std::string("EdgeConfigLoader: FAILED — JSON parse error: ") + e.what() +
            "\n  Plaintext (first 200 chars): " + plaintext.substr(0, 200);
        std::cerr  << "[EdgeConfig] ERROR: " << msg << std::endl;
        log(msg, LogLevel::ERRORS);
        throw std::runtime_error(msg);
    }

    // 5. Populate struct — throw on missing required fields
    auto requireString = [&](const std::string& key) -> std::string {
        if (!j.contains(key) || !j[key].is_string()) {
            std::string msg = "EdgeConfigLoader: FAILED — Missing/invalid field '" + key + "' in EdgeConfig JSON.";
            std::cerr  << "[EdgeConfig] ERROR: " << msg << std::endl;
            log(msg, LogLevel::ERRORS);
            throw std::runtime_error(msg);
        }
        return j[key].get<std::string>();
    };

    EdgeConfigData cfg;
    if (j.contains("Id") && j["Id"].is_number_integer()) {
        cfg.id = j["Id"].get<int>();
    }
    cfg.name       = requireString("Name");
    cfg.shortCode  = requireString("ShortCode");
    cfg.ip         = requireString("IP");
    cfg.authType   = requireString("AuthenticationType");
    cfg.username   = requireString("UserName");
    cfg.password   = requireString("Password");
    cfg.clientId   = requireString("ClientId");
    if (j.contains("EdgentType") && j["EdgentType"].is_string()) {
        cfg.edgentType = j["EdgentType"].get<std::string>();
    }

    std::cerr  << "[EdgeConfig] ✓ Config loaded successfully!" << std::endl;
    std::cerr  << "[EdgeConfig]   Id        : " << cfg.id << std::endl;
    std::cerr  << "[EdgeConfig]   Name      : " << cfg.name << std::endl;
    std::cerr  << "[EdgeConfig]   ShortCode : " << cfg.shortCode << std::endl;
    std::cerr  << "[EdgeConfig]   IP        : " << cfg.ip << std::endl;
    std::cerr  << "[EdgeConfig]   UserName  : " << cfg.username << std::endl;
    std::cerr  << "[EdgeConfig]   ClientId  : " << cfg.clientId << std::endl;

    log("EdgeConfigLoader: ✓ Config loaded — Id=" + std::to_string(cfg.id) +
        ", ShortCode=" + cfg.shortCode +
        ", UserName=" + cfg.username +
        ", ClientId=" + cfg.clientId,
        LogLevel::INFO);

    return cfg;
}

// ---------------------------------------------------------------------------
// SetEdgeConfigMqttPassword
// ---------------------------------------------------------------------------
void SetEdgeConfigMqttPassword(EdgeConfigData& cfg, const std::string& bearerToken) {
    cfg.mqttUsername = cfg.username;

    // MQTT password = UTF-8 bytes of JSON: {"token":"<bearerToken>"}
    // Matches the C# implementation: JsonConvert.SerializeObject(new { token = bearerToken })
    // then Encoding.UTF8.GetBytes(passwordJson)
    // std::string in C++ is a byte sequence, so passing it directly is correct.
    json passwordPayload;
    passwordPayload["token"] = bearerToken;
    cfg.mqttPassword = passwordPayload.dump();   // e.g. {"token":"eyJ..."}

    std::cerr  << "[EdgeConfig] MQTT credentials set — User=" << cfg.mqttUsername
               << ", Password={token:<bearer token " << (bearerToken.empty() ? "EMPTY" : "set")
               << ">} " << cfg.mqttPassword << "the following is the password"  << std::endl;
    log("EdgeConfigLoader: MQTT credentials set — User=" + cfg.mqttUsername +
        (bearerToken.empty() ? " (WARNING: empty bearer token)" : ""),
        LogLevel::INFO);
}

