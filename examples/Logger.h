#pragma once
#include <iostream>
#include <string>
#include <fstream>
#include <mutex>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <filesystem>
#include <sstream>
#include <cstring>

extern bool g_debug;  
extern bool g_logging_enabled;  // Global control for file logging

// Global variable definitions (will be overridden by applications)
inline bool g_debug = false;
inline bool g_logging_enabled = false;

enum LogLevel { INFO, DEBUG, ERRORS };

// Logging configuration structure
struct LogConfig {
    std::string baseFolder;
    std::string appName;
    bool createFolders;
    
    LogConfig(const std::string& folder = "", const std::string& name = "app", bool create = true) 
        : baseFolder(folder), appName(name), createFolders(create) {}
};

// Global log configuration
inline LogConfig& getLogConfig() {
    static LogConfig config;
    return config;
}

inline std::string &logFilePathRef() {
    static std::string path = "";
    return path;
}

inline std::ofstream &logStreamRef() {
    static std::ofstream stream;
    return stream;
}

inline std::mutex &logMutexRef() {
    static std::mutex m;
    return m;
}

// Create directory if it doesn't exist
inline bool createDirectoryIfNotExists(const std::string& path) {
    try {
        if (!std::filesystem::exists(path)) {
            return std::filesystem::create_directories(path);
        }
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating directory " << path << ": " << e.what() << std::endl;
        return false;
    }
}

// Generate day-wise log filename
inline std::string generateLogFilename() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf;
#if defined(_WIN32)
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&tm_buf, &t);
#endif
    
    // Use strftime for Windows to avoid put_time issues
    char filename[32];
#if defined(_WIN32)
    strftime(filename, sizeof(filename), "%Y-%m-%d", &tm_buf);
    return std::string(filename) + ".log";
#else
    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d") << ".log";
    return oss.str();
#endif
}

// Get full log path including folder and filename
inline std::string getFullLogPath() {
    auto& config = getLogConfig();
    std::string filename = generateLogFilename();
    
    if (config.baseFolder.empty()) {
        return filename;
    }
    
    std::string fullPath = config.baseFolder + "/" + filename;
    return fullPath;
}

// Initialize logging system
inline void init_logging(const std::string& baseFolder, const std::string& appName = "app", bool createFolders = true) {
    std::lock_guard<std::mutex> lock(logMutexRef());
    
    // Always initialize for ERROR messages, but only open stream if logging enabled
    
    auto& config = getLogConfig();
    config.baseFolder = baseFolder;
    config.appName = appName;
    config.createFolders = createFolders;
    
    // Create base folder if requested
    if (config.createFolders && !config.baseFolder.empty()) {
        if (!createDirectoryIfNotExists(baseFolder)) {
            std::cerr << "Warning: Could not create log directory: " << baseFolder << std::endl;
        }
    }
    
    // Set initial log file path
    logFilePathRef() = getFullLogPath();
    
    // Only open log stream if logging is enabled globally
    if (g_logging_enabled) {
        auto& s = logStreamRef();
        if (s.is_open()) s.close();
        s.open(logFilePathRef(), std::ios::out | std::ios::app);
        
        if (s.is_open()) {
            std::cout << "Logging initialized: " << logFilePathRef() << std::endl;
        } else {
            std::cerr << "Warning: Could not open log file: " << logFilePathRef() << std::endl;
        }
    }
}

// Legacy function for backward compatibility
inline void set_log_file(const std::string &path) {
    // Only set log file if logging is enabled globally
    if (!g_logging_enabled) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(logMutexRef());
    logFilePathRef() = path;
    auto &s = logStreamRef();
    if(s.is_open()) s.close();
    s.open(logFilePathRef(), std::ios::out | std::ios::app);
}

inline void ensure_log_stream_open() {
    // Always allow log stream to open for ERROR messages
    // For other messages, only open if logging is enabled globally
    if (!g_logging_enabled && logFilePathRef().empty()) {
        return;
    }
    
    auto &s = logStreamRef();
    if(!s.is_open()) {
        // Check if we need to rotate to a new day's log file
        std::string currentPath = getFullLogPath();
        if (currentPath != logFilePathRef()) {
            logFilePathRef() = currentPath;
        }
        s.open(logFilePathRef(), std::ios::out | std::ios::app);
    }
}

inline std::mutex &consoleMutexRef() {
    static std::mutex m;
    return m;
}

// ... (existing code)

inline void log(const std::string &msg, LogLevel level = LogLevel::INFO) {
    // Timestamp - using safer approach for Windows
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf;
#if defined(_WIN32)
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&tm_buf, &t);
#endif

    const char *lvl = level == LogLevel::INFO ? "INFO" : (level == LogLevel::DEBUG ? "DEBUG" : "ERROR");

    // Create timestamp string manually to avoid Windows put_time issues
    char timestamp[32];
#if defined(_WIN32)
    // Use strftime for Windows to avoid put_time issues
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &tm_buf);
#else
    // Use put_time for other platforms
    std::ostringstream oss;
    oss << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");
    strcpy(timestamp, oss.str().c_str());
#endif

    // Always log ERROR messages to file, regardless of g_logging_enabled
    // Other levels only log to file if logging is enabled globally
    bool shouldLogToFile = (level == LogLevel::ERRORS) || g_logging_enabled;
    
    if (shouldLogToFile) {
        std::lock_guard<std::mutex> lock(logMutexRef());
        ensure_log_stream_open();
        auto &s = logStreamRef();
        if(s.good()) {
            s << '[' << timestamp << "] " << '[' << lvl << "] " << msg << std::endl;
            s.flush();
        }
    }

    // Console output only when debug mode is enabled
    if (g_debug) {
        std::lock_guard<std::mutex> lock(consoleMutexRef());
        switch(level) {
            case LogLevel::INFO:
                std::cout << "[INFO] " << msg << std::endl;
                std::cout.flush(); // Ensure flush within lock
                break;
            case LogLevel::DEBUG:
                std::cout << "[DEBUG] " << msg << std::endl;
                std::cout.flush(); // Ensure flush within lock
                break;
            case LogLevel::ERRORS:
                std::cerr << "[ERROR] " << msg << std::endl;
                std::cerr.flush(); // Ensure flush within lock
                break;
        }
    }
}

// Convenience functions for different log levels
inline void log_info(const std::string& msg) { log(msg, LogLevel::INFO); }
inline void log_debug(const std::string& msg) { log(msg, LogLevel::DEBUG); }
inline void log_error(const std::string& msg) { log(msg, LogLevel::ERRORS); }

// Get current log file path
inline std::string get_current_log_path() {
    return logFilePathRef();
}

// Force log rotation (useful for testing or manual rotation)
inline void rotate_log() {
    // Only rotate log if logging is enabled globally
    if (!g_logging_enabled) {
        return;
    }
    
    std::lock_guard<std::mutex> lock(logMutexRef());
    auto& s = logStreamRef();
    if(s.is_open()) s.close();
    
    logFilePathRef() = getFullLogPath();
    s.open(logFilePathRef(), std::ios::out | std::ios::app);
}
