#pragma once
#include <iostream>
#include <string>
#include <fstream>
#include <mutex>
#include <chrono>
#include <ctime>
#include <iomanip>

extern bool g_debug;  

enum LogLevel { INFO, DEBUG, ERRORS };

inline std::string &logFilePathRef() {
    static std::string path = "client.log";
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

inline void set_log_file(const std::string &path) {
    std::lock_guard<std::mutex> lock(logMutexRef());
    logFilePathRef() = path;
    auto &s = logStreamRef();
    if(s.is_open()) s.close();
    s.open(logFilePathRef(), std::ios::out | std::ios::app);
}

inline void ensure_log_stream_open() {
    auto &s = logStreamRef();
    if(!s.is_open()) {
        s.open(logFilePathRef(), std::ios::out | std::ios::app);
    }
}

inline void log(const std::string &msg, LogLevel level = LogLevel::INFO) {
    // Timestamp
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf;
#if defined(_WIN32)
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&t, &tm_buf);
#endif

    const char *lvl = level == LogLevel::INFO ? "INFO" : (level == LogLevel::DEBUG ? "DEBUG" : "ERROR");

    {
        std::lock_guard<std::mutex> lock(logMutexRef());
        ensure_log_stream_open();
        auto &s = logStreamRef();
        if(s.good()) {
            s << '[' << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S") << "] " << '[' << lvl << "] " << msg << std::endl;
            s.flush();
        }
    }

    if (g_debug) {
        switch(level) {
            case LogLevel::INFO:
                std::cout << "[INFO] " << msg << std::endl;
                std::cout << std::endl;
                break;
            case LogLevel::DEBUG:
                std::cout << "[DEBUG] " << msg << std::endl;
                std::cout << std::endl;
                break;
            case LogLevel::ERRORS:
                std::cerr << "[ERROR] " << msg << std::endl;
                std::cout << std::endl;
                break;
        }
    }
}
