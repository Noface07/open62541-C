#pragma once
#include <mutex>
#include <chrono>
#include <string>
#include <iostream>
#include "Logger.h"

class InstrumentedMutex {
private:
    std::mutex mtx;
    std::string name;
    const int64_t WARN_THRESHOLD_MS; 

public:
    InstrumentedMutex(std::string n) : name(n), WARN_THRESHOLD_MS(100) {}

    void lock() {
        // printf("DEBUG: Attempting to lock %s\n", name.c_str());
        auto start = std::chrono::high_resolution_clock::now();
        mtx.lock();
        auto end = std::chrono::high_resolution_clock::now();
        
        auto wait_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        if (wait_time > WARN_THRESHOLD_MS) {
             // Force stdout output with printf
             printf("[LOCK_WARN] Thread waited %lld ms (Threshold: %lld) to acquire %s\n", wait_time, WARN_THRESHOLD_MS, name.c_str());
             
            std::cout << "[LOCK_WARN] Thread waited " << wait_time 
                      << "ms to acquire " << name << std::endl;

            log("[LOCK_WARN] Thread waited " + std::to_string(wait_time) + 
                "ms to acquire " + name,
                LogLevel::WARNING);
        }
    }

    void unlock() {
        // You could also track hold time here by storing the lock-time in a thread-local variable
        mtx.unlock();
    }

    // Helper for lock_guard
    std::mutex& get_internal() { return mtx; }
    std::string getName() const { return name; }
    int64_t getThreshold() const { return WARN_THRESHOLD_MS; }
};

// Custom Guard to track HOLD time
struct InstrumentedGuard {
    InstrumentedMutex& iMtx;
    std::chrono::time_point<std::chrono::high_resolution_clock> start;

    InstrumentedGuard(InstrumentedMutex& m) : iMtx(m) {
        iMtx.lock();
        start = std::chrono::high_resolution_clock::now();
    }

    ~InstrumentedGuard() {
        auto end = std::chrono::high_resolution_clock::now();
        auto hold_time = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        
        if (hold_time > iMtx.getThreshold()) {
             printf("[HOLD_WARN] Mutex %s was held for %lld ms! (Threshold: %lld)\n", iMtx.getName().c_str(), hold_time, iMtx.getThreshold());
            std::cout << "[HOLD_WARN] Mutex " << iMtx.getName() 
                      << " was held for " << hold_time << "ms!" << std::endl;

            log("[HOLD_WARN] Mutex " + iMtx.getName() + " was held for " + std::to_string(hold_time) + "ms!",
                LogLevel::WARNING);
        }
        iMtx.unlock();
    }
};