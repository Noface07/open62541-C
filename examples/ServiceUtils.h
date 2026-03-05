#pragma once
#ifdef _WIN32
#include <windows.h>
#endif
#include <iostream>
#include <string>

// Service Configuration
#define SERVICE_NAME  "AnexeeServer"
#define DISPLAY_NAME  "Anexee OPC UA Server"

// Global definition of the main server loop function
// To be implemented in server.cpp
int RunServer(int argc, char **argv);

// Service Control functions implementation
// Use inline to prevent linker errors if included in multiple places

inline bool InstallService(const std::string& serviceName, const std::string& displayName) {
#ifdef _WIN32
    std::string path;
    char buffer[MAX_PATH];
    if (GetModuleFileNameA(NULL, buffer, MAX_PATH) > 0) {
        path = std::string(buffer);
        // Add --service flag to the path
        path = "\"" + path + "\" --service";
    } else {
        std::cerr << "Cannot get executable path." << std::endl;
        return false;
    }

    SC_HANDLE hSCManager = OpenSCManager(NULL, NULL, SC_MANAGER_CREATE_SERVICE);
    if (!hSCManager) {
        DWORD err = GetLastError();
        std::cerr << "OpenSCManager failed: " << err << std::endl;
        if (err == 5) {
            std::cerr << "TIP: Error 5 means 'Access Denied'. Please run this command as Administrator." << std::endl;
        }
        return false;
    }

    SC_HANDLE hService = CreateServiceA(
        hSCManager, 
        serviceName.c_str(), 
        displayName.c_str(), 
        SERVICE_ALL_ACCESS, 
        SERVICE_WIN32_OWN_PROCESS, 
        SERVICE_AUTO_START, 
        SERVICE_ERROR_NORMAL, 
        path.c_str(), 
        NULL, NULL, NULL, NULL, NULL
    );

    if (!hService) {
        std::cerr << "CreateService failed: " << GetLastError() << std::endl;
        CloseServiceHandle(hSCManager);
        return false;
    }

    std::cout << "Service installed successfully." << std::endl;
    CloseServiceHandle(hService);
    CloseServiceHandle(hSCManager);
    return true;
#else
    std::cerr << "Service installation is only supported on Windows. On Linux, please use systemd." << std::endl;
    return false;
#endif
}

inline bool UninstallService(const std::string& serviceName) {
#ifdef _WIN32
    SC_HANDLE hSCManager = OpenSCManager(NULL, NULL, SC_MANAGER_CONNECT);
    if (!hSCManager) {
        std::cerr << "OpenSCManager failed: " << GetLastError() << std::endl;
        return false;
    }

    SC_HANDLE hService = OpenServiceA(hSCManager, serviceName.c_str(), DELETE);
    if (!hService) {
        std::cerr << "OpenService failed: " << GetLastError() << std::endl;
        CloseServiceHandle(hSCManager);
        return false;
    }

    if (!DeleteService(hService)) {
        std::cerr << "DeleteService failed: " << GetLastError() << std::endl;
        CloseServiceHandle(hService);
        CloseServiceHandle(hSCManager);
        return false;
    }

    std::cout << "Service uninstalled successfully." << std::endl;
    CloseServiceHandle(hService);
    CloseServiceHandle(hSCManager);
    return true;
#else
    std::cerr << "Service uninstallation is only supported on Windows." << std::endl;
    return false;
#endif
}
