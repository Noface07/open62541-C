#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <optional>
#include "SqliteQueueService.h"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include "MQTThandler.h" // Needed for MQTThandler pointer in AsyncPublisher

using namespace std;

// Forward declaration if explicit include causes cycle, but MQTThandler.h has pragma once
// class MQTTHandler; 

struct AsyncMessage {
    std::string topic;
    std::string payload;
};

class AsyncPublisher {
public:
    AsyncPublisher(MQTTHandler* handler) : mqttHandler(handler), running(true) {
        worker = std::thread(&AsyncPublisher::processQueue, this);
    }

    ~AsyncPublisher() {
        running = false;
        cv.notify_all();
        if (worker.joinable()) worker.join();
    }

    void enqueue(std::string topic, std::string payload) {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            messageQueue.push({topic, payload});
        }
        cv.notify_one();
    }

private:
    void processQueue() {
        while (running) {
            std::unique_lock<std::mutex> lock(queueMutex);
            cv.wait(lock, [this] { return !messageQueue.empty() || !running; });

            while (!messageQueue.empty()) {
                // Process batch or single? Single for now as per design
                AsyncMessage msg = messageQueue.front();
                messageQueue.pop();
                
                lock.unlock(); // Improve concurrency
                
                if (mqttHandler && mqttHandler->isConnected()) {
                    mqttHandler->publish(msg.topic, msg.payload);
                }
                
                if(running) lock.lock(); 
            }
        }
    }

    std::queue<AsyncMessage> messageQueue;
    std::mutex queueMutex;
    std::condition_variable cv;
    std::thread worker;
    MQTTHandler* mqttHandler;
    std::atomic<bool> running;
};

struct MappedInfospaceTag{
    int id;
    int tagId;
    string name;
    string namespaces;
    int samplingInterval;
    int deadband;
    int queuesize;
    int orgId;


    bool scaling;
    double rawMin;
    double rawMax;
    double scaleMin;
    double scaleMax;
    string sourceDatatype;
    bool enableExpression;
    string expression;


    // bool isSimulationProfile;
    // bool isLogging;
    // bool isVirtual;
};

struct TagInfo{
    bool scaling;
    double rawMin;
    double rawMax;
    double scaleMin;
    double scaleMax;
    string sourceDatatype;
    bool enableExpression;
    int samplingInterval;
    int deadband;
    int queuesize;
    string rdWtOpt;
    string expression;
    optional<string> namespaceNodeID;
    optional<vector<MappedInfospaceTag>> mappedInfospaceTags;
    int dataPointId;
    optional<string> name;
    optional<string> nodeId;
    optional<string> typeId;
    optional<string> parentId;
};

struct GroupInfo{
    vector<TagInfo> tags;
    int dataPointId;
    string typeId;
    string parentId;
    string name;
    string nodeId;
    int publishingInterval;
    int lifetimeCount;
    int maxKeepAliveCount;
    int priority;
    int maxNotificationsPerPublish;
};

struct ServerInfoO{
    string cfgName;
    string endpointUrl;
    string securityPolicy;
    string msgSecurityMode;
    string authType;
    vector<GroupInfo> groups;
    // vector<TagInfo> tags;
    int dataPointId;
    string name;
    string nodeId;
    string typeId;
    string parentId;
    string username;
    string password;
    string certificate;
    string privateKey;
    string sessionName;
};

#ifdef min
#undef min
#endif
#ifdef max
#undef max
#endif
#include <exprtk.hpp>

struct ExprTkContext {
    double value;
    exprtk::symbol_table<double> symbol_table;
    exprtk::expression<double> expression;
};

struct MyMonitorContext {
    MappedInfospaceTag infoSpace;
    MQTTHandler* mqttHandler;
    SqliteQueueService* sqliteService;
    std::shared_ptr<ExprTkContext> exprContext;
    std::shared_ptr<AsyncPublisher> asyncPublisher; // Helper for non-blocking publish
    // unordered_map<int, string> TopicMapping;
    // json payload;
    // Add more fields as needed
};


