#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <optional>
#include <functional>
#include "SqliteQueueService.h"
#include <cstdint>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <chrono>
#include "MQTThandler.h" // Needed for MQTThandler pointer in AsyncPublisher

using namespace std;

// Forward declaration if explicit include causes cycle, but MQTThandler.h has pragma once
// class MQTTHandler; 

struct AsyncMessage {
    std::string topic;
    std::string payload;
};

class AsyncPublisher {
    static constexpr size_t MAX_PENDING = 50000;

    /** When MQTT is down, spill queued topic+wrapperJson to SQLite; return false to
     * re-queue the message in RAM. */
    using MqttDownSpillFn =
        std::function<bool(const std::string &, const std::string &)>;

public:
    explicit AsyncPublisher(MQTTHandler *handler,
                            MqttDownSpillFn mqttDownSpill = nullptr)
        : mqttHandler(handler), mqttDownSpill_(std::move(mqttDownSpill)), running(true) {
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
            if(messageDeque.size() >= MAX_PENDING)
                messageDeque.pop_front();
            messageDeque.push_back({std::move(topic), std::move(payload)});
        }
        cv.notify_one();
    }

private:
    void processQueue() {
        while (running) {
            std::unique_lock<std::mutex> lock(queueMutex);
            /* wait_for: when MQTT is down we re-queue below; without a timeout the
             * thread would sleep until another enqueue() even though the broker
             * may have reconnected. */
            cv.wait_for(lock, std::chrono::milliseconds(500),
                        [this] { return !messageDeque.empty() || !running; });

            if (!running && messageDeque.empty()) break;

            std::deque<AsyncMessage> localBatch;
            std::swap(messageDeque, localBatch);
            lock.unlock();

            if (localBatch.empty()) continue;

            if (mqttHandler && mqttHandler->isConnected()) {
                std::vector<std::pair<std::string, std::string>> batchMsgs;
                batchMsgs.reserve(localBatch.size());

                while (!localBatch.empty()) {
                    AsyncMessage msg = std::move(localBatch.front());
                    localBatch.pop_front();
                    batchMsgs.push_back({std::move(msg.topic), std::move(msg.payload)});
                }
                mqttHandler->publishBatch(std::move(batchMsgs));
            } else if (mqttDownSpill_) {
                std::deque<AsyncMessage> toRequeue;
                while (!localBatch.empty()) {
                    AsyncMessage msg = std::move(localBatch.front());
                    localBatch.pop_front();
                    if (!mqttDownSpill_(msg.topic, msg.payload))
                        toRequeue.push_back(std::move(msg));
                }
                if (!toRequeue.empty()) {
                    std::lock_guard<std::mutex> relock(queueMutex);
                    for (auto it = toRequeue.rbegin(); it != toRequeue.rend(); ++it) {
                        if (messageDeque.size() >= MAX_PENDING)
                            messageDeque.pop_front();
                        messageDeque.push_front(std::move(*it));
                    }
                }
            } else {
                /* No spill: hold in RAM until MQTT returns. */
                std::lock_guard<std::mutex> relock(queueMutex);
                for (auto it = localBatch.rbegin(); it != localBatch.rend(); ++it) {
                    if (messageDeque.size() >= MAX_PENDING)
                        messageDeque.pop_front();
                    messageDeque.push_front(std::move(*it));
                }
            }
        }
    }

    std::deque<AsyncMessage> messageDeque;
    std::mutex queueMutex;
    std::condition_variable cv;
    std::thread worker;
    MQTTHandler *mqttHandler;
    MqttDownSpillFn mqttDownSpill_;
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
    string tagType;
    bool enableExpression;
    string expression;

    // Hierarchy id of the parent OPC UA tag (used by hot reload to identify
    // which monitored items belong to a given hierarchy node).
    int hierarchyId = 0;

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
    // `dataPointId` is the group's hierarchy id at startup. The hot reload
    // MQTT API names the same concept `id`; both refer to the same numeric
    // value used to look up the group in groupIdToName.
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
    std::function<void(std::function<void()>)> postWorkerTask;
    // unordered_map<int, string> TopicMapping;
    // json payload;
    // Add more fields as needed
};

// ─────────────────────────────────────────────────────────────────────────────
// Hot reload command + runtime registry types
// ─────────────────────────────────────────────────────────────────────────────

// Parsed payload of an MQTT message on topic "HTRLD/Edgents".
// action ∈ { "Add", "Delete", "Update", "Reload" }.
//
// Payload shape:
//   Add / Update: single hierarchy id under "id"               → ids = { id }
//   Delete:       array of hierarchy ids under "item_collection"
//                 (single "id" tolerated for backwards compat) → ids = [...]
//   Reload:       no id required — applies to the entire client
struct HotReloadCommand {
    std::vector<int> ids;  // one or many hierarchy ids
    int orgId = 0;
    std::string reference; // e.g. "OPCUA"
    std::string purpose;   // e.g. "OPC_HI_SERVER" — maps to ServerInfoO::typeId
    std::string action;

    // Convenience: most call sites operate on a single id (Add / Update /
    // Reload). Returns 0 when ids is empty.
    int firstId() const { return ids.empty() ? 0 : ids.front(); }
};

// Single live monitored item handle produced at startup or by an Add action.
// Stored in ClientContext::liveRegistry indexed by hierarchyId so Delete /
// Update can locate every entry produced by a previous Add.
struct LiveMonitorEntry {
    int hierarchyId = 0;
    int tagId = 0;                // key into Mapping / TopicMapping
    std::uint32_t monitoredItemId = 0;
    std::uint32_t subscriptionId = 0;
    std::string groupName;
    std::string endpointUrl;
    MappedInfospaceTag infoSpace;
};


