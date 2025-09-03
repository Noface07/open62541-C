#ifndef SQLITE_QUEUE_SERVICE_H
#define SQLITE_QUEUE_SERVICE_H

#include <string>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <map>
#include <any>
#include <sqlite3.h>
#include "nlohmann/json.hpp"

// --- Data Models (mirroring C# Models) ---

struct MqttPayload {
    int datapointId;
    std::string name;
    int tagId;
    std::string value;
    std::string tagType;
    std::string timeStamp;
    std::string source;
    int infoId;
    std::string quality;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(MqttPayload, datapointId, tagId, value, tagType, timeStamp, source, infoId, quality)
};

struct QueueModel {
    int id;
    std::string topic;
    std::string payload; // JSON string of MqttPayload
    std::string createdAt;
};

struct QueueItem {
    std::string topic;
    MqttPayload payload;
    long orgId;
};

struct ApiTagData {
    long orgId;
    int tagId;
    std::string value;
    std::string tagType;
    std::string timeStamp;
    std::string source;
    int datapointId;
    int infoId;
    int quality;
    int updateType;

    NLOHMANN_DEFINE_TYPE_INTRUSIVE(ApiTagData, orgId, tagId, value, tagType, timeStamp, source, datapointId, infoId, quality, updateType)
};

struct OfflineQueueOptions {
    int batchSize = 100;
    int uploadIntervalSeconds = 10;
};

// --- Service Class Definition ---

class SqliteQueueService {
public:
    SqliteQueueService(const std::string& dbPath, const OfflineQueueOptions& options);
    ~SqliteQueueService();

    // Public API mirroring C# service
    void SetConfigurations(const std::map<int, long>& datapointToOrgIdMap);
    void EnqueueMessage(const std::string& topic, const MqttPayload& payload, long orgId);

    void StartQueueWorker();
    void StopQueueWorker();

    void StartApiUploadTimer();
    void StopApiUploadTimer();

    void TriggerOfflineDataProcessing();
    long GetRowCount();

    void PublishLatestValuesToMqtt(const std::function<void(const std::string& topic, const std::string& payload)>& publishFunc);

    void DisposeDB();

    // Configuration setters
    void SetApiUrl(const std::string& url);
    void SetApiAuth(const std::string& user, const std::string& pass);
    void SetApiMetadata(const nlohmann::json& metadata);
    
    // MQTT connection state management
    static void SetMqttConnected(bool connected);
    static bool IsMqttConnected();
    
    // Trigger API upload manually
    void TriggerApiUpload();


private:
    // Database operations
    void InitializeDatabase();
    void InsertMessageToDatabase(const QueueItem& item);
    std::vector<QueueModel> GetBatchQueuedMessages(int batchSize);
    std::vector<QueueModel> GetLatestValues();
    void DeleteMessages(const std::vector<int>& ids);
    void ClearLatestValues();

    // Worker threads
    void QueueWorkerLoop();
    void ApiUploadWorkerLoop();

    // Helper methods
    bool SendToApi(const std::string& jsonPayload);
    std::string GetCurrentTimestamp();
    void ExecuteDbCommand(const std::function<int(sqlite3*)>& command);

    // Member variables
    std::string dbPath_;
    OfflineQueueOptions options_;
    sqlite3* db_;

    // Queue worker members
    std::queue<QueueItem> queue_;
    std::mutex queueMutex_;
    std::condition_variable queueCv_;
    std::thread workerThread_;
    std::atomic<bool> queueWorkerRunning_{false};

    // API upload worker members
    std::thread apiUploadThread_;
    std::mutex apiUploadMutex_;
    std::condition_variable apiUploadCv_;
    std::atomic<bool> apiUploadWorkerRunning_{false};
    std::atomic<bool> shouldUploadOfflineData_{false};
    
    // MQTT connection state - shared across components
    static std::atomic<bool> mqttConnected_;


    std::string fullBacklogTable_;
    std::string latestValuesTable_;
    std::map<int, long> datapointToOrgIdMap_;

    // API configuration
    std::string apiUrl_;
    std::string apiUser_;
    std::string apiPass_;
    nlohmann::json apiMetadata_;

};

#endif // SQLITE_QUEUE_SERVICE_H
