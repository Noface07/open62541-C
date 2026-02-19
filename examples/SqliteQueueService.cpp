#include "SqliteQueueService.h"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "Logger.h"
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

// Local Base64 encoder for HTTP Basic auth
static std::string
Base64Encode(const std::string &input) {
    static const char table[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(((input.size() + 2) / 3) * 4);
    unsigned int val = 0;
    int valb = -6;
    for(unsigned char c : input) {
        val = (val << 8) + c;
        valb += 8;
        while(valb >= 0) {
            out.push_back(table[(val >> valb) & 0x3F]);
            valb -= 6;
        }
    }
    if(valb > -6)
        out.push_back(table[((val << 8) >> (valb + 8)) & 0x3F]);
    while(out.size() % 4)
        out.push_back('=');
    return out;
}

// Define static member
std::atomic<bool> SqliteQueueService::mqttConnected_{false};

// --- Constructor & Destructor ---

SqliteQueueService::SqliteQueueService(const std::string &dbPath,
                                       const OfflineQueueOptions &options)
    : dbPath_(dbPath), options_(options), fullBacklogTable_("OfflineQueue"),
      latestValuesTable_("LatestValues"), db_(nullptr) {
    InitializeDatabase();
}

SqliteQueueService::~SqliteQueueService() { DisposeDB(); }

void
SqliteQueueService::DisposeDB() {
    StopQueueWorker();
    StopApiUploadTimer();
    if(db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}

// --- Configuration ---

void
SqliteQueueService::SetConfigurations(const std::map<int, long> &map) {
    datapointToOrgIdMap_ = map;
}

void
SqliteQueueService::SetApiUrl(const std::string &url) {
    apiUrl_ = url;
}

void
SqliteQueueService::SetApiAuth(const std::string &bearerToken) {
    apiBearerToken_ = bearerToken;
}

void
SqliteQueueService::SetApiMetadata(const nlohmann::json &metadata) {
    apiMetadata_ = metadata;
}

// MQTT connection state management
void
SqliteQueueService::SetMqttConnected(bool connected) {
    bool wasConnected = mqttConnected_.load();
    mqttConnected_.store(connected);
    std::cout << "[SQLite] MQTT connection state changed: "
              << (connected ? "CONNECTED" : "DISCONNECTED") << std::endl;

    // If we just connected and weren't connected before, wake up the API upload worker
    if(connected && !wasConnected) {
        std::cout << "[SQLite] MQTT reconnected - notifying API upload worker"
                  << std::endl;
    }
}

bool
SqliteQueueService::IsMqttConnected() {
    return mqttConnected_.load();
}

void
SqliteQueueService::TriggerApiUpload() {
    std::cout << "[SQLite] TriggerApiUpload called - checking worker status..."
              << std::endl;

    // Check if the API upload worker is running
    if(!apiUploadWorkerRunning_.load()) {
        std::cout << "[SQLite] API upload worker is not running - starting it now!"
                  << std::endl;
        StartApiUploadTimer();
    } else {
        std::cout << "[SQLite] API upload worker is already running - notifying it"
                  << std::endl;
        apiUploadCv_.notify_one();
    }
}

// --- Database Initialization ---

void
SqliteQueueService::InitializeDatabase() {
    // WAL mode is good for concurrency
    if(sqlite3_open_v2(dbPath_.c_str(), &db_,
                       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                       nullptr) != SQLITE_OK) {
        throw std::runtime_error("Failed to open SQLite DB: " +
                                 std::string(sqlite3_errmsg(db_)));
    }
    sqlite3_exec(db_, "PRAGMA journal_mode=WAL;", nullptr, nullptr, nullptr);
    sqlite3_exec(db_, "PRAGMA busy_timeout = 5000;", nullptr, nullptr, nullptr);

    const char *createOffline = "CREATE TABLE IF NOT EXISTS OfflineQueue ("
                                "Id INTEGER PRIMARY KEY AUTOINCREMENT,"
                                "Topic TEXT NOT NULL,"
                                "Payload TEXT NOT NULL,"
                                "CreatedAt TEXT NOT NULL);";

    const char *createLatest = "CREATE TABLE IF NOT EXISTS LatestValues ("
                               "Topic TEXT PRIMARY KEY,"
                               "Payload TEXT NOT NULL,"
                               "CreatedAt TEXT NOT NULL);";

    const char *createConfigCache = "CREATE TABLE IF NOT EXISTS ApiConfigCache ("
                                    "Key TEXT PRIMARY KEY,"
                                    "Value TEXT NOT NULL,"
                                    "UpdatedAt TEXT NOT NULL);";

    char *errMsg = nullptr;
    if(sqlite3_exec(db_, createOffline, nullptr, nullptr, &errMsg) != SQLITE_OK) {
        std::string err = errMsg;
        sqlite3_free(errMsg);
        throw std::runtime_error("Failed to create OfflineQueue table: " + err);
    }
    if(sqlite3_exec(db_, createLatest, nullptr, nullptr, &errMsg) != SQLITE_OK) {
        std::string err = errMsg;
        sqlite3_free(errMsg);
        throw std::runtime_error("Failed to create LatestValues table: " + err);
    }
    if(sqlite3_exec(db_, createConfigCache, nullptr, nullptr, &errMsg) != SQLITE_OK) {
        std::string err = errMsg;
        sqlite3_free(errMsg);
        throw std::runtime_error("Failed to create ApiConfigCache table: " + err);
    }
}

// --- Queue Worker ---

void
SqliteQueueService::EnqueueMessage(const std::string &topic, const MqttPayload &payload,
                                   long orgId) {
    {
        std::lock_guard<std::mutex> lock(queueMutex_);
        queue_.push({topic, payload, orgId});
    }
    queueCv_.notify_one();
}

void
SqliteQueueService::StartQueueWorker() {
    if(queueWorkerRunning_)
        return;
    queueWorkerRunning_ = true;
    workerThread_ = std::thread(&SqliteQueueService::QueueWorkerLoop, this);
}

void
SqliteQueueService::StopQueueWorker() {
    if(!queueWorkerRunning_)
        return;
    queueWorkerRunning_ = false;
    queueCv_.notify_all();
    if(workerThread_.joinable()) {
        workerThread_.join();
    }
}

void
SqliteQueueService::QueueWorkerLoop() {
    while(queueWorkerRunning_) {
        std::unique_lock<std::mutex> lock(queueMutex_);
        queueCv_.wait(lock, [&] { return !queue_.empty() || !queueWorkerRunning_; });

        if(!queueWorkerRunning_ && queue_.empty())
            break;

        QueueItem item = queue_.front();
        queue_.pop();
        lock.unlock();

        try {
            InsertMessageToDatabase(item);
        } catch(const std::exception &ex) {
            std::cerr << "SQLite insert failed for a message: " << ex.what() << std::endl;
        }
    }
}

// --- API Upload Worker ---

void
SqliteQueueService::StartApiUploadTimer() {
    std::cout << "[SQLite] StartApiUploadTimer called - current worker status: "
              << (apiUploadWorkerRunning_ ? "RUNNING" : "STOPPED") << std::endl;

    if(apiUploadWorkerRunning_) {
        std::cout
            << "[SQLite] API upload worker already running - triggering notification"
            << std::endl;
        apiUploadCv_.notify_one();
        return;
    }

    apiUploadWorkerRunning_ = true;
    apiUploadThread_ = std::thread(&SqliteQueueService::ApiUploadWorkerLoop, this);
    std::cout << "[SQLite] API uploader started" << std::endl;
}

void
SqliteQueueService::StopApiUploadTimer() {
    std::cout << "[SQLite] StopApiUploadTimer called - current worker status: "
              << (apiUploadWorkerRunning_ ? "RUNNING" : "STOPPED") << std::endl;

    {
        std::lock_guard<std::mutex> lock(apiUploadMutex_);
        if(!apiUploadWorkerRunning_) {
            std::cout << "[SQLite] API upload worker already stopped" << std::endl;
            return;  // already stopped
        }
        apiUploadWorkerRunning_ = false;
    }

    std::cout << "[SQLite] Stopping API upload worker..." << std::endl;
    apiUploadCv_.notify_all();

    if(apiUploadThread_.joinable()) {
        apiUploadThread_.join();
        std::cout << "[SQLite] API upload worker stopped successfully" << std::endl;
    }
}

void
SqliteQueueService::ApiUploadWorkerLoop() {
    while(apiUploadWorkerRunning_) {
        std::unique_lock<std::mutex> lock(apiUploadMutex_);

        // Wait until: MQTT connected && DB has data OR stop requested
        std::cout << "[SQLite] API upload worker waiting for conditions..." << std::endl;
        apiUploadCv_.wait(lock, [this] {
            bool isConnected = SqliteQueueService::mqttConnected_.load();
            long rowCount = GetRowCount();
            bool shouldProceed =
                !apiUploadWorkerRunning_ || (isConnected && rowCount > 0);
            std::cout << "[SQLite] API upload worker check - MQTT: "
                      << (isConnected ? "CONNECTED" : "DISCONNECTED")
                      << ", DB rows: " << rowCount
                      << ", shouldProceed: " << (shouldProceed ? "YES" : "NO")
                      << std::endl;
            return shouldProceed;
        });

        if(!apiUploadWorkerRunning_)
            break;

        if(!SqliteQueueService::mqttConnected_.load() || GetRowCount() == 0) {
            std::cout
                << "[SQLite] Conditions not met for API upload, going back to wait..."
                << std::endl;
            continue;  // go back to waiting
        }

        lock.unlock();  // don’t block notifies during upload

        try {
            auto messages = GetBatchQueuedMessages(options_.batchSize);
            if(messages.empty())
                continue;

            std::vector<ApiTagData> apiTagDataList;
            for(const auto &msg : messages) {
                MqttPayload payload = nlohmann::json::parse(msg.payload);

                long orgId = 0;
                auto it = datapointToOrgIdMap_.find(payload.datapointId);
                if(it != datapointToOrgIdMap_.end()) {
                    orgId = it->second;
                }

                int quality = 1;
                try {
                    quality = (payload.quality);
                } catch(...) {
                }

                apiTagDataList.push_back({orgId, payload.tagId, payload.value,
                                          payload.tagType, payload.timeStamp,
                                          payload.source, payload.datapointId,
                                          payload.infoId, quality, 2});
            }

            nlohmann::json finalPayload = apiMetadata_;
            finalPayload["Data"] = apiTagDataList;
            finalPayload["RequestDateTime"] = GetCurrentTimestamp();

            std::cout << "[SQLite] Attempting to send " << apiTagDataList.size()
                      << " records to API..." << std::endl;

            if(SendToApi(finalPayload.dump())) {
                std::vector<int> idsToDelete;
                for(const auto &msg : messages)
                    idsToDelete.push_back(msg.id);
                DeleteMessages(idsToDelete);
                std::cout << "[SQLite] Successfully uploaded " << idsToDelete.size()
                          << " messages to API" << std::endl;
            } else {
                std::cerr << "[SQLite] API upload failed, will retry later" << std::endl;
                std::cout << "[SQLite] Sleeping for 5 seconds before retry..."
                          << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(5));  // backoff
                std::cout << "[SQLite] Woke up from backoff, continuing upload loop..."
                          << std::endl;
            }
        } catch(const std::exception &ex) {
            std::cerr << "[SQLite] ApiUploadWorkerLoop exception: " << ex.what()
                      << std::endl;
        }

        lock.lock();  // re-lock before looping
    }

    std::cout << "[SQLite] API uploader thread stopped." << std::endl;
}

// --- Core Database Methods ---

void
SqliteQueueService::InsertMessageToDatabase(const QueueItem &item) {
    ExecuteDbCommand([&](sqlite3 *db) {
        char *errMsg = nullptr;
        if(sqlite3_exec(db, "BEGIN TRANSACTION;", nullptr, nullptr, &errMsg) !=
           SQLITE_OK) {
            std::string err = errMsg;
            sqlite3_free(errMsg);
            throw std::runtime_error("Begin transaction failed: " + err);
        }

        std::string payloadJson = nlohmann::json(item.payload).dump();
        std::string createdAt = GetCurrentTimestamp();

        std::cout << "[SQLite] InsertMessageToDatabase - Topic: " << item.topic
                  << ", Payload size: " << payloadJson.size() << std::endl;

        // 1. Insert or Replace into LatestValues
        sqlite3_stmt *stmt1;
        const char *sql1 = "INSERT OR REPLACE INTO LatestValues (Topic, Payload, "
                           "CreatedAt) VALUES (?, ?, ?);";
        sqlite3_prepare_v2(db, sql1, -1, &stmt1, nullptr);
        sqlite3_bind_text(stmt1, 1, item.topic.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt1, 2, payloadJson.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt1, 3, createdAt.c_str(), -1, SQLITE_TRANSIENT);
        bool success = sqlite3_step(stmt1) == SQLITE_DONE;
        sqlite3_finalize(stmt1);

        // 2. Insert into OfflineQueue
        if(success) {
            sqlite3_stmt *stmt2;
            const char *sql2 =
                "INSERT INTO OfflineQueue (Topic, Payload, CreatedAt) VALUES (?, ?, ?);";
            sqlite3_prepare_v2(db, sql2, -1, &stmt2, nullptr);
            sqlite3_bind_text(stmt2, 1, item.topic.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt2, 2, payloadJson.c_str(), -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt2, 3, createdAt.c_str(), -1, SQLITE_TRANSIENT);
            success = sqlite3_step(stmt2) == SQLITE_DONE;
            sqlite3_finalize(stmt2);
        }

        if(success) {
            sqlite3_exec(db, "COMMIT;", nullptr, nullptr, nullptr);
            std::cout << "[SQLite] Successfully inserted message to both LatestValues "
                         "and OfflineQueue tables"
                      << std::endl;
        } else {
            sqlite3_exec(db, "ROLLBACK;", nullptr, nullptr, nullptr);
            std::cerr << "[SQLite] Failed to execute insert statements: "
                      << std::string(sqlite3_errmsg(db)) << std::endl;
            throw std::runtime_error("Failed to execute insert statements: " +
                                     std::string(sqlite3_errmsg(db)));
        }
        return SQLITE_OK;
    });
}

std::vector<QueueModel>
SqliteQueueService::GetBatchQueuedMessages(int batchSize) {
    std::vector<QueueModel> messages;
    ExecuteDbCommand([&](sqlite3 *db) {
        sqlite3_stmt *stmt;
        const char *sql = "SELECT Id, Topic, Payload, CreatedAt FROM OfflineQueue ORDER "
                          "BY Id ASC LIMIT ?;";
        sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
        sqlite3_bind_int(stmt, 1, batchSize);

        while(sqlite3_step(stmt) == SQLITE_ROW) {
            messages.push_back({sqlite3_column_int(stmt, 0),
                                (const char *)sqlite3_column_text(stmt, 1),
                                (const char *)sqlite3_column_text(stmt, 2),
                                (const char *)sqlite3_column_text(stmt, 3)});
        }
        sqlite3_finalize(stmt);
        return SQLITE_OK;
    });
    return messages;
}

std::vector<QueueModel>
SqliteQueueService::GetLatestValues() {
    std::vector<QueueModel> messages;
    ExecuteDbCommand([&](sqlite3 *db) {
        sqlite3_stmt *stmt;
        const char *sql = "SELECT Topic, Payload, CreatedAt FROM LatestValues;";
        sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);

        while(sqlite3_step(stmt) == SQLITE_ROW) {
            messages.push_back({0,  // ID is not relevant here
                                (const char *)sqlite3_column_text(stmt, 0),
                                (const char *)sqlite3_column_text(stmt, 1),
                                (const char *)sqlite3_column_text(stmt, 2)});
        }
        sqlite3_finalize(stmt);
        return SQLITE_OK;
    });
    return messages;
}

void
SqliteQueueService::DeleteMessages(const std::vector<int> &ids) {
    if(ids.empty())
        return;
    ExecuteDbCommand([&](sqlite3 *db) {
        std::string query = "DELETE FROM OfflineQueue WHERE Id IN (";
        for(size_t i = 0; i < ids.size(); ++i) {
            query += (i == 0 ? "?" : ",?");
        }
        query += ");";

        sqlite3_stmt *stmt;
        sqlite3_prepare_v2(db, query.c_str(), -1, &stmt, nullptr);
        for(size_t i = 0; i < ids.size(); ++i) {
            sqlite3_bind_int(stmt, static_cast<int>(i + 1), ids[i]);
        }

        if(sqlite3_step(stmt) != SQLITE_DONE) {
            std::cerr << "Failed to delete messages: " << sqlite3_errmsg(db) << std::endl;
        }
        sqlite3_finalize(stmt);
        return SQLITE_OK;
    });
}

void
SqliteQueueService::ClearLatestValues() {
    ExecuteDbCommand([&](sqlite3 *db) {
        char *errMsg = nullptr;
        if(sqlite3_exec(db, "DELETE FROM LatestValues;", nullptr, nullptr, &errMsg) !=
           SQLITE_OK) {
            std::string err = errMsg;
            sqlite3_free(errMsg);
            std::cerr << "Failed to clear latest values: " << err << std::endl;
        }
        return SQLITE_OK;
    });
}

long
SqliteQueueService::GetRowCount() {
    long count = 0;
    ExecuteDbCommand([&](sqlite3 *db) {
        sqlite3_stmt *stmt;
        const char *sql = "SELECT COUNT(*) FROM OfflineQueue";
        sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
        if(sqlite3_step(stmt) == SQLITE_ROW) {
            count = static_cast<long>(sqlite3_column_int64(stmt, 0));
        }
        sqlite3_finalize(stmt);
        return SQLITE_OK;
    });
    return count;
}

// --- MQTT & API Communication ---

void
SqliteQueueService::PublishLatestValuesToMqtt(
    const std::function<void(const std::string &, const std::string &)> &publishFunc) {
    std::cout << "[SQLite] PublishLatestValuesToMqtt called!" << std::endl;
    try {
        auto latestValues = GetLatestValues();
        std::cout << "[SQLite] Retrieved " << latestValues.size()
                  << " latest values from database" << std::endl;

        if(latestValues.empty()) {
            std::cout << "[SQLite] No latest values to publish to MQTT" << std::endl;
            return;
        }

        std::cout << "[SQLite] Publishing " << latestValues.size()
                  << " latest values to MQTT..." << std::endl;
        int errorCount = 0;
        for(const auto &value : latestValues) {
            try {
                std::cout << "[SQLite] Publishing latest value for topic: " << value.topic
                          << std::endl;
                // The C# version wraps the payload in a {"Data": [...]} structure.
                // Replicating that.
                nlohmann::json wrapper;
                wrapper["Data"] =
                    nlohmann::json::array({nlohmann::json::parse(value.payload)});
                std::string payloadToPublish = wrapper.dump();
                std::cout << "[SQLite] Calling publishFunc for topic: " << value.topic
                          << " with payload size: " << payloadToPublish.size()
                          << std::endl;
                publishFunc(value.topic, payloadToPublish);
                std::cout << "[SQLite] Successfully published latest value for topic: "
                          << value.topic << std::endl;
            } catch(const std::exception &ex) {
                errorCount++;
                std::cerr << "[SQLite] Error publishing latest value for topic "
                          << value.topic << ": " << ex.what() << std::endl;
            }
        }

        if(errorCount == 0) {
            ClearLatestValues();
            std::cout
                << "[SQLite] Cleared latest values table after successful MQTT publishing"
                << std::endl;
        } else {
            std::cerr << "[SQLITE] Keeping latest values table due to " << errorCount
                      << " publishing errors" << std::endl;
        }

    } catch(const std::exception &ex) {
        std::cerr << "[SQLite] Unhandled exception in PublishLatestValuesToMqtt: "
                  << ex.what() << std::endl;
    }
}

bool
SqliteQueueService::SendToApi(const std::string &jsonPayload, bool isRetry) {
    try {
        std::cout << "[SQLite] SendToApi called with payload size: " << jsonPayload.size()
                  << " bytes" << std::endl;
        log("[SQLite] SendToApi called with payload size: " +
                std::to_string(jsonPayload.size()) + " bytes",
            LogLevel::INFO);

        if(apiUrl_.empty()) {
            std::cerr << "[SQLite] API URL not set" << std::endl;
            log("[SQLite] API URL not set", LogLevel::ERRORS);
            return false;
        }

        std::cout << "[SQLite] Sending API request to: " << apiUrl_ << std::endl;
        log("[SQLite] Sending API request to: " + apiUrl_, LogLevel::INFO);
        // parse apiUrl_ as http://host:port/path
        std::string scheme, host, port, target;
        if(apiUrl_.rfind("http://", 0) == 0) {
            scheme = "http";
            std::string rest = apiUrl_.substr(7);
            size_t slash = rest.find('/');
            std::string hostPort =
                (slash == std::string::npos) ? rest : rest.substr(0, slash);
            target = (slash == std::string::npos) ? "/" : rest.substr(slash);
            size_t colon = hostPort.find(':');
            if(colon != std::string::npos) {
                host = hostPort.substr(0, colon);
                port = hostPort.substr(colon + 1);
            } else {
                host = hostPort;
                port = "5128";
            }
        } else if(apiUrl_.rfind("https://", 0) == 0) {
            std::cerr << "[SQLite] HTTPS not supported in this implementation"
                      << std::endl;
            log("[SQLite] HTTPS not supported in this implementation", LogLevel::ERRORS);
            return false;
        } else {
            std::cerr << "[SQLite] Only http scheme supported in this example"
                      << std::endl;
            log("[SQLite] Only http scheme supported in this example", LogLevel::ERRORS);
            return false;
        }

        std::cout << "[SQLite] Parsed URL - Host: " << host << ", Port: " << port
                  << ", Target: " << target << std::endl;
        log("[SQLite] Parsed URL - Host: " + host + ", Port: " + port +
                ", Target: " + target,
            LogLevel::INFO);
        namespace beast = boost::beast;
        namespace http = beast::http;
        namespace net = boost::asio;
        net::io_context ioc;
        net::ip::tcp::resolver resolver(ioc);
        beast::tcp_stream stream(ioc);
        auto const results = resolver.resolve(host, port);
        stream.connect(results);
        http::request<http::string_body> req{http::verb::post, target, 11};
        req.set(http::field::host, host);
        req.set(http::field::content_type, "application/json");
        req.set(beast::http::field::authorization, "Bearer " + apiBearerToken_);
        req.body() = jsonPayload;
        req.prepare_payload();

        std::cout << "[SQLite] Request prepared with " << jsonPayload.size()
                  << " bytes payload" << std::endl;
        log("[SQLite] Request prepared with " + std::to_string(jsonPayload.size()) +
                " bytes payload",
            LogLevel::INFO);
        std::cout << "[SQLite] Sending HTTP request..." << std::endl;
        log("[SQLite] Sending HTTP request...", LogLevel::INFO);
        http::write(stream, req);
        beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream, buffer, res);
        stream.socket().shutdown(net::ip::tcp::socket::shutdown_both);

        int statusCode = res.result_int();
        std::cout << "[SQLite] API response status: " << statusCode << std::endl;
        log("[SQLite] API response status: " + std::to_string(statusCode),
            LogLevel::INFO);
        if(statusCode >= 200 && statusCode < 300) {
            std::cout << "[SQLite] API call successful" << std::endl;
            log("[SQLite] API call successful", LogLevel::INFO);
            return true;
        } else if(statusCode == 401 && !isRetry) {
            std::cout << "[SQLite] 401 Unauthorized. Attempting to refresh token..."
                      << std::endl;
            log("[SQLite] 401 Unauthorized. Attempting to refresh token...",
                LogLevel::WARNING);

            if(tokenRefreshCallback_) {
                std::string newToken = tokenRefreshCallback_();
                if(!newToken.empty()) {
                    SetApiAuth(newToken);
                    std::cout << "[SQLite] Token refreshed. Retrying request..."
                              << std::endl;
                    log("[SQLite] Token refreshed. Retrying request...", LogLevel::INFO);
                    return SendToApi(jsonPayload, true);
                } else {
                    std::cerr << "[SQLite] Failed to refresh token." << std::endl;
                    log("[SQLite] Failed to refresh token.", LogLevel::ERRORS);
                }
            } else {
                std::cerr << "[SQLite] No token refresh callback configured."
                          << std::endl;
                log("[SQLite] No token refresh callback configured.", LogLevel::ERRORS);
            }
            return false;
        } else {
            std::cerr << "[SQLite] API call failed with status: " << statusCode
                      << std::endl;
            log("[SQLite] API call failed with status: " + std::to_string(statusCode),
                LogLevel::ERRORS);
            std::cerr << "[SQLite] Response body: " << res.body() << std::endl;
            log("[SQLite] Response body: " + res.body(), LogLevel::ERRORS);
            return false;
        }
    } catch(std::exception const &e) {
        std::cerr << "[SQLite] SendToApi exception: " << e.what() << std::endl;
        log("[SQLite] SendToApi exception: " + std::string(e.what()), LogLevel::ERRORS);
        return false;
    }
}

// --- Helper Functions ---

void
SqliteQueueService::ExecuteDbCommand(const std::function<int(sqlite3 *)> &command) {
    int maxRetries = 3;
    for(int i = 0; i < maxRetries; ++i) {
        int rc = command(db_);
        if(rc == SQLITE_OK) {
            return;
        }
        if(rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            std::cerr << "Database is locked. Retrying attempt " << (i + 1) << "/"
                      << maxRetries << "..." << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } else {
            throw std::runtime_error("SQLite operation failed: " +
                                     std::string(sqlite3_errmsg(db_)));
        }
    }
    throw std::runtime_error(
        "SQLite operation failed after multiple retries: database remained locked.");
}

std::string
SqliteQueueService::GetCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto timeT = std::chrono::system_clock::to_time_t(now);
    std::tm tm;
#ifdef _WIN32
    gmtime_s(&tm, &timeT);
#else
    gmtime_r(&timeT, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

void
SqliteQueueService::SetTokenRefreshCallback(TokenRefreshCallback callback) {
    tokenRefreshCallback_ = callback;
}

// --- Config Cache Methods ---

void
SqliteQueueService::SetConfig(const std::string &key, const std::string &value) {
    ExecuteDbCommand([&](sqlite3 *db) {
        sqlite3_stmt *stmt;
        const char *sql = "INSERT OR REPLACE INTO ApiConfigCache (Key, Value, UpdatedAt) "
                          "VALUES (?, ?, ?);";
        sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, value.c_str(), -1, SQLITE_TRANSIENT);
        std::string ts = GetCurrentTimestamp();
        sqlite3_bind_text(stmt, 3, ts.c_str(), -1, SQLITE_TRANSIENT);

        if(sqlite3_step(stmt) != SQLITE_DONE) {
            std::string err = sqlite3_errmsg(db);
            sqlite3_finalize(stmt);
            throw std::runtime_error("SetConfig failed: " + err);
        }
        sqlite3_finalize(stmt);
        std::cout << "[SQLite] Config cached: " << key << " (" << value.size()
                  << " bytes)" << std::endl;
        return SQLITE_OK;
    });
}

std::string
SqliteQueueService::GetConfig(const std::string &key) {
    std::string result;
    ExecuteDbCommand([&](sqlite3 *db) {
        sqlite3_stmt *stmt;
        const char *sql = "SELECT Value FROM ApiConfigCache WHERE Key = ?;";
        sqlite3_prepare_v2(db, sql, -1, &stmt, nullptr);
        sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);

        if(sqlite3_step(stmt) == SQLITE_ROW) {
            const char *text = (const char *)sqlite3_column_text(stmt, 0);
            if(text) {
                result = text;
            }
        }
        sqlite3_finalize(stmt);
        return SQLITE_OK;
    });
    return result;
}
