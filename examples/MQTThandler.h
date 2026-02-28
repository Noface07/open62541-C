#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <unordered_map>

// Required Boost & MQTT headers
#include <async_mqtt/all.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <async_mqtt/asio_bind/predefined_layer/mqtts.hpp>

// Forward declare the client type to keep this header clean
// Changed from mqtt to mqtts for TLS/SSL support
using client_t =
    async_mqtt::client<async_mqtt::protocol_version::v5, async_mqtt::protocol::mqtts>;

using client_wt = async_mqtt::client<
    async_mqtt::protocol_version::v5,
    async_mqtt::protocol::mqtt>;

// Structure to track pending messages
struct PendingMessage {
    std::string topic;
    std::string payload;
    std::chrono::steady_clock::time_point timestamp;

    PendingMessage(const std::string &t, const std::string &p)
        : topic(t), payload(p), timestamp(std::chrono::steady_clock::now()) {}
};

class MQTTHandler {
  public:
    MQTTHandler(boost::asio::io_context &ioc, boost::asio::ssl::context &ssl_ctx);
    ~MQTTHandler();

    // Set reference to SqliteQueueService for notifications
    void
    setSqliteService(class SqliteQueueService *service);

    // --- Public API ---
    bool
    connect(const std::string &broker, const std::string &port,
            const std::string &username, const std::string &password,
            const bool &protocol,
            const std::string &clientId = "");
    void
    disconnect();
    bool
    publish(const std::string &topic, const std::string &payload);
    bool
    subscribe(const std::string &topic);
    bool
    subscribeBatch(const std::vector<std::string> &topics);
    bool
    isConnected() const;

    // --- Callbacks ---
    void
    setCallback(std::function<void(const std::string &, const std::string &)> callback);
    void
    setOnConnectCallback(std::function<void()> cb);
    void
    setOnDisconnectCallback(std::function<void()> cb);
    void
    setOnFailedMessageCallback(
        std::function<void(const std::vector<PendingMessage> &)> cb);

    // --- Token Refresh ---
    // Set a callback that returns a fresh MQTT password.
    // Called automatically before every reconnect attempt so an expired JWT
    // never causes a permanent not_authorized loop.
    void
    setPasswordRefreshCallback(std::function<std::string()> cb);

  private:
    // --- Internal Methods ---
    void
    start_receive();
    void
    try_reconnect();
    void
    notifyConnected();
    void
    notifyDisconnected();
    void
    clearPendingMessages();
    void
    processPendingMessages();

    // --- Member Variables ---
    boost::asio::io_context &m_ioc;
    boost::asio::ssl::context &m_ssl_ctx;
    std::unique_ptr<client_t> m_client;
    std::unique_ptr<client_wt> wm_client;
    std::thread m_mqtt_thread;
    std::atomic<bool> m_running;

    // State Management
    mutable std::mutex m_mutex;  // Protects m_connected, callbacks, and connection params
    std::atomic<bool> m_connected{false};
    std::atomic<bool> m_is_connecting{false};  // Prevents concurrent connect attempts

    using packet_id_t =
        async_mqtt::packet_id_type;  // adjust if the library exposes a typedef
    std::unordered_map<packet_id_t, PendingMessage> m_pending_messages;

    uint16_t m_next_message_id{1};
    std::chrono::seconds m_message_timeout{
        10};  // Messages older than this are considered failed

    // Callbacks
    std::function<void(const std::string &, const std::string &)> m_message_callback;
    std::function<void()> m_connect_callback;
    std::function<void()> m_disconnect_callback;
    std::function<void(const std::vector<PendingMessage> &)> m_failed_message_callback;

    // Connection Parameters (for reconnect)
    std::string m_broker;
    std::string m_port;
    std::string m_username;
    std::string m_password;
    std::string m_clientId;   // MQTT CONNECT client identifier (from EdgeConfig)
    bool m_protocol;

    // Optional callback to refresh the MQTT password before each reconnect.
    // Returns a fresh password string (e.g. new JSON-wrapped JWT token).
    std::function<std::string()> m_passwordRefreshCallback;

    // Asio Timer for Reconnection (replaces the manual thread)
    boost::asio::steady_timer m_reconnect_timer;
    std::chrono::seconds m_reconnect_interval{5};

    // Forward declaration for SqliteQueueService
    class SqliteQueueService *sqliteService_;
};
