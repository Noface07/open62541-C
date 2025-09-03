#include "MQTThandler.h"
#include "Logger.h" // For logging
#include <iostream> // For std::cerr
#include "SqliteQueueService.h"

namespace as = boost::asio;
namespace am = async_mqtt;

MQTTHandler::MQTTHandler(boost::asio::io_context &ioc)
    : m_ioc(ioc),
      m_client(std::make_unique<client_t>(ioc.get_executor())),
      m_running(true),
      m_connected(false),
      m_reconnect_timer(ioc),
      sqliteService_(nullptr) {
    log("Starting MQTT thread...", LogLevel::DEBUG);
    m_mqtt_thread = std::thread([this]() {
        log("MQTT thread started", LogLevel::DEBUG);
        while(m_running) {
            try {
                // Prevent io_context from stopping when there's no work
                as::executor_work_guard<as::io_context::executor_type> work_guard(m_ioc.get_executor());
                log("MQTT thread calling io_context.run()", LogLevel::DEBUG);
                m_ioc.run();
                log("MQTT thread io_context.run() returned", LogLevel::DEBUG);
            } catch(const std::exception &e) {
                log("MQTT thread error: " + std::string(e.what()), LogLevel::ERRORS);
                // In case run() exits, reset it so the loop can continue.
                if (m_ioc.stopped()) m_ioc.restart();
            }
        }
        log("MQTT thread exiting", LogLevel::DEBUG);
    });
    log("MQTT handler constructor completed", LogLevel::DEBUG);
}

MQTTHandler::~MQTTHandler() {
    m_running = false;
    disconnect();
    m_reconnect_timer.cancel(); // Stop the timer
    m_ioc.stop(); // Allow the thread to exit
    if(m_mqtt_thread.joinable()) {
        m_mqtt_thread.join();
    }
}

bool MQTTHandler::connect(const std::string &broker, const std::string &port,
                          const std::string &username, const std::string &password) {
    // Store connection details for auto-reconnect
    std::lock_guard<std::mutex> lock(m_mutex);
    m_broker = broker;
    m_port = port;
    m_username = username;
    m_password = password;

    // Post the initial connection attempt to the Asio thread
    as::post(m_ioc, [this]() {
        try_reconnect();
    });
    return true;
}

void MQTTHandler::try_reconnect() {
    // This function runs on the io_context thread, so no data races on client.
    // Use the atomic flag to ensure we don't start a new connection while one is in progress.
    if (isConnected() || m_is_connecting.exchange(true)) {
        return;
    }

    log("Attempting to connect to MQTT broker...", LogLevel::INFO);

    as::co_spawn(m_ioc, [this]() -> as::awaitable<void> {
        try {
            // Re-create the client for a clean state on each connection attempt
            m_client = std::make_unique<client_t>(m_ioc.get_executor());

            co_await m_client->async_underlying_handshake(m_broker, m_port, as::use_awaitable);

            auto connack_opt = co_await m_client->async_start(
                am::v5::connect_packet{true, 0x1234, "", std::nullopt, m_username, m_password},
                as::use_awaitable
            );

            if (connack_opt) {
                log("MQTT session started successfully.", LogLevel::INFO);
                m_is_connecting = false;
                notifyConnected();
                start_receive(); // Start the loop to listen for messages
            } else {
                log("Failed to connect to MQTT broker. Retrying...", LogLevel::ERRORS);
                m_is_connecting = false;
                notifyDisconnected(); // This will schedule the next retry
            }
        } catch (const std::exception& e) {
            log("MQTT connection error: " + std::string(e.what()), LogLevel::ERRORS);
            m_is_connecting = false;
            notifyDisconnected(); // This will schedule the next retry
        }
        co_return;
    }, as::detached);
}

void MQTTHandler::start_receive() {
    as::co_spawn(m_ioc, [this]() -> as::awaitable<void> {
        while (isConnected()) {
            try {
                auto pv_opt = co_await m_client->async_recv(as::use_awaitable);
                if (!pv_opt) {
                    log("Connection closed by broker.", LogLevel::INFO);
                    notifyDisconnected();
                    break; // Exit receive loop
                }

                pv_opt->visit(am::overload {
                    [&](client_t::publish_packet &p) {
                        std::lock_guard<std::mutex> lock(m_mutex);
                        if (m_message_callback) {
                            m_message_callback(p.topic(), std::string(p.payload()));
                        }
                    },
                    [](auto &) {} // Ignore other packet types
                });

            } catch(const std::exception &e) {
                log("MQTT receive loop error: " + std::string(e.what()), LogLevel::ERRORS);
                notifyDisconnected();
                break; // Exit receive loop
            }
        }
        co_return;
    }, as::detached);
}

void MQTTHandler::disconnect() {
    as::post(m_ioc, [this]() {
        m_reconnect_timer.cancel(); // Stop trying to reconnect

        // Lock once to safely manage state and client operations
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_connected) {
            m_client->async_disconnect();
            m_connected = false;
            if (m_disconnect_callback) {
                // Always post callbacks to avoid deadlocks or re-entrancy issues
                as::post(m_ioc, m_disconnect_callback);
            }
            log("Manually disconnected from MQTT broker.", LogLevel::INFO);
        }
    });
}

bool MQTTHandler::publish(const std::string &topic, const std::string &payload) {
    log("MQTTHandler::publish called for topic: " + topic + " with payload size: " + std::to_string(payload.size()), LogLevel::INFO);
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_connected) {
            log("Cannot publish: Not connected to MQTT broker.", LogLevel::ERRORS);
            return false;
        }
        log("MQTT connection confirmed - proceeding with publish", LogLevel::INFO);
    }

    as::post(m_ioc, [this, topic, payload]() {
        as::co_spawn(m_ioc, [this, topic, payload]() -> as::awaitable<void> {
            async_mqtt::packet_id_type packet_id{};
            try {
                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    if (!m_connected) co_return;

                    auto opt_packet_id = m_client->acquire_unique_packet_id();
                    if (!opt_packet_id) {
                        log("All packet_ids in use. Queueing message.", LogLevel::ERRORS);
                        if (m_failed_message_callback) {
                            PendingMessage pm(topic, payload);
                            as::post(m_ioc, [cb=m_failed_message_callback, pm]() {
                                cb({pm});
                            });
                        }
                        co_return;
                    }

                    packet_id = *opt_packet_id;
                    m_pending_messages.emplace(packet_id, PendingMessage(topic, payload));
                }

                co_await m_client->async_publish(packet_id, topic, payload,
                                                 am::qos::at_least_once,
                                                 as::use_awaitable);

                {
                    std::lock_guard<std::mutex> lock(m_mutex);
                    m_pending_messages.erase(packet_id);
                }

                log("Message delivered (PUBACK)", LogLevel::DEBUG);
            } catch (const std::exception& e) {
                log("MQTT publish error: " + std::string(e.what()), LogLevel::ERRORS);
                notifyDisconnected();
            }
            co_return;
        }, as::detached);
    });

    return true;
}


bool MQTTHandler::subscribe(const std::string &topic) {
    if (!isConnected()) {
        log("Cannot subscribe: Not connected to MQTT broker.", LogLevel::INFO);
        return false;
    }
    
    as::post(m_ioc, [this, topic]() {
        as::co_spawn(m_ioc, [this, topic]() -> as::awaitable<void> {
            try {
                std::vector<am::topic_subopts> sub_entry = {{topic, am::qos::at_most_once}};
                co_await m_client->async_subscribe(am::v5::subscribe_packet{
                    *m_client->acquire_unique_packet_id(),
                    am::force_move(sub_entry)
                }, as::use_awaitable);
                log("Subscribed to topic: " + topic, LogLevel::INFO);
            } catch(const std::exception &e) {
                log("MQTT subscribe error: " + std::string(e.what()), LogLevel::ERRORS);
            }
            co_return;
        }, as::detached);
    });
    return true;
}

bool MQTTHandler::isConnected() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_connected;
}

void MQTTHandler::setCallback(std::function<void(const std::string &, const std::string &)> callback) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_message_callback = std::move(callback);
}

void MQTTHandler::setOnConnectCallback(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_connect_callback = std::move(cb);
}

void MQTTHandler::setOnDisconnectCallback(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_disconnect_callback = std::move(cb);
}

void MQTTHandler::setOnFailedMessageCallback(std::function<void(const std::vector<PendingMessage>&)> cb) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_failed_message_callback = std::move(cb);
}

void MQTTHandler::setSqliteService(SqliteQueueService* service) {
    sqliteService_ = service;
}

    void MQTTHandler::notifyConnected() {
        std::function<void()> callback_to_fire;
        std::function<void(const std::vector<PendingMessage>&)> failed_callback;
        std::vector<PendingMessage> pending_to_save;

        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_connected) {
                m_connected = true;
                m_reconnect_timer.cancel();

                for (const auto& pair : m_pending_messages) {
                    pending_to_save.push_back(pair.second);
                }
                clearPendingMessages();

                callback_to_fire = m_connect_callback;
                failed_callback = m_failed_message_callback;
            }
        }

        // Update shared MQTT connection state and trigger API upload
        if (sqliteService_) {
            SqliteQueueService::SetMqttConnected(true);
            sqliteService_->TriggerApiUpload();
            log("MQTT reconnected! Notified SqliteQueueService to start API uploads.", LogLevel::INFO);
        }

        if (callback_to_fire) as::post(m_ioc, callback_to_fire);
        if (failed_callback && !pending_to_save.empty()) {
            log("Saving " + std::to_string(pending_to_save.size()) +
                " pending messages to database on reconnection", LogLevel::INFO);
            as::post(m_ioc, [failed_callback, pending_to_save]() {
                failed_callback(pending_to_save);
            });
        }
    }

    void MQTTHandler::notifyDisconnected() {
        std::function<void()> disconnect_cb;
        std::function<void(const std::vector<PendingMessage>&)> failed_cb;
        std::vector<PendingMessage> failed_messages;

        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_connected) {
                m_connected = false;

                for (auto& [_, msg] : m_pending_messages) {
                    failed_messages.push_back(msg);
                }
                m_pending_messages.clear();

                disconnect_cb = m_disconnect_callback;
                failed_cb = m_failed_message_callback;
            }
        }
        
        // Update shared MQTT connection state
        if (sqliteService_) {
            SqliteQueueService::SetMqttConnected(false);
            log("MQTT disconnected! Notified SqliteQueueService to stop API uploads.", LogLevel::INFO);
        }

        if (disconnect_cb) as::post(m_ioc, disconnect_cb);
        if (failed_cb && !failed_messages.empty()) {
            as::post(m_ioc, [failed_cb, failed_messages]() {
                failed_cb(failed_messages);
            });
        }

        m_reconnect_timer.expires_after(m_reconnect_interval);
        m_reconnect_timer.async_wait([this](auto ec) {
            if (!ec) try_reconnect();
        });
    }

void MQTTHandler::clearPendingMessages() {
    // PRECONDITION: m_mutex must already be locked by caller
    // This method is private and should only be called from thread-safe contexts
    m_pending_messages.clear();
    log("Cleared all pending messages", LogLevel::DEBUG);
}

void MQTTHandler::processPendingMessages() {
    // PRECONDITION: m_mutex must already be locked by caller
    // This method is private and should only be called from thread-safe contexts
    auto now = std::chrono::steady_clock::now();
    auto it = m_pending_messages.begin();
    
    while (it != m_pending_messages.end()) {
        if (now - it->second.timestamp > m_message_timeout) {
            log("Message timed out: " + it->second.topic, LogLevel::DEBUG);
            it = m_pending_messages.erase(it);
        } else {
            ++it;
        }
    }
}
