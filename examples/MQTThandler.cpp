#include "MQTThandler.h"

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

#include <async_mqtt/all.hpp>
#include <async_mqtt/asio_bind/predefined_layer/mqtts.hpp>
#include <async_mqtt/asio_bind/predefined_layer/ws.hpp>
#include <async_mqtt/asio_bind/predefined_layer/wss.hpp>
#include <boost/asio.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <nlohmann/json.hpp>

#include "Logger.h"

using json = nlohmann::ordered_json;

using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;


MQTTHandler::MQTTHandler(boost::asio::io_context &ioc)
    : m_ioc(ioc), m_client(std::make_unique<client_t>(ioc.get_executor())),
      m_running(true), m_connected(false) {
    m_mqtt_thread = std::thread([this]() {
        while(m_running) {
            try {
                m_ioc.run();
            } catch(const std::exception &e) {
                std::cerr << "MQTT thread error: " << e.what() << std::endl;
            }
        }
    });
}


MQTTHandler::~MQTTHandler() {
    disconnect();
    m_running = false;
    if(m_mqtt_thread.joinable()) {
        m_mqtt_thread.join();
    }
}

bool
MQTTHandler::connect(const std::string &broker, const std::string &port,
                     const std::string &username, const std::string &password) {
    try {
        std::lock_guard<std::mutex> lock(m_mutex);
        as::co_spawn(
            m_ioc,
            [this, broker, port, username, password]() -> as::awaitable<void> {
                try {
                    // Connect to broker
                    co_await m_client->async_underlying_handshake(broker, port,
                                                                  as::use_awaitable);

                    // Start MQTT session with username/password
                    auto connack_opt = co_await m_client->async_start(
                        am::v5::connect_packet{true, 0x1234, "", std::nullopt, username,
                                               password},
                        as::use_awaitable);

                    if(!connack_opt) {
                        log("Failed to connect to MQTT broker", LogLevel::ERRORS);
                        m_connected = false;
                        co_return;
                    }

                    m_connected = true;
                    log("MQTT session started successfully");

                    // Start global message receive loop here
                    as::co_spawn(
                        m_ioc,
                        [this]() -> as::awaitable<void> {
                            try {
                                while(m_running) {
                                    auto pv_opt =
                                        co_await m_client->async_recv(as::use_awaitable);
                                    if(!pv_opt)
                                        break;
                                    pv_opt->visit(am::overload{
                                        [&](client_t::publish_packet &p) {
                                            if(m_callback) {
                                                m_callback(p.topic(), p.payload());
                                            }
                                        },
                                        [](auto &) {}});
                                }
                            } catch(const std::exception &e) {
                                log("MQTT receive loop error: " + string(e.what()),
                                    LogLevel::ERRORS);
                            }
                            co_return;
                        },
                        as::detached);

                } catch(const std::exception &e) {
                    log("MQTT connection error: " + string(e.what()));
                    m_connected = false;
                }
                co_return;
            },
            as::detached);
        return true;
    } catch(const std::exception &e) {
        log("Failed to initiate MQTT connection: " + string(e.what()), LogLevel::ERRORS);
        return false;
    }
}

void
MQTTHandler::disconnect() {
    try {
        std::lock_guard<std::mutex> lock(m_mutex);
        if(m_connected) {
            as::post(m_ioc, [this]() { m_client->async_disconnect(); });
            m_connected = false;
            log("Disconnected from MQTT broker");
        }
    } catch(const std::exception &e) {
        log("Error disconnecting from MQTT broker: " + string(e.what()),
            LogLevel::ERRORS);
    }
}

bool
MQTTHandler::publish(const std::string &topic, const std::string &payload) {
    try {
        std::lock_guard<std::mutex> lock(m_mutex);
        if(!m_connected) {
            log("Cannot publish: Not connected to MQTT broker", LogLevel::ERRORS);
            return false;
        }
        as::post(m_ioc, [this, topic, payload]() {
            as::co_spawn(
                m_ioc,
                [this, topic, payload]() -> as::awaitable<void> {
                    try {
                        co_await m_client->async_publish(topic, payload,
                                                         am::qos::at_most_once);
                    } catch(const std::exception &e) {
                        ("MQTT publish error: " + string(e.what()), LogLevel::ERRORS);
                    }
                    co_return;
                },
                as::detached);
        });
        return true;
    } catch(const std::exception &e) {
        log("Failed to publish message: " + string(e.what()), LogLevel::ERRORS);
        return false;
    }
}

bool
MQTTHandler::isConnected() {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_connected;
}

bool
MQTTHandler::subscribe(const std::string &topic) {
    try {
        std::lock_guard<std::mutex> lock(m_mutex);
        if(!m_connected) {
            log("Cannot subscribe: Not connected to MQTT broker", LogLevel::ERRORS);
            return false;
        }
        as::post(m_ioc, [this, topic]() {
            as::co_spawn(
                m_ioc,
                [this, topic]() -> as::awaitable<void> {
                    try {
                        std::vector<am::topic_subopts> sub_entry = {
                            {topic, am::qos::at_most_once}};
                        auto suback_opt = co_await m_client->async_subscribe(
                            am::v5::subscribe_packet{
                                *m_client->acquire_unique_packet_id(),
                                am::force_move(sub_entry)},
                            as::use_awaitable);
                        if(!suback_opt) {
                            log("Failed to subscribe to topic: " + topic, LogLevel::ERRORS);
                            co_return;
                        }
                        log("Subscribed to topic: " + topic);
                    } catch(const std::exception &e) {
                        ("MQTT subscribe error: " + string(e.what()), LogLevel::ERRORS);
                    }
                    co_return;
                },
                as::detached);
        });
        return true;
    } catch(const std::exception &e) {
        log("Failed to subscribe to topic: " + string(e.what()), LogLevel::ERRORS);
        return false;
    }
}

void
MQTTHandler::setCallback(
    std::function<void(const std::string &, const std::string &)> callback) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_callback = std::move(callback);
}
