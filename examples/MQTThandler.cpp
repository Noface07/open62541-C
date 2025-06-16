#include <async_mqtt/all.hpp>
#include <async_mqtt/asio_bind/predefined_layer/mqtts.hpp>
#include <async_mqtt/asio_bind/predefined_layer/ws.hpp> 
#include <async_mqtt/asio_bind/predefined_layer/wss.hpp>
#include <boost/asio.hpp>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <open62541/client_config_default.h>
#include <open62541/client_highlevel.h>
#include <open62541/client_subscriptions.h>
#include <open62541/plugin/log_stdout.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <functional>
#include <nlohmann/json.hpp>
using json = nlohmann::json;

using namespace std;
namespace as = boost::asio;
namespace am = async_mqtt;       
namespace beast = boost::beast;
using tcp = boost::asio::ip::tcp;  





class MQTTHandler {
private:
    as::io_context& m_ioc;
    using client_t = am::client<am::protocol_version::v5, am::protocol::mqtt>;
    std::unique_ptr<client_t> m_client;
    std::thread m_mqtt_thread;
    std::atomic<bool> m_running{true};
    std::mutex m_mutex;
    std::function<void(const std::string&, const std::string&)> m_callback;
    bool m_connected{false};

public:
    MQTTHandler(as::io_context& ioc) : 
        m_ioc(ioc),
        m_client(std::make_unique<client_t>(ioc.get_executor())) {
        m_mqtt_thread = std::thread([this]() {
            while(m_running) {
                try {
                    m_ioc.run();
                } catch (const std::exception& e) {
                    std::cerr << "MQTT thread error: " << e.what() << std::endl;
                }
            }
        });
    }

    ~MQTTHandler() {
        disconnect();
        m_running = false;
        if(m_mqtt_thread.joinable()) {
            m_mqtt_thread.join();
        }
    }

    bool connect(const std::string& broker, const std::string& port,
                const std::string& username, const std::string& password) {
        try {
            std::lock_guard<std::mutex> lock(m_mutex);
            as::co_spawn(
                m_ioc,
                [this, broker, port, username, password]() -> as::awaitable<void> {
                    try {
                        // Connect to broker
                        co_await m_client->async_underlying_handshake(broker, port, as::use_awaitable);
                        std::cout << "Connected to MQTT broker: " << broker << ":" << port << std::endl;

                        // Start MQTT session with username/password
                        auto connack_opt = co_await m_client->async_start(
                            am::v5::connect_packet{
                                true,   // clean_start
                                0x1234, // keep_alive
                                "",     // Client Identifier
                                std::nullopt, // no will
                                username,   // username
                                password    // password
                            },
                            as::use_awaitable
                        );
                        if (!connack_opt) {
                            std::cerr << "Failed to connect to MQTT broker" << std::endl;
                            m_connected = false;
                            co_return;
                        }
                        m_connected = true;
                        std::cout << "MQTT session started successfully" << std::endl;
                    } catch (const std::exception& e) {
                        std::cerr << "MQTT connection error: " << e.what() << std::endl;
                        m_connected = false;
                    }
                    co_return;
                },
                as::detached
            );
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to initiate MQTT connection: " << e.what() << std::endl;
            return false;
        }
    }

    void disconnect() {
        try {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_connected) {
                as::post(m_ioc, [this]() {
                    m_client->async_disconnect();
                });
                m_connected = false;
                std::cout << "Disconnected from MQTT broker" << std::endl;
            }
        } catch (const std::exception& e) {
            std::cerr << "Error disconnecting from MQTT broker: " << e.what() << std::endl;
        }
    }

    bool publish(const std::string& topic, const std::string& payload) {
        try {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_connected) {
                std::cerr << "Cannot publish: Not connected to MQTT broker" << std::endl;
                return false;
            }
            as::post(m_ioc, [this, topic, payload]() {
                as::co_spawn(
                    m_ioc,
                    [this, topic, payload]() -> as::awaitable<void> {
                        try {
                            co_await m_client->async_publish(topic, payload, am::qos::at_most_once);
                        } catch (const std::exception& e) {
                            std::cerr << "MQTT publish error: " << e.what() << std::endl;
                        }
                        co_return;
                    },
                    as::detached
                );
            });
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to publish message: " << e.what() << std::endl;
            return false;
        }
    }

    bool subscribe(const std::string& topic) {
        try {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_connected) {
                std::cerr << "Cannot subscribe: Not connected to MQTT broker" << std::endl;
                return false;
            }
            as::post(m_ioc, [this, topic]() {
                as::co_spawn(
                    m_ioc,
                    [this, topic]() -> as::awaitable<void> {
                        try {
                            std::vector<am::topic_subopts> sub_entry;
                            sub_entry.push_back({topic, am::qos::at_most_once});
                            auto suback_opt = co_await m_client->async_subscribe(
                                am::v5::subscribe_packet{
                                    *m_client->acquire_unique_packet_id(),
                                    am::force_move(sub_entry)
                                },
                                as::use_awaitable
                            );
                            if (!suback_opt) {
                                std::cerr << "Failed to subscribe to topic: " << topic << std::endl;
                                co_return;
                            }
                            std::cout << "Subscribed to topic: " << topic << std::endl;

                            // Start receiving loop
                            while (m_running) {
                                auto pv_opt = co_await m_client->async_recv(as::use_awaitable);
                                if (!pv_opt) break;
                                pv_opt->visit(
                                    am::overload{
                                        [&](client_t::publish_packet& p) {
                                            if (m_callback) {
                                                m_callback(p.topic(), p.payload());
                                            }
                                        },
                                        [](auto&) {}
                                    }
                                );
                            }
                        } catch (const std::exception& e) {
                            std::cerr << "MQTT subscribe error: " << e.what() << std::endl;
                        }
                        co_return;
                    },
                    as::detached
                );
            });
            return true;
        } catch (const std::exception& e) {
            std::cerr << "Failed to subscribe to topic: " << e.what() << std::endl;
            return false;
        }
    }

    void setCallback(std::function<void(const std::string&, const std::string&)> callback) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_callback = callback;
    }





};