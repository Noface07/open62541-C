#pragma once

#include <string>
#include <functional>
#include <boost/asio/io_context.hpp>
#include <thread>
#include <memory>
#include <atomic>
#include <mutex>
#include <async_mqtt/all.hpp>

// Move this outside the class!
using client_t = async_mqtt::client<async_mqtt::protocol_version::v5, async_mqtt::protocol::mqtt>;

// Shared enums (if needed by other files)
enum DataSourceType {
    INFO_STATE, //State
    INFO_INST, //Instentaionus
    INFO_INC, //Counter
    INFO_DCR,
    DT_TYP_TXT, //String
};
enum DataSource {
    OpcDADataSource = 1, //OPC DA
    SIMULATOR = 2,
    EXPRESSION = 3,
    OpcUADataSource = 4
};
enum DataQuality {
    GOOD = 1,
    BAD = 2,
    UNKNOWN = 3,
};
enum UpdateType {
    TELEMETERY = 1,
    COMMAND = 2,
    BULKDATA= 3,
};

class MQTTHandler {
private:
    boost::asio::io_context& m_ioc;
    std::unique_ptr<client_t> m_client;
    std::thread m_mqtt_thread;
    std::atomic<bool> m_running;
    std::mutex m_mutex;
    std::function<void(const std::string&, const std::string&)> m_callback;
    bool m_connected;

public:
    MQTTHandler(boost::asio::io_context &ioc);
    ~MQTTHandler();

    bool connect(const std::string& broker, const std::string& port,
                 const std::string& username, const std::string& password);
    bool isConnected();
    void disconnect();
    bool publish(const std::string& topic, const std::string& payload);
    bool subscribe(const std::string& topic);
    void setCallback(std::function<void(const std::string&, const std::string&)> callback);
};
