#ifndef REDIS_CLIENT_H
#define REDIS_CLIENT_H

#include <string>
#include <vector>
#include <mutex>
#include <optional>
#include <boost/asio.hpp>

#include <zlib.h>

class RedisClient {
public:
    RedisClient();
    ~RedisClient();

    // Initialize Connection details
    void init(const std::string& host, int port, const std::string& password, int dbIndex, const std::string& keyPrefix);

    // Connect to Redis
    // Returns true if successful (or already connected)
    bool connect();

    // Store value with TTL (in seconds)
    // Key will be prefixed automatically
    bool set(const std::string& key, const std::string& value, int ttlSeconds);

    // Store COMPRESSED value with TTL
    // Uses ZLIB and adds Magic Header for transparent decompression
    bool setCompressed(const std::string& key, const std::string& value, int ttlSeconds);

    // Retrieve value (Transparently decompresses if magic header is found)
    // Key will be prefixed automatically
    std::optional<std::string> get(const std::string& key);

    // clear all keys with the configured prefix
    void clearCache();

    // Disconnect
    void disconnect();

    // Check if enabled/connected
    bool isConnected();

private:
    // Internal helper to send command and get reply
    std::string executeCommand(const std::vector<std::string>& args);
    std::string readResponse();
    
    // Compression Helpers
    std::string compressData(const std::string& data);
    std::string decompressData(const std::string& compressedData);
    bool isCompressed(const std::string& data);

    // Reconnection logic
    bool reconnect();

    // Members
    std::string m_host;
    int m_port;
    std::string m_password;
    int m_dbIndex;
    std::string m_keyPrefix;

    boost::asio::io_context m_ioc;
    boost::asio::ip::tcp::socket m_socket;
    bool m_connected;

    std::recursive_mutex m_mutex; // Protect socket access
};

// Global Instance Declaration
extern RedisClient g_redisClient;

#endif // REDIS_CLIENT_H
