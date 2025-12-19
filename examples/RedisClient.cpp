#include "RedisClient.h"
#include <iostream>
#include <sstream>
#include <boost/asio.hpp>
#include <thread>

using boost::asio::ip::tcp;

RedisClient g_redisClient;

RedisClient::RedisClient() : m_socket(m_ioc), m_connected(false), m_port(6379), m_dbIndex(0) {}

RedisClient::~RedisClient() {
    disconnect();
}

void RedisClient::init(const std::string& host, int port, const std::string& password, int dbIndex, const std::string& keyPrefix) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    m_host = host;
    m_port = port;
    m_password = password;
    m_dbIndex = dbIndex;
    m_keyPrefix = keyPrefix;

    // Ensure prefix ends with a separator if not empty
    if (!m_keyPrefix.empty() && m_keyPrefix.back() != ':') {
        m_keyPrefix += ":";
    }
}

void RedisClient::disconnect() {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    if (m_connected) {
        boost::system::error_code ec;
        m_socket.close(ec);
        m_connected = false;
    }
}

bool RedisClient::isConnected() {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    return m_connected;
}

bool RedisClient::connect() {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    if (m_connected) return true;

    try {
        tcp::resolver resolver(m_ioc);
        auto endpoints = resolver.resolve(m_host, std::to_string(m_port));
        
        // Connect
        boost::asio::connect(m_socket, endpoints);
        m_connected = true;

        // AUTH
        if (!m_password.empty()) {
            std::string resp = executeCommand({"AUTH", m_password});
            if (resp.find("OK") == std::string::npos) {
                std::cerr << "[REDIS] Auth failed: " << resp << std::endl;
                disconnect();
                return false;
            }
        }

        // SELECT DB
        std::string resp = executeCommand({"SELECT", std::to_string(m_dbIndex)});
        if (resp.find("OK") == std::string::npos) {
            std::cerr << "[REDIS] Select DB failed: " << resp << std::endl;
            disconnect();
            return false;
        }

        std::cout << "[REDIS] Connected to " << m_host << ":" << m_port << " DB:" << m_dbIndex << std::endl;
        return true;

    } catch (std::exception& e) {
        std::cerr << "[REDIS] Connection failed: " << e.what() << std::endl;
        m_connected = false;
        return false;
    }
}

bool RedisClient::reconnect() {
    disconnect();
    return connect();
}

// Helper to format command in RESP
// *<num_args>\r\n$<len>\r\n<arg>\r\n...
std::string formatRESP(const std::vector<std::string>& args) {
    std::ostringstream oss;
    oss << "*" << args.size() << "\r\n";
    for (const auto& arg : args) {
        oss << "$" << arg.length() << "\r\n" << arg << "\r\n";
    }
    return oss.str();
}

std::string RedisClient::executeCommand(const std::vector<std::string>& args) {
    if (!m_connected) throw std::runtime_error("Not connected");

    std::string payload = formatRESP(args);
    boost::asio::write(m_socket, boost::asio::buffer(payload));

    return readResponse();
}

std::string RedisClient::readResponse() {
    boost::asio::streambuf buf;
    boost::asio::read_until(m_socket, buf, "\r\n");
    std::istream is(&buf);
    std::string line;
    std::getline(is, line);
    if (!line.empty() && line.back() == '\r') line.pop_back();

    if (line.empty()) return "-Error: Empty response";

    char type = line[0];
    std::string content = line.substr(1);

    switch (type) {
        case '+': // Simple String (e.g., +OK)
            return content;
        case '-': // Error
            return "-Error: " + content;
        case ':': // Integer
            return content; // Return number as string
        case '$': // Bulk String
        {
            int len = std::stoi(content);
            if (len == -1) return ""; // Null

            // Read exact bytes + CRLF
            // We already read the first line. Now read len + 2 bytes.
            // But buf might contain some of it already.
            
            std::string data;
            data.resize(len);
            
            // Read status: how many bytes currently in buf?
            size_t bytes_in_buf = buf.size();
            
            // We need to read 'len' bytes + 2 bytes (CRLF)
            // But 'read_until' documentation says it might read past delimiter.
            // So we just read everything needed.
            
            // Easiest is to cycle read until we have enough.
            size_t to_read = len + 2;
            if (buf.size() < to_read) {
                 boost::asio::read(m_socket, buf, boost::asio::transfer_at_least(to_read - buf.size()));
            }
            
            is.read(&data[0], len);
            char cr, lf;
            is.get(cr); is.get(lf); // Consume \r\n
            
            return data;
        }
        case '*': // Array (not supported for simple SET/GET)
             return "-Error: Array response not supported";
        default:
             return "-Error: Unknown type";
    }
}

bool RedisClient::set(const std::string& key, const std::string& value, int ttlSeconds) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    if (!connect()) return false;

    try {
        std::string fullKey = m_keyPrefix + key;
        // CMD: SET key value EX ttl
        std::string resp = executeCommand({"SET", fullKey, value, "EX", std::to_string(ttlSeconds)});
        if (resp == "OK") return true;
        std::cerr << "[REDIS] SET failed: " << resp << std::endl;
        return false;
    } catch (std::exception& e) {
        std::cerr << "[REDIS] SET Exception: " << e.what() << std::endl;
        // Try reconnect once?
        if (reconnect()) {
             try {
                std::string fullKey = m_keyPrefix + key;
                std::string resp = executeCommand({"SET", fullKey, value, "EX", std::to_string(ttlSeconds)});
                return (resp == "OK");
             } catch (...) { return false; }
        }
        return false;
    }
}

std::optional<std::string> RedisClient::get(const std::string& key) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    if (!connect()) return std::nullopt;

    try {
        std::string fullKey = m_keyPrefix + key;
        // CMD: GET key
        std::string resp = executeCommand({"GET", fullKey});
        
        if (resp.rfind("-Error", 0) == 0) return std::nullopt; // or throw
        if (resp.empty()) return std::nullopt; // Null bulk string often returns empty/null handling in readResponse?
        // Wait, readResponse returns "" for $-1.
        // What if value IS empty string? $0\r\n\r\n. content="0". read returns "".
        // Distinction: 
        // $-1 -> Null
        // $0 -> ""
        // My readResponse returns "" for $-1.
        // It returns "" for $0 \r\n \r\n ?
        // Let's refine readResponse behavior.
        // Actually, if Cache Miss ($-1), we want std::nullopt.
        // If Cache Hit but empty string ($0), we want "".
        // I need to update readResponse signature or convention.
        
        // Simpler: If response is valid data, return it.
        // For cache, if it returns "", we assume miss or empty.
        // Since we store JSON, it's never empty.
        // So "" == Miss/Null is acceptable for this use case.
        
        if (resp == "") return std::nullopt; 
        
        return resp;

    } catch (std::exception& e) {
        std::cerr << "[REDIS] GET Exception: " << e.what() << std::endl;
        // Try reconnect once
        if (reconnect()) {
             try {
                std::string fullKey = m_keyPrefix + key;
                std::string resp = executeCommand({"GET", fullKey});
                if (resp == "") return std::nullopt;
                return resp;
             } catch (...) { return std::nullopt; }
        }
        return std::nullopt;
    }
}

void RedisClient::clearCache() {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    if (!connect()) return;

    try {
        std::cout << "[REDIS] Clearing cache with prefix: " << m_keyPrefix << "*" << std::endl;
        
        // Lua script to find and delete keys atomicaly
        // ARGV[1] is the pattern
        std::string script = "local keys = redis.call('keys', ARGV[1]) if #keys > 0 then return redis.call('del', unpack(keys)) else return 0 end";
        
        std::string pattern = m_keyPrefix + "*";
        
        // CMD: EVAL script 0 pattern
        std::string resp = executeCommand({"EVAL", script, "0", pattern});
        
        // Response should be integer (number of keys deleted)
        // e.g., ":5" or ":0"
        if (resp.length() > 0 && resp[0] == ':') {
             std::cout << "[REDIS] Cleared " << resp.substr(1) << " keys." << std::endl;
        } else {
             std::cout << "[REDIS] Clear cache response: " << resp << std::endl;
        }

    } catch (std::exception& e) {
        std::cerr << "[REDIS] ClearCache Exception: " << e.what() << std::endl;
    }
}
