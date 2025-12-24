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
        std::string resp;
        
        // CMD: SET key value [EX ttl]
        if (ttlSeconds > 0) {
            resp = executeCommand({"SET", fullKey, value, "EX", std::to_string(ttlSeconds)});
        } else {
            // Persistent set (no expiration)
            resp = executeCommand({"SET", fullKey, value});
        }

        if (resp == "OK") return true;
        std::cerr << "[REDIS] SET failed: " << resp << std::endl;
        return false;
    } catch (std::exception& e) {
        std::cerr << "[REDIS] SET Exception: " << e.what() << std::endl;
        // Try reconnect once?
        if (reconnect()) {
             try {
                std::string fullKey = m_keyPrefix + key;
                std::string resp;
                if (ttlSeconds > 0) {
                    resp = executeCommand({"SET", fullKey, value, "EX", std::to_string(ttlSeconds)});
                } else {
                    resp = executeCommand({"SET", fullKey, value});
                }
                return (resp == "OK");
             } catch (...) { return false; }
        }
        return false;
    }
}

// Compression Helpers
std::string RedisClient::compressData(const std::string& data) {
    z_stream zs;
    memset(&zs, 0, sizeof(zs));

    if (deflateInit(&zs, Z_BEST_SPEED) != Z_OK) { // Use Best Speed for large payloads
        throw std::runtime_error("deflateInit failed while compressing.");
    }

    zs.next_in = (Bytef*)data.data();
    zs.avail_in = (uInt)data.size();

    int ret;
    char outbuffer[32768];
    std::string outstring;

    // get the compressed bytes blockwise
    do {
        zs.next_out = (Bytef*)outbuffer;
        zs.avail_out = sizeof(outbuffer);

        ret = deflate(&zs, Z_FINISH);

        if (outstring.size() < zs.total_out) {
            // append the block to the output string
            outstring.append(outbuffer, zs.total_out - outstring.size());
        }
    } while (ret == Z_OK);

    deflateEnd(&zs);

    if (ret != Z_STREAM_END) {
        throw std::runtime_error("Exception during zlib compression: " + std::to_string(ret));
    }

    return outstring;
}

std::string RedisClient::decompressData(const std::string& compressedData) {
    z_stream zs;
    memset(&zs, 0, sizeof(zs));

    if (inflateInit(&zs) != Z_OK) {
        throw std::runtime_error("inflateInit failed while decompressing.");
    }

    zs.next_in = (Bytef*)compressedData.data();
    zs.avail_in = (uInt)compressedData.size();

    int ret;
    char outbuffer[32768];
    std::string outstring;

    // get the decompressed bytes blockwise
    do {
        zs.next_out = (Bytef*)outbuffer;
        zs.avail_out = sizeof(outbuffer);

        ret = inflate(&zs, 0);

        if (outstring.size() < zs.total_out) {
            outstring.append(outbuffer, zs.total_out - outstring.size());
        }

    } while (ret == Z_OK);

    inflateEnd(&zs);

    if (ret != Z_STREAM_END) {
        throw std::runtime_error("Exception during zlib decompression: " + std::to_string(ret));
    }

    return outstring;
}

// Magic Header: 4 bytes "ZLIB"
const std::string MAGIC_HEADER = "ZLIB";

bool RedisClient::isCompressed(const std::string& data) {
    if (data.size() < MAGIC_HEADER.size()) return false;
    return (data.compare(0, MAGIC_HEADER.size(), MAGIC_HEADER) == 0);
}

bool RedisClient::setCompressed(const std::string& key, const std::string& value, int ttlSeconds) {
    try {
        std::string compressed = compressData(value);
        std::string payload = MAGIC_HEADER + compressed;
        
        // Log savings (verbose)
        // double ratio = (double)payload.size() / (double)value.size();
        // std::cout << "[REDIS] Compressing " << key << ": " << value.size() << " -> " << payload.size() << " bytes\n";
        
        return set(key, payload, ttlSeconds);
    } catch (std::exception& e) {
        std::cerr << "[REDIS] Compression failed for " << key << ": " << e.what() << "\n";
        return false;
    }
}

std::optional<std::string> RedisClient::get(const std::string& key) {
    std::lock_guard<std::recursive_mutex> lock(m_mutex);
    if (!connect()) return std::nullopt;

    try {
        std::string fullKey = m_keyPrefix + key;
        std::string resp = executeCommand({"GET", fullKey});
        
        if (resp.rfind("-Error", 0) == 0) return std::nullopt;
        if (resp == "") return std::nullopt; // Assumes empty string = miss/null for our JSON case

        // Check for Compression
        if (isCompressed(resp)) {
            // Decompress
            try {
                std::string raw = resp.substr(MAGIC_HEADER.size());
                return decompressData(raw);
            } catch (std::exception& e) {
                 std::cerr << "[REDIS] Decompression failed for " << key << ": " << e.what() << "\n";
                 // Return raw or null? Return null to indicate corruption/failure
                 return std::nullopt;
            }
        }
        
        return resp;

    } catch (std::exception& e) {
        std::cerr << "[REDIS] GET Exception: " << e.what() << std::endl;
        if (reconnect()) {
             try {
                std::string fullKey = m_keyPrefix + key;
                std::string resp = executeCommand({"GET", fullKey});
                if (resp == "") return std::nullopt;
                
                if (isCompressed(resp)) {
                    return decompressData(resp.substr(MAGIC_HEADER.size()));
                }
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
        std::string script = "local keys = redis.call('keys', ARGV[1]) if #keys > 0 then return redis.call('del', unpack(keys)) else return 0 end";
        
        std::string pattern = m_keyPrefix + "*";
        
        std::string resp = executeCommand({"EVAL", script, "0", pattern});
        
        if (resp.length() > 0 && resp[0] == ':') {
             std::cout << "[REDIS] Cleared " << resp.substr(1) << " keys." << std::endl;
        } else {
             std::cout << "[REDIS] Clear cache response: " << resp << std::endl;
        }

    } catch (std::exception& e) {
        std::cerr << "[REDIS] ClearCache Exception: " << e.what() << std::endl;
    }
}

