#include "Encryption.h"
#include <openssl/evp.h>
#include <openssl/aes.h>
#include <openssl/rand.h>
#include <openssl/hmac.h>
#include <openssl/buffer.h>
#include <vector>
#include <iostream>
#include <iomanip>
#include <cstring>
#include <sstream>
#include "Logger.h"

// Helper to convert to Base64
static std::string base64_encode(const std::vector<unsigned char>& data) {
    BIO *b64 = BIO_new(BIO_f_base64());
    BIO *bmem = BIO_new(BIO_s_mem());
    b64 = BIO_push(b64, bmem);
    BIO_set_flags(b64, BIO_FLAGS_BASE64_NO_NL); // No newlines
    BIO_write(b64, data.data(), (int)data.size());
    BIO_flush(b64);
    BUF_MEM *bptr;
    BIO_get_mem_ptr(b64, &bptr);
    std::string res(bptr->data, bptr->length);
    BIO_free_all(b64);
    return res;
}

static std::vector<unsigned char>
base64_decode(const std::string &input) {
    BIO *b64 = BIO_new(BIO_f_base64());
    BIO *bmem = BIO_new_mem_buf(input.data(), (int)input.size());
    bmem = BIO_push(b64, bmem);
    BIO_set_flags(bmem, BIO_FLAGS_BASE64_NO_NL);

    std::vector<unsigned char> buffer(input.size());
    int decodedLen = BIO_read(bmem, buffer.data(), (int)input.size());
    buffer.resize(decodedLen > 0 ? decodedLen : 0);

    BIO_free_all(bmem);
    return buffer;
}

std::string GetEncryptedString(std::string encryptionKey, std::string data, int encType) {
    if (encryptionKey.empty()) {
        encryptionKey = "BAKRNOCTECHONDATER"; 
    }

    // 1. Convert data to UTF-16LE bytes (Unicode in C#)
    std::vector<unsigned char> clearBytes;
    for(unsigned char c : data) {
         clearBytes.push_back(c);
         clearBytes.push_back(0); // 0 byte for LE high byte of standard ASCII
    }

    // 2. Key Derivation (PBKDF2)
    // Salt: { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 }
    unsigned char salt[] = { 0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65, 0x64, 0x76, 0x65, 0x64, 0x65, 0x76 };
    unsigned char key[32];
    unsigned char iv[16];
    
    // Rfc2898DeriveBytes in .NET (older ctor) uses HMACSHA1 and 1000 iterations.
    // It generates a pseudo-random stream. We need 48 bytes total (32 Key + 16 IV).
    unsigned char derived[48];
    PKCS5_PBKDF2_HMAC(encryptionKey.c_str(), (int)encryptionKey.length(),
                      salt, sizeof(salt), 1000, EVP_sha1(),
                      48, derived);
    
    std::memcpy(key, derived, 32);
    std::memcpy(iv, derived + 32, 16);

    // 3. AES Encryption (AES-256-CBC)
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    // Use AES-256-CBC because key size derived is 32 bytes (256 bits)
    EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, key, iv);
    
    // Standard PKCS7 padding is enabled by default in OpenSSL
    
    std::vector<unsigned char> encrypted(clearBytes.size() + EVP_MAX_BLOCK_LENGTH); 
    int len;
    int ciphertext_len;

    EVP_EncryptUpdate(ctx, encrypted.data(), &len, clearBytes.data(), (int)clearBytes.size());
    ciphertext_len = len;

    EVP_EncryptFinal_ex(ctx, encrypted.data() + len, &len);
    ciphertext_len += len;
    
    encrypted.resize(ciphertext_len);
    EVP_CIPHER_CTX_free(ctx);

    // 4. Base64 encode the encrypted bytes
    std::string base64Str = base64_encode(encrypted);
    log("DEBUG: Base64 Encrypted: " + base64Str, LogLevel::INFO);
    
    return base64Str;
}

std::string
GetDecryptedString(std::string decryptionKey, std::string encryptedBase64, int encType) {
    if(decryptionKey.empty()) {
        decryptionKey = "TechDC0nf!g";
    }

    // 1. Base64 decode
    std::vector<unsigned char> encryptedBytes = base64_decode(encryptedBase64);

    // 2. Key Derivation (same as encryption)
    unsigned char salt[] = {0x49, 0x76, 0x61, 0x6e, 0x20, 0x4d, 0x65,
                            0x64, 0x76, 0x65, 0x64, 0x65, 0x76};

    unsigned char key[32];
    unsigned char iv[16];
    unsigned char derived[48];

    PKCS5_PBKDF2_HMAC(decryptionKey.c_str(), (int)decryptionKey.length(), salt,
                      sizeof(salt), 1000, EVP_sha1(), 48, derived);

    std::memcpy(key, derived, 32);
    std::memcpy(iv, derived + 32, 16);

    // 3. AES-256-CBC Decryption
    EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
    EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, key, iv);

    std::vector<unsigned char> decrypted(encryptedBytes.size() + EVP_MAX_BLOCK_LENGTH);
    int len;
    int plaintext_len;

    EVP_DecryptUpdate(ctx, decrypted.data(), &len, encryptedBytes.data(),
                      (int)encryptedBytes.size());

    plaintext_len = len;

    if(EVP_DecryptFinal_ex(ctx, decrypted.data() + len, &len) <= 0) {
        EVP_CIPHER_CTX_free(ctx);
        log("ERROR: Decryption failed (bad padding or wrong key)", LogLevel::ERRORS);
        return "";
    }

    plaintext_len += len;
    decrypted.resize(plaintext_len);

    EVP_CIPHER_CTX_free(ctx);

    // 4. Convert UTF-16LE bytes back to std::string
    std::string result;
    for(size_t i = 0; i + 1 < decrypted.size(); i += 2) {
        result.push_back(decrypted[i]);  
    }

    log("DEBUG: Decrypted String: " + result, LogLevel::INFO);

    return result;
} 
