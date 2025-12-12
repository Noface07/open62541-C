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
