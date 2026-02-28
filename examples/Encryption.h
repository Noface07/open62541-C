#pragma once
#include <string>
#include <vector>

std::string GetEncryptedString(std::string encryptionKey, std::string data, int encType = 0);
std::string GetDecryptedString(std::string decryptionKey, std::string encryptedBase64, int encType = 0);
    