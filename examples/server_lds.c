#include <open62541/plugin/log_stdout.h>
#include <open62541/server.h>
#include <open62541/server_config_default.h>

#include <signal.h>
#include <stdlib.h>
#include <stdio.h>

static UA_Boolean running = true;
static void
stopHandler(int sig) {
    running = false;
}



static UA_ByteString loadFile(const char *path) {
    UA_ByteString fileContents = UA_BYTESTRING_NULL;
    FILE *fp = fopen(path, "rb");
    if(!fp)
        return fileContents;
    fseek(fp, 0, SEEK_END);
    fileContents.length = (size_t)ftell(fp);
    fileContents.data = (UA_Byte *)UA_malloc(fileContents.length * sizeof(UA_Byte));
    fseek(fp, 0, SEEK_SET);
    if(fread(fileContents.data, sizeof(UA_Byte), fileContents.length, fp) != fileContents.length) {
        UA_ByteString_clear(&fileContents);
    }
    fclose(fp);
    return fileContents;
}

#ifdef _WIN32
static size_t
loadCertsFromDirectory(const char *dirPath, UA_ByteString **certs) {
    char searchPath[512];
    snprintf(searchPath, sizeof(searchPath), "%s\\*.der", dirPath);

    WIN32_FIND_DATAA findData;
    HANDLE hFind = FindFirstFileA(searchPath, &findData);

    if (hFind == INVALID_HANDLE_VALUE) {
        return 0;
    }

    // First count the number of files
    size_t count = 0;
    do {
        if (strstr(findData.cFileName, ".der")) {
            count++;
        }
    } while (FindNextFileA(hFind, &findData) != 0);
    FindClose(hFind);

    if (count == 0) {
        return 0;
    }

    // Allocate memory for file names
    char **derFiles = (char**)UA_malloc(sizeof(char*) * count);
    for (size_t i = 0; i < count; i++) {
        derFiles[i] = (char*)UA_malloc(MAX_PATH);
    }

    // Get the file names
    hFind = FindFirstFileA(searchPath, &findData);
    size_t index = 0;
    do {
        if (strstr(findData.cFileName, ".der")) {
            strncpy(derFiles[index], findData.cFileName, MAX_PATH - 1);
            derFiles[index][MAX_PATH - 1] = '\0';
            index++;
        }
    } while (FindNextFileA(hFind, &findData) != 0);
    FindClose(hFind);

    // Load the certificates
    *certs = (UA_ByteString*)UA_malloc(sizeof(UA_ByteString) * count);
    for (size_t i = 0; i < count; ++i) {
        char fullpath[512];
        snprintf(fullpath, sizeof(fullpath), "%s\\%s", dirPath, derFiles[i]);
        (*certs)[i] = loadFile(fullpath);
        UA_free(derFiles[i]);
    }
    UA_free(derFiles);

    return count;
}
#else
/* Load all .der files from a directory */
static size_t
loadCertsFromDirectory(const char *dirPath, UA_ByteString **certs) {
    DIR *dir = opendir(dirPath);
    if(!dir) return 0;

    // First count the number of files
    struct dirent *entry;
    size_t count = 0;
    while((entry = readdir(dir)) != NULL) {
        if(strstr(entry->d_name, ".der"))
            count++;
    }
    rewinddir(dir);
    
    if(count == 0) {
        closedir(dir);
        return 0;
    }

    // Allocate memory for file names
    char **derFiles = (char**)UA_malloc(sizeof(char*) * count);
    for (size_t i = 0; i < count; i++) {
        derFiles[i] = (char*)UA_malloc(NAME_MAX + 1);
    }

    // Get the file names
    size_t index = 0;
    while((entry = readdir(dir)) != NULL) {
        if(strstr(entry->d_name, ".der")) {
            strncpy(derFiles[index], entry->d_name, NAME_MAX);
            derFiles[index][NAME_MAX] = '\0';
            index++;
        }
    }
    closedir(dir);

    // Load the certificates
    *certs = (UA_ByteString*)UA_malloc(sizeof(UA_ByteString) * count);
    for(size_t i = 0; i < count; i++) {
        char fullpath[512];
        snprintf(fullpath, sizeof(fullpath), "%s/%s", dirPath, derFiles[i]);
        (*certs)[i] = loadFile(fullpath);
        UA_free(derFiles[i]);
    }
    UA_free(derFiles);
    
    return count;
}
#endif




int
main(void) {
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);




    UA_ByteString certificate = loadFile("server/own/certs/server_cert.der");
    UA_ByteString privateKey  = loadFile("server/own/certs/server_key.der");
    
    if(certificate.length == 0 || privateKey.length == 0) {
        UA_LOG_FATAL(UA_Log_Stdout, UA_LOGCATEGORY_USERLAND,
                     "Could not load server certificate or key from certs/own/");
        return EXIT_FAILURE;
    }

    UA_ByteString *trustList = NULL;
    size_t trustListSize = loadCertsFromDirectory("server/trusted/certs", &trustList);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Loaded %zu trusted certificate(s).", trustListSize);
    
    UA_ByteString *issuerList = NULL;
    size_t issuerListSize = loadCertsFromDirectory("server/issued/certs", &issuerList);
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Loaded %zu issuer certificate(s).", issuerListSize);

    size_t revocationListSize = 0;
    UA_ByteString *revocationList = NULL;

    UA_Server *server = UA_Server_new();
    UA_ServerConfig *config = UA_Server_getConfig(server);
    size_t nsIdx = UA_Server_addNamespace(server, "urn:my.properties");

    UA_StatusCode retval =
    UA_ServerConfig_setDefaultWithSecurityPolicies(config, 4840,
        &certificate, &privateKey,
        trustList, trustListSize,
        issuerList, issuerListSize,
        revocationList, revocationListSize);


    // UA_ServerConfig_setMinimalCustomBuffer(config, 4840, NULL, 0, 0);

    // This is an LDS server only. Set the application type to DISCOVERYSERVER.
    // NOTE: This will cause UaExpert to not show this instance in the server list.
    // See also: https://forum.unified-automation.com/topic1987.html
    config->applicationDescription.applicationType = UA_APPLICATIONTYPE_DISCOVERYSERVER;
    UA_String_clear(&config->applicationDescription.applicationUri);
    config->applicationDescription.applicationUri = UA_String_fromChars("urn:Anexee.server.application");

    config->secureChannelPKI.clear(&config->secureChannelPKI);
    config->sessionPKI.clear(&config->sessionPKI);
    UA_CertificateGroup_AcceptAll(&config->secureChannelPKI);
    UA_CertificateGroup_AcceptAll(&config->sessionPKI);

    // Add a secure endpoint (example for Basic256Sha256)
    config->endpointsSize = 1;
    config->endpoints = (UA_EndpointDescription *)UA_Array_new(
        1, &UA_TYPES[UA_TYPES_ENDPOINTDESCRIPTION]);

    // Endpoint 0: None/Anonymous
    UA_EndpointDescription_init(&config->endpoints[0]);
    config->endpoints[0].endpointUrl = UA_STRING_ALLOC("opc.tcp://0.0.0.0:4840");
    config->endpoints[0].securityMode = UA_MESSAGESECURITYMODE_NONE;
    config->endpoints[0].securityPolicyUri =
        UA_STRING_ALLOC("http://opcfoundation.org/UA/SecurityPolicy#None");
    config->endpoints[0].transportProfileUri = UA_STRING_ALLOC(
        "http://opcfoundation.org/UA-Profile/Transport/uatcp-uasc-uabinary");

    config->endpoints[0].userIdentityTokensSize = 1;
    config->endpoints[0].userIdentityTokens =
        (UA_UserTokenPolicy *)UA_Array_new(1, &UA_TYPES[UA_TYPES_USERTOKENPOLICY]);

    UA_UserTokenPolicy_init(&config->endpoints[0].userIdentityTokens[0]);
    config->endpoints[0].userIdentityTokens[0].tokenType = UA_USERTOKENTYPE_ANONYMOUS;
    config->endpoints[0].userIdentityTokens[0].policyId =
        UA_STRING_ALLOC("open62541-anonymous-policy");
    config->endpoints[0].userIdentityTokens[0].securityPolicyUri =
        UA_STRING_ALLOC("http://opcfoundation.org/UA/SecurityPolicy#None");

    // Endpoint 1: SignAndEncrypt/Certificate
    // UA_EndpointDescription_init(&config->endpoints[0]);
    // config->endpoints[0].endpointUrl = UA_STRING_ALLOC("opc.tcp://Asce:4840");
    // config->endpoints[0].securityMode = UA_MESSAGESECURITYMODE_SIGNANDENCRYPT;
    // config->endpoints[0].securityPolicyUri =
    // UA_STRING_ALLOC("http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256");
    // config->endpoints[0].userIdentityTokensSize = 1;
    // config->endpoints[0].userIdentityTokens = (UA_UserTokenPolicy *) UA_Array_new(1,
    // &UA_TYPES[UA_TYPES_USERTOKENPOLICY]);
    // UA_UserTokenPolicy_init(&config->endpoints[1].userIdentityTokens[0]);
    // config->endpoints[0].userIdentityTokens[0].tokenType =
    // UA_USERTOKENTYPE_CERTIFICATE; config->endpoints[0].userIdentityTokens[0].policyId =
    // UA_STRING_ALLOC("X509");
    // config->endpoints[0].userIdentityTokens[0].securityPolicyUri =
    // UA_STRING_ALLOC("http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256");

    // for(size_t i = 0; i < config->endpointsSize; i++) {
    //     UA_EndpointDescription *ep = &config->endpoints[i];

    //     UA_String_clear(&ep->endpointUrl);
    //     ep->endpointUrl = UA_STRING_ALLOC("opc.tcp://Asce:4840");

    //     /* Clear existing UserTokenPolicies first */
    //     if(ep->userIdentityTokens) {
    //         UA_Array_delete(ep->userIdentityTokens, ep->userIdentityTokensSize,
    //         &UA_TYPES[UA_TYPES_USERTOKENPOLICY]);
    //     }

    //     config->endpoints[0].securityMode = UA_MESSAGESECURITYMODE_NONE;

    //     /* Set Anonymous UserTokenPolicy */
    //     ep->userIdentityTokensSize = 1;
    //     ep->userIdentityTokens = (UA_UserTokenPolicy *) UA_Array_new(1,
    //     &UA_TYPES[UA_TYPES_USERTOKENPOLICY]);

    //     UA_UserTokenPolicy_init(&ep->userIdentityTokens[0]);
    //     UA_UserTokenPolicy *policy = &ep->userIdentityTokens[0];
    //     policy->tokenType = UA_USERTOKENTYPE_ANONYMOUS;
    //     policy->policyId = UA_STRING_ALLOC("Anonymous");
    //     policy->securityPolicyUri =
    //     UA_STRING_ALLOC("http://opcfoundation.org/UA/SecurityPolicy#None");

    //     /* Log */
    //     UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Endpoint URL: %.*s",
    //                 (int)ep->endpointUrl.length, ep->endpointUrl.data);
    //     UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "SecurityPolicy: %.*s",
    //                 (int)ep->securityPolicyUri.length, ep->securityPolicyUri.data);
    //     UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "SecurityMode: %d",
    //     ep->securityMode);
    // }

    for(size_t i = 0; i < config->endpointsSize; i++) {
        UA_EndpointDescription *ep = &config->endpoints[i];
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "LDS Endpoint %zu: %.*s", i,
                    (int)ep->endpointUrl.length, ep->endpointUrl.data);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "  LDS SecurityPolicy: %.*s",
                    (int)ep->securityPolicyUri.length, ep->securityPolicyUri.data);
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "  LDS SecurityMode: %d",
                    ep->securityMode);
        for(size_t j = 0; j < ep->userIdentityTokensSize; j++) {
            UA_UserTokenPolicy *pol = &ep->userIdentityTokens[j];
            UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER,
                        "    LDS TokenType: %d, PolicyId: %.*s, SecurityPolicyUri: %.*s",
                        pol->tokenType, (int)pol->policyId.length, pol->policyId.data,
                        (int)pol->securityPolicyUri.length, pol->securityPolicyUri.data);
        }
    }

    // config->mdnsEnabled = true;
    // config->mdnsConfig.mdnsServerName = UA_String_fromChars("LDS");

    // config->mdnsConfig.serverCapabilitiesSize = 1;
    // UA_String *caps = (UA_String *) UA_Array_new(1, &UA_TYPES[UA_TYPES_STRING]);
    // caps[0] = UA_String_fromChars("LDS");
    // config->mdnsConfig.serverCapabilities = caps;

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Configured maxSessions: %d",
                config->maxSessions);
    config->maxSessions = 100;

    retval = UA_Server_run(server, &running);

    UA_Server_delete(server);
    // return retval == UA_STATUSCODE_GOOD ? EXIT_SUCCESS : EXIT_FAILURE;
}
