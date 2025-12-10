#include "AccessControl.h"
#include "SessionManager.h"
#include "Logger.h"
#include <open62541/plugin/accesscontrol_default.h>

extern SessionManager g_sessionManager;

// Helper to check if a user has access to a node
static bool checkAccess(UA_Server *server, const UA_NodeId *sessionId, const UA_NodeId *nodeId) {
    // 1. Allow access to NS0 (Standard OPC UA nodes)
    if(nodeId->namespaceIndex == 0) {
        return true;
    }

    // 2. Get Session Context
    SessionContext* ctx = g_sessionManager.getSession(*sessionId);
    if(!ctx) {
        // If no session context (e.g. anonymous or admin not in SessionManager), 
        // deny access to custom namespaces to be safe, or allow if it's admin.
        // For now, assume strict isolation: No context = No access to tenant data.
        return false;
    }

    // 3. Get Namespace URI of the node
    UA_String uri;
    UA_StatusCode rc = UA_Server_getNamespaceByIndex(server, nodeId->namespaceIndex, &uri);
    if(rc != UA_STATUSCODE_GOOD) {
        return false;
    }

    std::string nsUri((char*)uri.data, uri.length);

    // LOGGING


    // 4. Check if Namespace belongs to the User's Org
    // User's namespace URI: "anexee:{ShortCode}"
    // We can simply check if the node's namespace URI matches the user's namespace URI.
    // Or if it starts with "anexee:" and doesn't match, deny.
    
    if(nsUri == ctx->namespaceUri) {
        return true; // Access granted: Node is in User's Org Namespace
    }

    // 5. Deny access to other Orgs' namespaces
    // If the namespace starts with "anexee:", it belongs to a tenant.
    if(nsUri.rfind("anexee:", 0) == 0) {
        log("⛔ Access Denied: User '" + ctx->shortCode + "' cannot access Node in '" + nsUri + "'", LogLevel::INFO);
        return false; // Access denied: Belongs to another Org
    }

    // 6. Allow access to other namespaces (e.g. local server namespaces not managed by multi-tenancy)
    return true;
}
 
UA_UInt32
getUserRightsMask(UA_Server *server, UA_AccessControl *ac,
                  const UA_NodeId *sessionId, void *sessionContext,
                  const UA_NodeId *nodeId, void *nodeContext) {
    
    if(checkAccess(server, sessionId, nodeId)) {
        return 0xFFFFFFFF; // Full rights
    }
    return 0; // No rights (Hidden)
}

UA_Byte
getUserAccessLevel(UA_Server *server, UA_AccessControl *ac,
                   const UA_NodeId *sessionId, void *sessionContext,
                   const UA_NodeId *nodeId, void *nodeContext) {
    
    if(checkAccess(server, sessionId, nodeId)) {
        return UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE | UA_ACCESSLEVELMASK_HISTORYREAD;
    }
    return 0; // No access
}

UA_Boolean
getUserExecutable(UA_Server *server, UA_AccessControl *ac,
                  const UA_NodeId *sessionId, void *sessionContext,
                  const UA_NodeId *methodId, void *methodContext) {
    
    if(checkAccess(server, sessionId, methodId)) {
        return UA_TRUE;
    }
    return UA_FALSE;
}

UA_Boolean
allowBrowseNode(UA_Server *server, UA_AccessControl *ac,
                const UA_NodeId *sessionId, void *sessionContext,
                const UA_NodeId *nodeId, void *nodeContext) {
    
    if(checkAccess(server, sessionId, nodeId)) {
        return UA_TRUE;
    }
    return UA_FALSE;
}


UA_StatusCode
AccessControl_setup(UA_ServerConfig *config) {
    // Start with default access control (handles login, etc.)
    // We are NOT replacing the entire plugin, just overriding the permission callbacks.
    // The login callback is already set in server.cpp via UA_AccessControl_defaultWithLoginCallback
    
    // Override callbacks
    config->accessControl.getUserRightsMask = getUserRightsMask;
    config->accessControl.getUserAccessLevel = getUserAccessLevel;
    config->accessControl.getUserExecutable = getUserExecutable;
    config->accessControl.allowBrowseNode = allowBrowseNode;

    // Note: We keep the default getUserRole, allowUserStep, etc.
    
    return UA_STATUSCODE_GOOD;
}
