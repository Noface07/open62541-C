#pragma once

#include <open62541/plugin/accesscontrol.h>
#include <open62541/server.h>
#include "SessionManager.h"

// Initialize the custom access control plugin
UA_StatusCode
AccessControl_setup(UA_ServerConfig *config);

// Custom callbacks
UA_UInt32
getUserRightsMask(UA_Server *server, UA_AccessControl *ac,
                  const UA_NodeId *sessionId, void *sessionContext,
                  const UA_NodeId *nodeId, void *nodeContext);

UA_Byte
getUserAccessLevel(UA_Server *server, UA_AccessControl *ac,
                   const UA_NodeId *sessionId, void *sessionContext,
                   const UA_NodeId *nodeId, void *nodeContext);

UA_Boolean
getUserExecutable(UA_Server *server, UA_AccessControl *ac,
                  const UA_NodeId *sessionId, void *sessionContext,
                  const UA_NodeId *methodId, void *methodContext);

UA_Boolean
allowBrowseNode(UA_Server *server, UA_AccessControl *ac,
                const UA_NodeId *sessionId, void *sessionContext,
                const UA_NodeId *nodeId, void *nodeContext);
