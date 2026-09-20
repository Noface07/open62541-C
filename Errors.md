http://{{URL}}/api/GetOpcUaServersWithOrgMappings

{
    "data":{
        "nodeId" : "ND05"
    }
}

{
    "data": [
        {
            "id": 2,
            "dataPointId": 2,
            "name": "Instance 1",
            "ip": "opc.tcp://Asce",
            "port": 53535,
            "nodeId": "ND05",
            "orgMappings": [
                {
                    "id": 1,
                    "hierarchyId": 2,
                    "mapOrgId": 177,
                    "orgShortCode": "DemoTech"
                }
            ]
        }
    ],
    "dataResponse": {
        "returnCode": 0,
        "responseDateTime": "2026-08-08T17:47:40.4336265+05:30",
        "description": "Server list fetched successfully.",
        "requestId": "ca40dfdf-bb20-4fb8-9adf-b8779247b4eb"
    },
    "orgId": 0
}


now the thing is I have one tenant in this server instance

why am I getting 2 namespaces?


server logs:

Γ£ô Configuration file loaded successfully
[server] Parsing appsettings.json ...
[server] JSON parsed OK.
[server] Extracting AppSettings ...
[server] AppSettings OK.
[server] Extracting MqttConfig ...
[server] MqttConfig OK ΓÇö broker=216.48.184.131:15576
[EdgeConfig] Attempting to load EdgeConfig_ND07_Server_*.txt ...
[EdgeConfig] Executable directory : D:\OPC UA Server\OPCUA- open 62451\open62541-C\build\bin\examples
[EdgeConfig] Searching for EdgeConfig_Server_* file ...
[EdgeConfig] Found file: .\EdgeConfig_Server_ND05.txt
[EdgeConfig] Opening file ...
[EdgeConfig] Read 684 bytes of ciphertext.
[EdgeConfig] Decrypting content ...
[EdgeConfig] Decrypted OK (253 chars). Parsing JSON ...
[EdgeConfig] Γ£ô Config loaded successfully!
[EdgeConfig]   Id        : 31
[EdgeConfig]   Name      : Demo Node
[EdgeConfig]   ShortCode : ND05
[EdgeConfig]   IP        : 192.124.120.129
[EdgeConfig]   UserName  : ND05
[EdgeConfig]   ClientId  : XEE-EDGENT-UA_SERVER-ND05-810476
Γ£ô Configuration parsed successfully
API Host: 157.20.190.140:5128
Acquiring bearer token (attempt 1/3)...
Γ£ô Bearer token acquired successfully
[EdgeConfig] MQTT credentials set ΓÇö User=ND05, Password={token:<bearer token set>} {"token":"eyJhbGciOiJSUzI1NiIsImtpZCI6IjBGQTUzMzBENURENTRCQjUzOEY5RUMwRTQ0NzJGOUYzRjBGM0VBNDIiLCJ4NXQiOiJENlV6RFYzVlM3VTQtZXdPUkhMNThfRHo2a0kiLCJ0eXAiOiJhdCtqd3QifQ.eyJpc3MiOiJodHRwOi8vMTU3LjIwLjE5MC4xNDA6NTA0OC8iLCJleHAiOjE3ODYxOTIyNTYsImlhdCI6MTc4NjE5MTM1Niwic2NvcGUiOiJXZWIgTW9iaWxlIG9mZmxpbmVfYWNjZXNzIiwianRpIjoiNTljNzA5MjctNjQ2MC00ZGZiLTg0YTQtNTk4MWNlYjc4ZjljIiwic3ViIjoibmQwNUBub2RlLmxvY2FsIiwicHJlZmVycmVkX3VzZXJuYW1lIjoiRGVtbyBOb2RlIE5vZGUiLCJuYW1lIjoiRGVtbyBOb2RlIE5vZGUiLCJhdXRoX3RpbWUiOjE3ODYxOTEzNTYsIlVzZXJJZCI6Ik16QXpORFE9IiwiVXNlckNvZGUiOiJORDA1IiwiT3JnSWQiOiIxNzciLCJPcmdOYW1lIjoiRGVtb1RlY2ggSW5kdXN0cmllcyBMdGQuIiwiT3JnVGltZVpvbmUiOiJUWl9JU1RfS09MS0EiLCJBZG1pbkxldmVsIjoiMCIsIkxhbmd1YWdlSWQiOiIxIiwiRW1haWwiOiJuZDA1QG5vZGUubG9jYWwiLCJzaWQiOiIxODBhODExYi01MjViLTQyMDMtYjZmNi02Y2Q3ZGEwNDg4ZjUiLCJvaV9hdV9pZCI6IjE4MGE4MTFiLTUyNWItNDIwMy1iNmY2LTZjZDdkYTA0ODhmNSIsIm9pX3Byc3QiOiJyb2NsaWVudCIsImNsaWVudF9pZCI6InJvY2xpZW50Iiwib2lfdGtuX2lkIjoiMjhmNDU0NzUtYWU4YS00MTZiLTliN2ItNGQ5Y2RlMDYzYTQ3In0.C_YFbfZ-MJM_wJDHNn4nYAN5e-UtbD9AAJ4s2zl9cfkEtnMhFZuZcNLIqtiUq-J01ViPWI96AcNQGiBsSuXGD47cmtJlDkm5c8A5AAUce9QFp6GSv0BegjUR7RMEqN2IEJOBa_ydDbL7oGjqbxqYsRbuTqU3FDkiu4WpPwjuXj_FNUq4B8J_dk-12r2CnTkVFM5HddOl-6RRbJ17TUXOu4MEyBHZ4nX0lw_pwdCITjSTMZ1IjlalLmfDUhAsDyPEcHSw5F7TMDctuGKZrrml1tDmBTu0xqac099dQ8zWs2DXB4k4DpWK_lwkpfxgUu-AxR7-jdbEJ3H-2yHNz8Lf5Q"}the following is the password
Logging initialized: logs/server/2026-08-08.log
[2026-08-08 17:45:56.687 (UTC+0550)] info/server        Loaded 9 trusted certificate(s).
[2026-08-08 17:45:56.687 (UTC+0550)] info/server        Loaded 1 issuer certificate(s).
[2026-08-08 17:45:56.687 (UTC+0550)] info/server        Loaded 0 revocation certificate(s).
[2026-08-08 17:45:56.689 (UTC+0550)] warn/server        AccessControl: Unconfigured AccessControl. Users have all permissions.
[2026-08-08 17:45:56.689 (UTC+0550)] info/server        AccessControl: Anonymous login is enabled
[2026-08-08 17:45:56.689 (UTC+0550)] warn/server        x509 Certificate Authentication configured, but no encrypting SecurityPolicy. This can leak credentials on the network.
[SQLite] Config cached: SERVER_CONFIGS_ND05 (179 bytes)
[2026-08-08 17:45:56.828 (UTC+0550)] info/server        Server Endpoint 0: (null)
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#None
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityMode: 1
[2026-08-08 17:45:56.828 (UTC+0550)] info/server        Server Endpoint 1: (null)
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityMode: 2
[2026-08-08 17:45:56.828 (UTC+0550)] info/server        Server Endpoint 2: (null)
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Basic256Sha256
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityMode: 3
[2026-08-08 17:45:56.828 (UTC+0550)] info/server        Server Endpoint 3: (null)
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityMode: 2
[2026-08-08 17:45:56.828 (UTC+0550)] info/server        Server Endpoint 4: (null)
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Aes256_Sha256_RsaPss
[2026-08-08 17:45:56.828 (UTC+0550)] info/server          Server SecurityMode: 3
[2026-08-08 17:45:56.829 (UTC+0550)] info/server        Server Endpoint 5: (null)
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityMode: 2
[2026-08-08 17:45:56.829 (UTC+0550)] info/server        Server Endpoint 6: (null)
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Aes128_Sha256_RsaOaep
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityMode: 3
[2026-08-08 17:45:56.829 (UTC+0550)] info/server        Server Endpoint 7: (null)
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#ECC_nistP256
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityMode: 2
[2026-08-08 17:45:56.829 (UTC+0550)] info/server        Server Endpoint 8: (null)
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#ECC_nistP256
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityMode: 3
[2026-08-08 17:45:56.829 (UTC+0550)] info/server        Server Endpoint 9: (null)
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Basic128Rsa15
[2026-08-08 17:45:56.829 (UTC+0550)] info/server          Server SecurityMode: 2
[2026-08-08 17:45:56.830 (UTC+0550)] info/server        Server Endpoint 10: (null)
[2026-08-08 17:45:56.830 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Basic128Rsa15
[2026-08-08 17:45:56.830 (UTC+0550)] info/server          Server SecurityMode: 3
[2026-08-08 17:45:56.830 (UTC+0550)] info/server        Server Endpoint 11: (null)
[2026-08-08 17:45:56.830 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Basic256
[2026-08-08 17:45:56.830 (UTC+0550)] info/server          Server SecurityMode: 2
[2026-08-08 17:45:56.830 (UTC+0550)] info/server        Server Endpoint 12: (null)
[2026-08-08 17:45:56.830 (UTC+0550)] info/server          Server SecurityPolicy: http://opcfoundation.org/UA/SecurityPolicy#Basic256
[2026-08-08 17:45:56.830 (UTC+0550)] info/server          Server SecurityMode: 3
[2026-08-08 17:45:56.831 (UTC+0550)] info/network       TCP     | Listening on all interfaces
[2026-08-08 17:45:56.838 (UTC+0550)] info/network       TCP 580 | Creating listen socket for "Asce" (with local hostname "Asce") on port 53535
[2026-08-08 17:45:56.839 (UTC+0550)] info/network       TCP 672 | Creating listen socket for "Asce" (with local hostname "Asce") on port 53535
[2026-08-08 17:45:57.335 (UTC+0550)] info/network       TCP 660 | Connection opened from "127.0.0.1" via the server socket 672
[2026-08-08 17:45:57.335 (UTC+0550)] info/channel       TCP 660 | SC 1  | SecureChannel created
[SQLite] Config cached: USER_PROFILE_Admin.DemoTech (885 bytes)
[2026-08-08 17:46:13.365 (UTC+0550)] info/channel       TCP 660 | SC 1  | SecureChannel closed
[2026-08-08 17:46:13.365 (UTC+0550)] info/network       TCP 660 | Socket closed
[2026-08-08 17:46:16.762 (UTC+0550)] info/network       TCP 680 | Connection opened from "127.0.0.1" via the server socket 672
[2026-08-08 17:46:16.762 (UTC+0550)] info/channel       TCP 680 | SC 2  | SecureChannel created
[2026-08-08 17:46:16.965 (UTC+0550)] info/channel       TCP 680 | SC 2  | SecureChannel closed
[2026-08-08 17:46:16.965 (UTC+0550)] info/network       TCP 680 | Socket closed
[2026-08-08 17:46:17.957 (UTC+0550)] info/network       TCP 508 | Connection opened from "127.0.0.1" via the server socket 672
[2026-08-08 17:46:17.957 (UTC+0550)] info/channel       TCP 508 | SC 3  | SecureChannel created
[2026-08-08 17:46:18.205 (UTC+0550)] info/channel       TCP 508 | SC 3  | SecureChannel closed
[2026-08-08 17:46:18.205 (UTC+0550)] info/network       TCP 508 | Socket closed
[SQLite] Config cached: TOPIC_LIST_177 (29224743 bytes)
[2026-08-08 17:46:30.704 (UTC+0550)] info/network       TCP 732 | Connection opened from "127.0.0.1" via the server socket 672
[2026-08-08 17:46:30.704 (UTC+0550)] info/channel       TCP 732 | SC 4  | SecureChannel created
[2026-08-08 17:46:30.904 (UTC+0550)] info/channel       TCP 732 | SC 4  | SecureChannel closed
[2026-08-08 17:46:30.904 (UTC+0550)] info/network       TCP 732 | Socket closed
[2026-08-08 17:46:30.966 (UTC+0550)] info/network       TCP 676 | Connection opened from "127.0.0.1" via the server socket 672
[2026-08-08 17:46:30.966 (UTC+0550)] info/channel       TCP 676 | SC 5  | SecureChannel created
[SQLite] Config cached: USER_PROFILE_Admin.DemoTech (888 bytes)
[SQLite] Config cached: ALARMS_177 (8888 bytes)


