# Architecture Map - `examples/client` and `examples/server_cpp`

This document maps the current architecture of the complex C++ examples in this repository:

- `client` executable
- `server_cpp` executable
- shared support modules linked into both

It is intended as a navigation and refactor guide.

## 1) Build Targets and Composition

Source of truth: `examples/CMakeLists.txt`

- `client` target:
  - `client.cpp`
  - shared lib sources from `CLIENT_LIB_SOURCES`
- `server_cpp` target:
  - `server.cpp`
  - same shared lib sources
  - extra server-only modules:
    - `AandC.cpp`
    - `SessionManager.cpp`
    - `SessionWorker.cpp`
    - `AccessControl.cpp`
    - `InstrumentedMutex.cpp`
- shared `CLIENT_LIB_SOURCES`:
  - `MQTThandler.cpp`
  - `SqliteQueueService.cpp`
  - `fetchAPI.cpp`
  - `Encryption.cpp`
  - `Monitoring.cpp`
  - `RedisClient.cpp`
  - `EdgeConfigLoader.cpp`

## 2) Runtime Topology (High Level)

### Client (`examples/client.cpp`)

Main responsibilities:

1. Load app settings and encrypted edge config.
2. Obtain bearer token and derive MQTT credentials.
3. Load OPC UA hierarchy (Redis -> SQLite -> API fallback).
4. Build one `ClientContext` per OPC UA endpoint.
5. Create subscriptions and monitored items.
6. Bridge OPC data/events to MQTT and SQLite queueing.
7. Handle incoming MQTT control commands and write to OPC UA.

Key integration modules:

- API + parsing: `fetchAPI.cpp/.h`
- MQTT transport: `MQTThandler.cpp/.h`
- offline queue + API uploader: `SqliteQueueService.cpp/.h`
- monitoring callbacks: `Monitoring.cpp/.h`
- Redis cache: `RedisClient.cpp/.h`

### Server (`examples/server.cpp`)

Main responsibilities:

1. Load app settings + edge config + certificates.
2. Initialize OPC UA server and security/access control.
3. Start MQTT runtime and subscription orchestration.
4. Authenticate sessions and map users to org tenants.
5. Spawn/reuse per-org worker contexts through `SessionManager`.
6. Build org-specific namespace/tag/alarm nodes via worker jobs.
7. Process incoming MQTT telemetry/event payloads and update OPC UA.

Server-only domain modules:

- alarms and conditions: `AandC.cpp/.h`
- access control callbacks: `AccessControl.cpp/.h`
- session lifecycle: `SessionManager.cpp/.h`
- org worker logic: `SessionWorker.cpp`

## 3) Process / Mode Model

### Client modes

- Normal console mode:
  - entry via `main()` -> `runClient(...)`
- Windows service mode:
  - `WinMain` service dispatcher path
  - service worker thread calls `runClient(...)`

### Server modes

- Normal console mode:
  - `main()` -> `RunServer(...)`
- Windows service management:
  - `--install`, `--uninstall` in `main()`
  - helpers in `ServiceUtils.h`
- Windows service runtime:
  - `--service` -> `ServiceMain` -> `RunServer(...)`
- Manager/child behavior in `RunServer(...)`:
  - manager fetches server configs and can spawn child instances
  - child mode consumes bootstrap config from stdin

## 4) Concurrency and Thread Ownership

### Server-side thread model

- OPC UA main loop thread:
  - owns all `UA_Server` mutations
  - drains `g_serverQueue` and runs `UA_Server_run_iterate`
- MQTT io thread:
  - runs Boost.Asio `ioc.run()`
  - performs MQTT connect/reconnect/recv/subscription tasks
- Session worker threads:
  - one shared worker per org (reused across sessions)
  - fetch org topic/alarm configs and enqueue server jobs

Key safety boundary:

- cross-thread server work is passed via `enqueueServerJob(...)`

### Client-side thread model

- one thread per `ClientContext` (per OPC endpoint):
  - connect/reconnect and `UA_Client_run_iterate`
  - execute queued tasks (monitor setup, writes)
- MQTTHandler internal thread:
  - runs Asio loop and reconnect logic
- Async publisher thread (`AsyncPublisher`):
  - non-blocking MQTT publish queue
- SQLite service threads:
  - queue worker
  - API upload worker

## 5) Data Flows

### Client telemetry path

1. OPC UA monitored callback (`Monitoring.cpp`) receives value/event.
2. Payload normalized into `MqttPayload`.
3. Publish to MQTT directly or via `AsyncPublisher`.
4. Persist/enqueue to SQLite through `SqliteQueueService`.
5. API uploader flushes when MQTT connectivity allows.

### Client control path

1. MQTT callback in `client.cpp` receives command payload.
2. Resolve TagId -> endpoint/node via global `Mapping`.
3. Push write task into target `ClientContext::taskQueue`.
4. Endpoint thread executes OPC UA write.

### Server ingest path

1. MQTT recv loop gets telemetry/alarm/event payloads.
2. Parse payload and route by topic semantics.
3. Build job data and enqueue via `enqueueServerJob`.
4. OPC UA main loop executes write/update/alarm operations.
5. Alarms trigger condition events and update branch/cache state.

## 6) State Stores and Caching Strategy

Three-tier read strategy appears in multiple flows:

1. Redis cache (`RedisClient`) - preferred
2. SQLite config cache (`SqliteQueueService::GetConfig`) - fallback
3. REST API (`fetchAPI::getResponse`) - source of truth

Typical cached keys:

- `USER_PROFILE_<user>`
- `TOPIC_LIST_<orgId>`
- `ALARMS_<orgId>`
- `OPCUA_HIERARCHY_<NodeID>`
- `SERVER_CONFIGS_<NodeID>`

## 7) Module Map (What Is Where)

### Entry points and orchestration

- `examples/client.cpp`:
  - client bootstrap, service mode hooks, endpoint context lifecycle
- `examples/server.cpp`:
  - server bootstrap, MQTT runtime, tenant/session callbacks, main server loop
- `examples/RedisUpdater.cpp`:
  - utility CLI for warming/updating Redis + SQLite cache

### Messaging and transport

- `examples/MQTThandler.h/.cpp`:
  - MQTT connect/reconnect, subscribe/publish, callbacks, token refresh hook
- `examples/structs.h`:
  - async publisher queue and core hierarchy data structs

### API, parsing, and models

- `examples/fetchAPI.h/.cpp`:
  - HTTP API calls and JSON -> model parsing
  - defines global `Mapping` and `TopicMapping`
- `examples/OrgConfig.h`, `examples/ServerConfig.h`, `examples/AlarmConfig.h`, `examples/UserProfile.h`:
  - API model structs

### Persistence and cache

- `examples/RedisClient.h/.cpp`:
  - raw RESP Redis client + optional zlib payload compression
- `examples/SqliteQueueService.h/.cpp`:
  - offline queue, latest value store, config cache, API upload workers

### OPC UA monitoring/client data plane

- `examples/Monitoring.h/.cpp`:
  - monitored item/event callbacks and NodeId parsing

### Server multi-tenancy and alarms

- `examples/SessionManager.h/.cpp`:
  - session registration, worker reuse, session context lifecycle
- `examples/SessionWorker.cpp`:
  - org-specific topic/alarm fetch + address-space job generation
- `examples/AccessControl.h/.cpp`:
  - access control callback wiring
- `examples/AandC.h/.cpp`:
  - alarm condition methods/callbacks and branch/cache management
- `examples/InstrumentedMutex.cpp`:
  - mutex lock/hold-time instrumentation helper used by alarm/session code

### Configuration, crypto, utilities

- `examples/EdgeConfigLoader.h/.cpp`:
  - decrypt and parse `EdgeConfig_*.txt`; derive MQTT password from bearer token
- `examples/Encryption.h/.cpp`:
  - AES-based helper encryption/decryption utilities
- `examples/Logger.h`:
  - thread-safe logging and log file setup
- `examples/ServiceUtils.h`:
  - Windows service install/uninstall helpers for server

## 8) External Dependencies and Boundaries

Core external integrations:

- OPC UA SDK: `open62541`
- MQTT + async runtime: `async_mqtt`, Boost.Asio
- HTTP client path: Boost.Beast
- Cache: Redis (custom RESP client)
- Local persistence: SQLite
- JSON: `nlohmann::json`
- compression: `zlib`

Primary runtime boundary decision:

- `UA_Server` mutation is centralized to one owner loop (job queue pattern).
- Client endpoint operations are isolated by endpoint thread contexts.

## 9) Practical Refactor Boundaries (for future work)

Good module boundaries that already exist and can be extracted/cleaned independently:

1. `transport-mqtt`: `MQTThandler.*`
2. `storage-offline`: `SqliteQueueService.*`
3. `api-adapter`: `fetchAPI.*` + model headers
4. `tenant-runtime`: `SessionManager.*` + `SessionWorker.cpp`
5. `alarm-engine`: `AandC.*` + `AccessControl.*`

These are the safest places to start when implementing hot-reload and reducing monolith complexity.
