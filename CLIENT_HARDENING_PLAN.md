# OPC UA Client Hardening — Build Plan & Tickets

Target: `client.exe` (MSVC build of the `client` project). Scope is the client
target only: `client.cpp`, `Monitoring.cpp/.h`, `MQTThandler.cpp/.h`,
`structs.h`, `fetchAPI.cpp/.h`, `SqliteQueueService.cpp/.h`.
`server_cpp` is NOT touched by this work except where a shared file changes
(noted per ticket — those changes are additive and behavior-preserving for the
server).

Build is done through MSVC (Visual Studio solution in `build/`). Do not run
`cmake --build`.

---

## Build plan

Pre-check already done: every changed source (`client.cpp`, `Monitoring.cpp`,
`MQTThandler.cpp`, `fetchAPI.cpp`, `SqliteQueueService.cpp`,
`TelemetrySpill.cpp`) plus the server target files that share the changed
headers (`server.cpp`, `SessionWorker.cpp`) parse with **0 errors** under
`g++ -std=c++20 -fsyntax-only` using the project's include paths. That is a
parse check, not an MSVC build — step 3 below is still required.

1. Pull the working tree (branch `unredis`).
2. Open the solution in Visual Studio, configuration **Release x64**.
3. Build project `client` → `build\bin\examples\client.exe`.
   Also rebuild `server_cpp` (shared files changed: `fetchAPI.cpp/.h`,
   `structs.h`, `SqliteQueueService.cpp/.h`, `Monitoring.cpp/.h`,
   `MQTThandler.cpp/.h`). Expect 0 errors; new warnings are acceptable.
4. Smoke test (see "Verification" below) on the dev machine against the
   local API/broker (`appsettings.json` currently points at 127.0.0.1).
5. Copy `client.exe` into `build\bin\OPC UA Client\` and recompile
   `opcuaclient.iss` with Inno Setup.
6. Deploy. Rollback = redeploy the previous `client.exe` (no schema or
   config format changes; the SQLite DB is forward/backward compatible).

### Verification checklist (run in this order)

| # | Check | Pass criteria |
|---|-------|---------------|
| V1 | Start client with `--debug` | Connects to every server in hierarchy; `logs/client/<date>.log` written even without `--debug` (C-28) |
| V2 | Watch a tag in UaExpert while client runs | Values publish to MQTT; no `MonitoredItem failed` lines |
| V3 | Send an MQTT control write for a Boolean and an Int32 tag | `Value written successfully` — no `BadTypeMismatch` (C-03) |
| V4 | Send an MQTT control write for a Double tag | Still works (regression guard for C-03) |
| V5 | Kill the OPC UA server, restart it, wait for reconnect | Values resume; process RSS does not grow across 5 reconnect cycles (C-05) |
| V6 | HTRLD Add a datapoint, then HTRLD Reload | The added datapoint is still monitored after the reload (C-06) |
| V7 | HTRLD Add/Delete storm (10 commands) while publishing control writes | No crash (C-01, C-02) |
| V8 | Stop MQTT broker for 2 min, restart | Values spill to SQLite and drain; API upload goes to `ApplicationEndURLHost:Port` (C-11, C-18) |
| V9 | `sc stop` the service while API is unreachable at startup | Service stops within ~10 s instead of hanging (C-15) |
| V10 | Break `appsettings.json`, start as service | Service reports a non-zero exit code in Event Viewer / `sc query` (C-22) |
| V11 | `client.exe --install` / `sc start AnexeeOPCUAClient` / `client.exe --uninstall` | Service registers, starts, stops, unregisters (C-37) |
| V12 | Let the client run > 1 h with MQTT down | `OfflineQueue` rows older than `RetentionDays` are purged (C-19) |
| V13 | Change a tag in the backend, restart the client (API reachable) | The change is live after restart; log shows `fetched from API` (C-07) |
| V14 | Block the API host, restart the client | Starts from cache; log shows `loaded from local database (API unavailable)` (C-07) |

---

## Tickets

Severity: **P0** = crash / data loss / security, **P1** = wrong behavior in
the field, **P2** = robustness / cleanup. Status: `DONE` = implemented in this
change set, `PENDING` = needs a decision, `SKIP` = explicitly left alone.

| ID | Sev | Status | Title | Files | Risk of the fix |
|----|-----|--------|-------|-------|-----------------|
| C-01 | P0 | DONE | `Mapping`/`TopicMapping` accessed from hot-reload + MQTT threads without a lock | `fetchAPI.h/.cpp`, `client.cpp` | Low — adds `g_mappingMutex`; every access wrapped; no logic change |
| C-02 | P0 | DONE | `ctx->subscriptions` / `groupIdToName` mutated on OPC thread, iterated on hot-reload thread | `client.cpp` | Low — adds `ClientContext::subsMutex`; copies taken under lock |
| C-03 | P0 | DONE | Control writes always send `UA_Double` → `BadTypeMismatch` on non-Double nodes | `client.cpp` | Low — reads the node's DataType first; falls back to Double if the read fails |
| C-04 | P1 | DONE (with C-03) | Control writes use the stored `ns=` instead of the resolved namespace; `s=` ids unsupported | `client.cpp` | Low — uses `resolvedNs` when known, stored index otherwise |
| C-05 | P0 | DONE | `MyMonitorContext` leaked on every reconnect / delete | `structs.h`, `Monitoring.cpp`, `client.cpp` | Medium — contexts are now `shared_ptr`; tasks hold a ref; released only on OPC thread after disconnect |
| C-06 | P0 | DONE | Hot-reload Adds lost after Reload / server Update | `client.cpp` | Low — per-context `hotAdded` list replayed after `onConnected` |
| C-07 | P0 | DONE | Startup reads SQLite cache first; backend changes never picked up on restart | `client.cpp` | Medium — hierarchy and UserProfile are now API-first. Cache is used only when the API call throws (offline) or, for the profile, returns an incomplete record. An API call that succeeds with an **empty** hierarchy is treated as authoritative (no stale cache) and retried, same as before. |
| C-08 | P0 | SKIP | Plaintext creds / private keys written to `offline_queue.db` | — | Left per instruction |
| C-09 | P0 | SKIP | MQTT TLS does not verify broker cert | — | Left per instruction |
| C-10 | P0 | DONE | Hardcoded fallback credentials + profile cache keyed on hardcoded email | `client.cpp` | Low — fallback is now the EdgeConfig identity; cache key is `USER_PROFILE_<EdgeConfig user>` |
| C-11 | P1 | DONE | Bulk upload URL hardcoded to a stale IP | `client.cpp` | Low — built from `ApplicationEndURLHost:Port`. Metadata numbers left as-is (unknown server contract) |
| C-12 | P0 | DONE | Pointer arithmetic on string literal in `MonitorEvent` log | `Monitoring.cpp` | None |
| C-13 | P0 | DONE | `myLog` lacks format-specifier guard + try/catch | `client.cpp` | None — mirrors the server's proven version |
| C-14 | P1 | SKIP | Blocking sign-in on MQTT thread per reconnect | — | Left per instruction |
| C-15 | P1 | DONE | Startup hierarchy loop ignores `g_running` | `client.cpp` | None |
| C-16 | P1 | DONE | ExprTk compile + NodeId parse run on the OPC thread | `client.cpp` | Medium — moved to worker tasks; namespace resolved once on OPC thread first |
| C-17 | P1 | DONE | `getBearerTokenNow` captures by-value params by reference into `std::async` | `client.cpp` | Low — captures by value; nested async removed |
| C-18 | P1 | DONE | Failed QoS0 publishes silently dropped; pending-message machinery dead | `MQTThandler.cpp/.h` | Low — failed publishes now routed to the existing failed-message callback (SQLite spill); dead members removed |
| C-19 | P1 | DONE | `RetentionDays`/`RetryBatchSize`/`ApplicationEndURL` unused; DB grows forever | `SqliteQueueService.cpp/.h`, `client.cpp` | Low — hourly purge of `OfflineQueue` rows older than `RetentionDays`; batch size wired |
| C-20 | P1 | DONE | Per-tag `samplingInterval`/`queueSize` ignored; deadband truncated to int | `structs.h`, `fetchAPI.cpp`, `client.cpp` | Medium — backend values now take effect (0 still means "server default"). Note: hot-reload Adds still use **no** deadband — the hot-reload API ships `deadbandPercent`, but the monitor filter is absolute, so applying it would be wrong. Startup tags use the absolute `deadband` field as before (now as a double). |
| C-21 | P1 | DONE | Hot-reload Add attaches to the first group regardless of parent | `client.cpp` | Low — resolves group via `parentId` → `groupIdToName`; falls back to first group |
| C-22 | P1 | DONE | Service always reports `NO_ERROR` on failure | `client.cpp` | None |
| C-23 | P1 | SKIP (explained) | CRL glob only loads `*.der`; issuers loaded but unused | — | Loading a stale CRL could reject valid chains; not changing before deploy |
| C-24 | P1 | DONE | `securityPolicyUri` leaked per reconnect; unknown policy → `#None` with SIGN mode | `client.cpp` | Low — clears before alloc; unknown policy leaves SDK default and warns |
| C-25 | P1 | DONE | `edgeCfg` mutated from MQTT thread via captured reference | `client.cpp` | None — callback captures user/password by value |
| C-26 | P2 | DONE | 32 worker threads per server | `client.cpp` | Low — `WORKER_COUNT` 32 → 4 (tasks are microseconds) |
| C-27 | P2 | DONE | Blocking `MessageBox` prompts in console mode | `client.cpp` | None — start/started prompts removed; "already running" error box kept |
| C-28 | P2 | DONE | File logging off unless `--debug` | `client.cpp` | None — file logging always on; console echo still `--debug` only |
| C-29 | P2 | DONE | MQTT callback registered after subscriptions/servers → startup messages dropped | `client.cpp` | Low — callback registered right after MQTT connect; hot-reload tasks queue until worker starts |
| C-30 | P2 | SKIP | Only `Data[0]` processed | — | Left per instruction |
| C-31 | P2 | DONE | Event subscription per server is pure overhead | `client.cpp` | Low — base/event subscription no longer created |
| C-32 | P2 | DONE | `requestDateTime` hardcoded | `client.cpp` | None |
| C-33 | P2 | DONE | Legacy `MonitorItem` path (detached threads, non-terminated string, leaks) | `Monitoring.cpp/.h` | None — dead code removed (no callers) |
| C-34 | P2 | DONE | Duplicated `AUTH_STG_ANYMS` condition | `client.cpp` | None |
| C-35 | P2 | DONE | Destructor stops io_context before posted DISCONNECT runs | `MQTThandler.cpp` | Low — waits up to 1 s for the disconnect to be issued |
| C-36 | P2 | DONE | `printf` of user profile to console | `client.cpp` | None |
| C-37 | P2 | DONE | No `--install`/`--uninstall`; installer references nonexistent `--service` | `client.cpp` | Low — service name unchanged (`AnexeeOPCUAClient`) so existing installs keep working |

---

## Notes for whoever deploys

- No config file format changes. `appsettings.json` is read exactly as before;
  `RetentionDays` and `RetryBatchSize` now actually do something.
- Service name is still `AnexeeOPCUAClient`. Existing registrations keep working.
- `logs/client/` is now written on every run. Disk usage is small (day-wise
  files) but worth knowing.
- If a tag's backend `samplingInterval`/`queueSize` was set to something odd,
  it now takes effect (C-20). Previously everything was sampling=0 / queue=10000.
