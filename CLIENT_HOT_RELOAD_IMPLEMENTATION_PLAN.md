# Client Hot Reload Implementation Plan

## Objective
Implement **runtime hot reloading** in the OPC UA client using MQTT trigger topic `HTRLD/Edgents` so that `Add`, `Delete`, `Update`, and `Reload` actions are applied **without client restart**.

## Input Flow
1. MQTT message received on topic `HTRLD/Edgents`
2. Payload shape:
```json
{
  "id": 72,
  "orgId": 14,
  "Reference": "OPCUA",
  "purpose": "OPC_HI_SERVER",
  "action": "Add"
}
```
3. Client calls:
`http://MAINURL/api/EdgentHotReloading`
with same body.
4. Client parses API response and executes runtime action.

## Supported Actions
- `Add`
- `Delete`
- `Update`
- `Reload`

## High-Level Design
1. Treat `HTRLD/Edgents` as a **control trigger channel**.
2. Keep all `UA_Client_*` mutation operations on OPC thread via existing `postOpcTask`.
3. Maintain a runtime registry of subscriptions/monitored items to enable safe remove/update.
4. Use a single hot-reload coordinator queue to serialize action execution.

## Delivery Phases

### Phase 1: Command Ingestion + API Integration (3-4 days)
**Goals**
- Detect and parse `HTRLD/Edgents` messages.
- Validate command payload.
- Call `/api/EdgentHotReloading`.

**Implementation**
- Add topic branch in `examples/client.cpp` MQTT callback.
- Add `HotReloadCommand` DTO + parser + validator.
- Add API helper in `examples/fetchAPI.cpp` (+ declaration in `examples/fetchAPI.h`).
- Normalize nested API response envelope and extract effective payload.

**Acceptance**
- Valid MQTT command triggers API call and produces structured log.
- Invalid command is rejected with clear reason.

---

### Phase 2: Runtime Registry (4-5 days)
**Goals**
- Track live runtime artifacts required for action-based remove/update.

**Registry Contents**
- Hierarchy id
- Tag id
- Endpoint
- Group/subscription id
- Monitored item id
- NodeId and topic mapping

**Implementation**
- Add registry model in `examples/structs.h` (or dedicated registry header).
- Populate registry when monitored items are created in `examples/client.cpp`.
- Add reverse indexes by hierarchy id and tag id.

**Acceptance**
- Given hierarchy id, client can enumerate all associated live monitors/subscriptions.

---

### Phase 3: Action Executor (5-7 days)
**Goals**
- Apply `Add/Delete/Update/Reload` safely at runtime.

**Implementation**
- Add dispatcher:
  - `applyAdd(command, apiData)`
  - `applyDelete(command, apiData)`
  - `applyUpdate(command, apiData)`
  - `applyReload(command, apiData)`
- Ensure all UA mutations run via `postOpcTask`.
- `Add`: create missing subscriptions/monitors, update registry and mapping.
- `Delete`: remove monitored items/subscriptions and registry entries.
- `Update`: compute delta and patch only changed elements.
- `Reload`: full reconcile for scoped hierarchy id.

**Acceptance**
- All four actions complete without process restart.

---

### Phase 4: Concurrency + Safety (3-4 days)
**Goals**
- Prevent race conditions and inconsistent state under burst commands.

**Implementation**
- Add `HotReloadCoordinator` with single worker queue.
- Coalesce commands by hierarchy id:
  - `Reload` supersedes pending `Add/Update/Delete` for same id.
- Add retry/backoff for API failures.
- Define locking rules for shared structures (`Mapping`, registry, client pool access).

**Acceptance**
- Burst commands do not corrupt runtime registry or crash UA flow.

---

### Phase 5: Validation + Soak (3-4 days)
**Test Matrix**
- `Add`, `Delete`, `Update`, `Reload` happy path
- Duplicate commands
- Out-of-order commands
- Reconnect during reload
- API timeout/failure
- Partial apply failure + retry

**Observability**
- Add summary logs per action:
  - `action`, `id`, `duration_ms`, `adds`, `deletes`, `updates`, `status`

**Acceptance**
- Extended churn run completes without leaks/stalls/state drift.

## Estimated Effort
- **MVP (functional + safe):** 15-20 dev days
- **Production-hardened:** 20-25 dev days

## Execution Order (Recommended)
1. Phase 1 command + API plumbing
2. Phase 2 runtime registry
3. Phase 3 `Add` action first
4. Phase 3 `Delete`, `Update`, `Reload`
5. Phase 4 coordinator + coalescing
6. Phase 5 testing and stabilization

## Notes and Constraints
- If `/api/EdgentHotReloading` response is partial, client should fetch full hierarchy snapshot before apply.
- Existing startup parser in `examples/fetchAPI.cpp` can be reused for normalized data construction.
- Avoid direct UA calls from worker threads; route all UA changes through OPC thread task queue.
