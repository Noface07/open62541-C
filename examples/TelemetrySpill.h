#pragma once

#include <map>
#include <string>

class SqliteQueueService;

/**
 * Parse telemetry JSON {"Data":[{...}]} (same shape as Monitoring / MQTT publish)
 * and enqueue to SqliteQueueService. Returns false if sqlite is null, payload empty,
 * or JSON does not match the expected structure.
 */
bool TryEnqueueTelemetryWrapper(const std::string &topic,
                                const std::string &wrapperJson,
                                SqliteQueueService *sqlite,
                                const std::map<int, long> &datapointToOrgId);
