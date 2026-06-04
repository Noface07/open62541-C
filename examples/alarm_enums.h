#ifndef ALARM_ENUMS_H
#define ALARM_ENUMS_H

#include <string>

// ============================================================================
// MQTT Payload Enums (matching AE Engine specification)
// ============================================================================

// Source enum: Identifies the origin of the alarm event
enum class AlarmSource {
    OPC = 1,
    Simulator = 2,
    Expr = 3,
    AEEngine = 24
};

// Quality enum: Data quality indicator
enum class AlarmQuality {
    Good = 1,
    Bad = 2,
    Unknown = 3
};

// UpdateType enum: Type of message being sent
enum class UpdateType {
    Telemetry = 1,
    Control = 2,
    BulkData = 3
};

// ============================================================================
// Helper Functions
// ============================================================================

// Convert AlarmSource enum to string
inline std::string alarmSourceToString(AlarmSource source) {
    switch(source) {
        case AlarmSource::OPC: return "OPC";
        case AlarmSource::Simulator: return "Simulator";
        case AlarmSource::Expr: return "Expr";
        case AlarmSource::AEEngine: return "AEEngine";
        default: return "Unknown";
    }
}

// Convert int to AlarmSource enum
inline AlarmSource intToAlarmSource(int value) {
    switch(value) {
        case 1: return AlarmSource::OPC;
        case 2: return AlarmSource::Simulator;
        case 3: return AlarmSource::Expr;
        case 24: return AlarmSource::AEEngine;
        default: return AlarmSource::AEEngine;
    }
}

// Convert AlarmQuality enum to string
inline std::string alarmQualityToString(AlarmQuality quality) {
    switch(quality) {
        case AlarmQuality::Good: return "Good";
        case AlarmQuality::Bad: return "Bad";
        case AlarmQuality::Unknown: return "Unknown";
        default: return "Unknown";
    }
}

// Convert int to AlarmQuality enum
inline AlarmQuality intToAlarmQuality(int value) {
    switch(value) {
        case 1: return AlarmQuality::Good;
        case 2: return AlarmQuality::Bad;
        case 3: return AlarmQuality::Unknown;
        default: return AlarmQuality::Unknown;
    }
}

#endif // ALARM_ENUMS_H
