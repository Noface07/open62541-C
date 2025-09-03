# Enhanced Logging System for OPC UA Server and Client

This document describes the enhanced logging system that provides separate logging folders and day-wise log files for both the OPC UA Server and Client.

## Features

### 1. **Conditional File Logging**
- **Normal mode**: Only ERROR messages logged to files, silent operation for other levels
- **Debug mode**: Full logging with separate folders and day-wise files for all levels
- **Server**: Logs stored in `logs/server/` folder (ERROR always, others in debug mode)
- **Client**: Logs stored in `logs/client/` folder (ERROR always, others in debug mode)

### 2. **Day-wise Log Files**
- Log files are automatically created with date-based names (e.g., `2024-01-15.log`)
- New log files are created automatically when the date changes
- Automatic log rotation based on date
- **Only active when `--debug` flag is used**

### 3. **Enhanced Logging Functions**
- `init_logging(folder, appName, createFolders)` - Initialize logging system
- `log_info(msg)` - Log info messages
- `log_debug(msg)` - Log debug messages  
- `log_error(msg)` - Log error messages
- `get_current_log_path()` - Get current log file path
- `rotate_log()` - Force log rotation

## Directory Structure

```
logs/
├── server/
│   ├── 2024-01-15.log
│   ├── 2024-01-16.log
│   └── ...
└── client/
    ├── 2024-01-15.log
    ├── 2024-01-16.log
    └── ...
```

## Usage

### Server Logging

```cpp
// Initialize server logging
init_logging("logs/server", "server", true);

// Log messages
log("Server started successfully", LogLevel::INFO);
log_debug("Configuration loaded");
log_error("Connection failed");
```

### Client Logging

```cpp
// Initialize client logging
init_logging("logs/client", "client", true);

// Log messages
log("Client connected", LogLevel::INFO);
log_debug("Subscription created");
log_error("Authentication failed");
```

## Command Line Options

### Server
```bash
# Normal mode (no file logging, silent operation)
./server

# Debug mode (logs to both file and console)
./server --debug
```

### Client
```bash
# Normal mode (no file logging, silent operation)
./client

# Debug mode (logs to both file and console)
./client --debug
```

## Log File Format

Each log entry includes:
- Timestamp: `[2024-01-15 14:30:25]`
- Log Level: `[INFO]`, `[DEBUG]`, or `[ERROR]`
- Message: The actual log message

Example:
```
[2024-01-15 14:30:25] [INFO] Server logging initialized with day-wise log files
[2024-01-15 14:30:26] [DEBUG] Setting up server configuration
[2024-01-15 14:30:27] [INFO] Security policies configured successfully
```

## Automatic Features

1. **Folder Creation**: Log folders are created automatically if they don't exist
2. **Date-based Rotation**: New log files are created automatically each day
3. **File Management**: Old log files are preserved for historical analysis
4. **Thread Safety**: All logging operations are thread-safe

## Benefits

- **Separation**: Server and client logs are completely independent
- **Organization**: Day-wise files make it easy to find specific logs
- **Maintenance**: Automatic rotation reduces manual log management
- **Debugging**: Debug mode provides real-time console output
- **Production Ready**: ERROR messages always logged for troubleshooting
- **Flexible**: Normal mode silent, debug mode verbose

## Migration from Old System

The new system maintains backward compatibility:
- `set_log_file()` still works (legacy function)
- Existing log calls continue to work
- Gradual migration to new functions is supported

## Troubleshooting

### Log Files Not Created
- Check if the application has write permissions to the logs directory
- Verify that the `init_logging()` function is called early in the application

### Debug Mode Not Working
- Ensure `--debug` flag is passed as the first argument
- Check if `g_debug` variable is properly declared

### Permission Issues
- On Windows, run as Administrator if needed
- On Linux, check folder permissions and ownership
