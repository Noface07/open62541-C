cmake_minimum_required(VERSION 3.13)
project(open62541-examples C CXX)

if(${CMAKE_VERSION} VERSION_LESS 3.12)
    cmake_policy(VERSION ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION})
endif()

# The examples folder can be built standalone.
# First install open62541 system-wide using `make install` or vcpkg.
# Then build examples anywhere by pointing to the source directory.

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    # Standalone build: Find installed open62541 package
    find_package(open62541 REQUIRED)

    # Empty dummy function for grouping sources (used in open62541 build only)
    function(assign_source_group)
    endfunction()
endif()







# === Dependency resolution (works on both vcpkg-Windows and native Linux) ===

# Silence CMake policy warning for Boost CMP0167 (find_package Boost CONFIG)
if(POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif()

# Always ask CMake to find the CONFIG versions first (vcpkg provides them)
find_package(Boost CONFIG QUIET COMPONENTS system)
if(NOT Boost_FOUND)
    # Fallback to module mode (works with classic CMake + vcpkg triplet that lacks config files)
    find_package(Boost REQUIRED COMPONENTS system)
endif()
## ---- SQLite3 (vcpkg or system) ----
# Try CONFIG first (vcpkg provides either unofficial::sqlite3 or SQLite::SQLite3)
set(SQLITE3_TARGET "")
find_package(SQLite3 CONFIG QUIET)
if(SQLite3_FOUND)
    if(TARGET SQLite::SQLite3)
        set(SQLITE3_TARGET SQLite::SQLite3)
    elseif(TARGET unofficial::sqlite3)
        set(SQLITE3_TARGET unofficial::sqlite3)
    endif()
endif()

# Fallback to module mode (sets SQLite::SQLite3) for system installs
if(NOT SQLITE3_TARGET)
    find_package(SQLite3 REQUIRED) # module
    set(SQLITE3_TARGET SQLite::SQLite3)
endif()

message(STATUS "Using SQLite target: ${SQLITE3_TARGET}")
find_package(nlohmann_json 3.2.0 REQUIRED CONFIG)
# --- HARD OVERRIDE FOR ZLIB ---
message(STATUS "FORCING ZLIB paths for vcpkg x64-windows...")

set(ZLIB_FOUND TRUE)
set(ZLIB_INCLUDE_DIR "D:/VCPKG/vcpkg/installed/x64-windows/include")
set(ZLIB_LIBRARY "D:/VCPKG/vcpkg/installed/x64-windows/lib/zlib.lib")

# This creates the logical name "ZLIB::ZLIB" that your other targets expect
if(NOT TARGET ZLIB::ZLIB)
    add_library(ZLIB::ZLIB UNKNOWN IMPORTED)
    set_target_properties(ZLIB::ZLIB PROPERTIES
        IMPORTED_LOCATION "${ZLIB_LIBRARY}"
        INTERFACE_INCLUDE_DIRECTORIES "${ZLIB_INCLUDE_DIR}"
    )
    message(STATUS "ZLIB::ZLIB target created manually.")
endif()
find_package(Threads REQUIRED)

# ---- libpqxx (optional) ----
#set(PQXX_FOUND FALSE)
#set(PQXX_LINK_TARGET "")

# First try Config package (vcpkg installs this on all platforms)
#find_package(pqxx CONFIG QUIET)
#if(pqxx_FOUND)
#    set(PQXX_FOUND TRUE)
#    set(PQXX_LINK_TARGET pqxx::pqxx)
#endif()

# Fallback to PkgConfig on Linux if Config package not found
#if(NOT PQXX_FOUND AND NOT WIN32)
#    find_package(PkgConfig QUIET)
#    if(PkgConfig_FOUND)
#        pkg_check_modules(PQXX QUIET IMPORTED_TARGET pqxx)
#        if(PQXX_FOUND)
#            set(PQXX_LINK_TARGET PkgConfig::PQXX)
#        endif()
#    endif()
#endif()

# Manual fallback (rare; mainly for custom installs)
#if(NOT PQXX_FOUND)
#    find_path(PQXX_INCLUDE_DIR NAMES pqxx/pqxx)
#    find_library(PQXX_LIBRARY NAMES pqxx libpqxx)
#    if(PQXX_INCLUDE_DIR AND PQXX_LIBRARY)
#        add_library(pqxx::pqxx UNKNOWN IMPORTED)
#        set_target_properties(pqxx::pqxx PROPERTIES
#            IMPORTED_LOCATION "${PQXX_LIBRARY}"
#            INTERFACE_INCLUDE_DIRECTORIES "${PQXX_INCLUDE_DIR}")
#        set(PQXX_FOUND TRUE)
#        set(PQXX_LINK_TARGET pqxx::pqxx)
#    endif()
#endif()

#if(PQXX_FOUND)
#    message(STATUS "Found libpqxx (target: ${PQXX_LINK_TARGET})")
#else()
#   message(STATUS "libpqxx not found – 'server_cpp' example will be skipped")
#endif()

# Allow user override or env var for async_mqtt header-only library
set(ASYNC_MQTT_INCLUDE_DIR "$ENV{ASYNC_MQTT_INCLUDE_DIR}" CACHE PATH "Path to async_mqtt include directory")
# If not set, look in typical locations
if(NOT ASYNC_MQTT_INCLUDE_DIR)
    find_path(ASYNC_MQTT_INCLUDE_DIR async_mqtt/all.hpp PATHS
        ${CMAKE_CURRENT_LIST_DIR}/../external/async_mqtt
        $ENV{HOME}/async_mqtt/include
        DOC "Path to async_mqtt include directory")
endif()

# ===================================================================
# END dependency resolution
# ===================================================================











# Use modern include directories scoped to targets when possible.
# But for examples, common.h is in the source directory, so add globally:
include_directories(${CMAKE_CURRENT_LIST_DIR})

# Set output directories for all build types
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin/examples)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_DEBUG ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELEASE ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_RELWITHDEBINFO ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_MINSIZEREL ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

# C++20 standard for C++ examples
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Macro to add example executables with proper properties
macro(add_example EXAMPLE_NAME EXAMPLE_SOURCE)
    add_executable(${EXAMPLE_NAME} ${EXAMPLE_SOURCE} ${PROJECT_SOURCE_DIR}/common.h)
    target_link_libraries(${EXAMPLE_NAME} PRIVATE open62541::open62541)
    assign_source_group(${EXAMPLE_SOURCE})
    set_target_properties(${EXAMPLE_NAME} PROPERTIES
        FOLDER "open62541/examples"
        VS_DEBUGGER_WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/bin"
    )
endmacro()

# ===================================================================
# START OF CORRECTIONS: Define complex C++ executables separately
# ===================================================================

# Define a variable for all the source files shared by your complex examples
set(CLIENT_LIB_SOURCES
    MQTThandler.cpp
    SqliteQueueService.cpp
    fetchAPI.cpp
    Encryption.cpp
    Monitoring.cpp
    RedisClient.cpp
)

# --- Define the 'client' executable ---
add_executable(client client.cpp ${CLIENT_LIB_SOURCES})
target_include_directories(client PRIVATE ${ASYNC_MQTT_INCLUDE_DIR})
target_link_libraries(client PRIVATE
    open62541::open62541
    ${SQLITE3_TARGET}
    Boost::system
    nlohmann_json::nlohmann_json
    Threads::Threads
    ZLIB::ZLIB
)
#if(MSVC)
 #   target_compile_options(client PRIVATE /Zm2000)
#endif()

set_target_properties(client PROPERTIES FOLDER "open62541/examples/Complex")

# --- Define the 'server_cpp' executable ---
#if(PQXX_FOUND)
    add_executable(server_cpp server.cpp ${CLIENT_LIB_SOURCES} AandC.cpp SessionManager.cpp SessionWorker.cpp AccessControl.cpp InstrumentedMutex.cpp)
    target_include_directories(server_cpp PRIVATE ${ASYNC_MQTT_INCLUDE_DIR})
    target_link_libraries(server_cpp PRIVATE
        open62541::open62541
#        ${PQXX_LINK_TARGET}
        ${SQLITE3_TARGET}
        Boost::system
        nlohmann_json::nlohmann_json
        Threads::Threads
        ZLIB::ZLIB
    )   
    set_target_properties(server_cpp PROPERTIES FOLDER "open62541/examples/Complex")
#endif()

# ===================================================================
# END OF CORRECTIONS
# ===================================================================

# --- Define the 'RedisUpdater' utility ---
add_executable(RedisUpdater RedisUpdater.cpp RedisClient.cpp fetchAPI.cpp Encryption.cpp SqliteQueueService.cpp)
target_link_libraries(RedisUpdater PRIVATE
    open62541::open62541
    Boost::system
    ${SQLITE3_TARGET}
    Threads::Threads
    nlohmann_json::nlohmann_json
    ZLIB::ZLIB
)
target_include_directories(RedisUpdater PRIVATE ${ASYNC_MQTT_INCLUDE_DIR}) # For async_mqtt/all.hpp in fetchAPI.cpp
set_target_properties(RedisUpdater PROPERTIES FOLDER "open62541/examples/Utility")

# Add simple C examples using the macro
# NOTE: The original add_example calls for 'client' and 'server_cpp' have been removed.

# add_example(tutorial_datatypes tutorial_datatypes.c)
# add_example(tutorial_server_firststeps tutorial_server_firststeps.c)
# add_example(tutorial_server_variable tutorial_server_variable.c)
# add_example(tutorial_server_datasource tutorial_server_datasource.c)
# add_example(tutorial_server_variabletype tutorial_server_variabletype.c)
# add_example(tutorial_server_object tutorial_server_object.c)
# add_example(tutorial_server_reverseconnect tutorial_server_reverseconnect.c)

if(UA_ENABLE_SUBSCRIPTIONS)
    # add_example(tutorial_server_monitoreditems tutorial_server_monitoreditems.c)
endif()

if(UA_ENABLE_METHODCALLS)
    # add_example(tutorial_server_method tutorial_server_method.c)
    if(UA_MULTITHREADING GREATER_EQUAL 100)
        # add_example(tutorial_server_method_async tutorial_server_method_async.c)
    endif()
endif()

if(UA_ENABLE_SUBSCRIPTIONS_ALARMS_CONDITIONS)
    # add_example(tutorial_server_alarms_conditions tutorial_server_alarms_conditions.c)
endif()

# add_example(tutorial_client_firststeps tutorial_client_firststeps.c)

if(UA_ENABLE_SUBSCRIPTIONS_EVENTS)
    # add_example(tutorial_client_events tutorial_client_events.c)
    # add_example(tutorial_server_events tutorial_server_events.c)
endif()

# Client examples
# add_example(client_connect client_connect.c)
# add_example(client_async client_async.c)
# add_example(client_connect_loop client_connect_loop.c)

if(UA_ENABLE_HISTORIZING)
    # add_example(client_historical client_historical.c)
endif()

if(UA_ENABLE_METHODCALLS AND UA_MULTITHREADING GREATER_EQUAL 100)
    # add_example(client_method_async client_method_async.c)
endif()

if(UA_ENABLE_SUBSCRIPTIONS)
    # add_example(client_subscription_loop client_subscription_loop.c)
endif()

# Feature examples
# add_example(ci_server ci_server.c)
# add_example(server_settimestamp server_settimestamp.c)
# add_example(server_mainloop server_mainloop.c)
# add_example(server_instantiation server_instantiation.c)
# add_example(server_repeated_job server_repeated_job.c)
# add_example(server_inheritance server_inheritance.c)
# add_example(server_loglevel server_loglevel.c)
# add_example(custom_datatype_client custom_datatype/client_types_custom.c)
# add_example(custom_datatype_server custom_datatype/server_types_custom.c)

if(UA_ENABLE_SUBSCRIPTIONS_EVENTS)
    # add_example(server_events_random events/server_random_events.c)
    # add_example(client_event_filter events/client_eventfilter.c)
    if(UA_ENABLE_PARSING)
        # add_example(client_event_filter_queries events/client_filter_queries.c)
    endif()
endif()

if(UA_ENABLE_JSON_ENCODING)
    # add_example(server_json_config server_json_config.c)
endif()

if(UA_ENABLE_HISTORIZING)
    # add_example(tutorial_server_historicaldata tutorial_server_historicaldata.c)
    # add_example(tutorial_server_historicaldata_circular tutorial_server_historicaldata_circular.c)
endif()

# Encryption examples
if(UA_ENABLE_ENCRYPTION OR
   UA_ENABLE_ENCRYPTION STREQUAL "MBEDTLS" OR
   UA_ENABLE_ENCRYPTION STREQUAL "OPENSSL")
    # add_example(server_encryption encryption/server_encryption.c)
    # add_example(server_encryption_filestore encryption/server_encryption_filestore.c)
    # add_example(client_encryption encryption/client_encryption.c)

    # target_include_directories(server_encryption PRIVATE "${PROJECT_SOURCE_DIR}/examples")
    # target_include_directories(client_encryption PRIVATE "${PROJECT_SOURCE_DIR}/examples")

    if(UA_ENABLE_TPM2_KEYSTORE)
        # add_example(server_encryption_tpm_keystore encryption/server_encryption_tpm_keystore.c)
        # add_example(client_encryption_tpm_keystore encryption/client_encryption_tpm_keystore.c)
        # target_link_libraries(server_encryption_tpm_keystore PRIVATE tpm2_pkcs11 ssl crypto)
        # target_link_libraries(client_encryption_tpm_keystore PRIVATE tpm2_pkcs11 ssl crypto)
    endif()
endif()

# Access control examples
if(UA_ENABLE_NODEMANAGEMENT)
    # add_example(access_control_server access_control/server_access_control.c)
    # add_example(access_control_client access_control/client_access_control.c)
    if(UA_ENABLE_ENCRYPTION OR
       UA_ENABLE_ENCRYPTION STREQUAL "MBEDTLS" OR
       UA_ENABLE_ENCRYPTION STREQUAL "OPENSSL")
        # add_example(access_control_client_encrypt access_control/client_access_control_encrypt.c)
    endif()
endif()

# Discovery examples
if(UA_ENABLE_DISCOVERY_MULTICAST)
    # add_example(discovery_server_lds server_lds.c)
    # add_example(discovery_server_register server_register.c)
    # add_example(discovery_client_find_servers client_find_servers.c)
    # add_example(discovery_server_multicast server_multicast.c)
endif()

# add_subdirectory(nodeset)

# PubSub examples
if(UA_ENABLE_PUBSUB)
    # add_example(tutorial_pubsub_connection pubsub/tutorial_pubsub_connection.c)
    # add_example(tutorial_pubsub_publish pubsub/tutorial_pubsub_publish.c)
    # add_example(tutorial_pubsub_subscribe pubsub/tutorial_pubsub_subscribe.c)
    # add_example(server_pubsub_publish_on_demand pubsub/server_pubsub_publisher_on_demand.c)
    # add_example(server_pubsub_publisher_iop pubsub/server_pubsub_publisher_iop.c)
    # add_example(pubsub_subscribe_standalone_dataset pubsub/pubsub_subscribe_standalone_dataset.c)

    # add_example(server_pubsub_publish_rt_offsets pubsub_realtime/server_pubsub_publish_rt_offsets.c)
    # add_example(server_pubsub_subscribe_rt_offsets pubsub_realtime/server_pubsub_subscribe_rt_offsets.c)

    if(UA_ARCHITECTURE_POSIX AND NOT APPLE)
        # add_example(server_pubsub_publish_rt_state_machine pubsub_realtime/server_pubsub_publish_rt_state_machine.c)
        # target_link_libraries(server_pubsub_publish_rt_state_machine PRIVATE rt)
        # add_example(server_pubsub_subscribe_rt_state_machine pubsub_realtime/server_pubsub_subscribe_rt_state_machine.c)
        # target_link_libraries(server_pubsub_subscribe_rt_state_machine PRIVATE rt)
    endif()

    if(UA_ENABLE_ENCRYPTION_MBEDTLS)
        # add_example(pubsub_publish_encrypted pubsub/pubsub_publish_encrypted.c)
        # add_example(pubsub_subscribe_encrypted pubsub/pubsub_subscribe_encrypted.c)

        if(UA_ENABLE_TPM2_SECURITY)
            # add_example(pubsub_publish_encrypted_tpm pubsub/pubsub_publish_encrypted_tpm.c)
            # add_example(pubsub_subscribe_encrypted_tpm pubsub/pubsub_subscribe_encrypted_tpm.c)
        endif()

        if(UA_ENABLE_TPM2_KEYSTORE)
            # add_example(pubsub_publish_encrypted_tpm_keystore pubsub/pubsub_publish_encrypted_tpm_keystore.c)
            # add_example(pubsub_subscribe_encrypted_tpm_keystore pubsub/pubsub_subscribe_encrypted_tpm_keystore.c)
            # target_link_libraries(pubsub_publish_encrypted_tpm_keystore PRIVATE tpm2_pkcs11 ssl crypto)
            # target_link_libraries(pubsub_subscribe_encrypted_tpm_keystore PRIVATE tpm2_pkcs11 ssl crypto)
        endif()

        if(UA_ENABLE_PUBSUB_SKS)
            # add_subdirectory(pubsub/sks)
        endif()
    endif()

    if(UA_ENABLE_MQTT)
        # add_example(tutorial_pubsub_mqtt_publish pubsub/tutorial_pubsub_mqtt_publish.c)
        # add_example(tutorial_pubsub_mqtt_subscribe pubsub/tutorial_pubsub_mqtt_subscribe.c)
    endif()

    if(UA_ENABLE_PUBSUB_FILE_CONFIG)
        # add_example(server_pubsub_file_configuration pubsub/server_pubsub_file_configuration.c)
    endif()

    # add_example(server_pubsub_subscribe_custom_monitoring pubsub/server_pubsub_subscribe_custom_monitoring.c)
endif()

# Nodeset loader examples
if(UA_ENABLE_NODESETLOADER)
    # add_subdirectory(nodeset_loader)
endif()

if(UA_ENABLE_NODESET_INJECTOR)
    # set(UA_NODESETINJECTOR_SOURCE_FILES ${UA_NODESETINJECTOR_SOURCE_FILES} PARENT_SCOPE)
endif()
