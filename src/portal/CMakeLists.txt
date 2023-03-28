

set(binary_name block-db)
add_executable(
        ${binary_name}
        main_be.cpp
        portal_server.cpp)
# "Boost_XXX_LIBRARY" is case sensitive
target_link_libraries(
        ${binary_name}
        concurrency
        store
        replog
        raft
        network
        proto
        common
        pthread
        ${STORAGE_LIBS}
        ${PROTOBUF_LIBRARY}
        ${Boost_THREAD_LIBRARY}
        ${Boost_PROGRAM_OPTIONS_LIBRARY}
        ${Boost_LOG_LIBRARY}
        ${Boost_SERIALIZATION_LIBRARY}
        ${Boost_JSON_LIBRARY}
)

add_library(
        portal
        portal_server.cpp
        portal_client.cpp
        block_client.cpp
        workload.cpp
)

add_executable(
        block-client
        main_fe.cpp
        portal_client.cpp
        block_client.cpp
        workload.cpp)

target_link_libraries(
        block-client
        network
        common
        proto
        pthread
        tkrzw
        ${PROTOBUF_LIBRARY}
        ${Boost_THREAD_LIBRARY}
        ${Boost_PROGRAM_OPTIONS_LIBRARY}
        ${Boost_LOG_LIBRARY}
        ${Boost_SERIALIZATION_LIBRARY}
        ${Boost_JSON_LIBRARY}
)
