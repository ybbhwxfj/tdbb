

set(binary_name block-db)
add_executable(
        ${binary_name}
        block-db.cpp
        portal.cpp)
# "Boost_XXX_LIBRARY" is case sensitive
target_link_libraries(
        ${binary_name}
        concurrency
        store
        replog
        raft
        network
        proto
        panel
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
        portal.cpp
        portal_client.cpp
        workload.cpp
)

add_executable(
        block-client
        block-client.cpp
        portal_client.cpp
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
