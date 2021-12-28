
add_executable(
        test_raft
        raft_test.cpp
        raft_node.cpp
        raft_test_context.cpp
        raft_test_general.cpp
        raft_test_log_gen.cpp
        raft_test_restart.cpp
        raft_test_startup.cpp
)

target_link_libraries(test_raft
        raft
        network
        pthread
        boost_thread
        common
        proto
        test_common
        ${Boost_LOG_LIBRARY}
        ${PROTOBUF_LIBRARY}
        ${Boost_JSON_LIBRARY}
        ${Boost_SERIALIZATION_LIBRARY}
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
        ${Boost_TEST_EXEC_MONITOR_LIBRARY}
        )

add_test(NAME test_raft COMMAND test_raft)