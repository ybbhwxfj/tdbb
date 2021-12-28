
add_executable(
        test_network
        network_test.cpp)
target_link_libraries(test_network
        proto
        network
        common
        test_common
        ${PROTOBUF_LIBRARY}
        ${Boost_LOG_LIBRARY}
        ${Boost_SERIALIZATION_LIBRARY}
        ${Boost_FILESYSTEM_LIBRARY}
        ${Boost_JSON_LIBRARY}
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
        ${Boost_TEST_EXEC_MONITOR_LIBRARY}
        )
add_test(NAME test_network COMMAND test_network)