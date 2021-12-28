
add_library(test_common
        gen_config.cpp
        )

add_executable(
        test_config
        config_test.cpp
)

target_link_libraries(test_config
        common
        test_common
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
        ${Boost_TEST_EXEC_MONITOR_LIBRARY}
        ${Boost_LOG_LIBRARY}
        ${Boost_JSON_LIBRARY}
        )


add_executable(test_enum2str
        enum2string_test.cpp)
target_link_libraries(test_enum2str
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
        ${Boost_TEST_EXEC_MONITOR_LIBRARY}
        ${Boost_LOG_LIBRARY}
        ${Boost_JSON_LIBRARY}
        )
add_test(NAME test_enum2str COMMAND test_enum2str)

add_executable(
        test_wait_graph
        wait_graph_test.cpp)
target_link_libraries(test_wait_graph
        common
        proto
        ${PROTOBUF_LIBRARY}
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
        ${Boost_TEST_EXEC_MONITOR_LIBRARY}
        ${Boost_LOG_LIBRARY}
        ${Boost_JSON_LIBRARY}
        )
add_test(NAME test_wait_graph COMMAND test_wait_graph)