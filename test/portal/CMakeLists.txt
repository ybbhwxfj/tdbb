
#value can be found in include/common/db_type.h


set(test_binary test_db)
add_executable(
        ${test_binary}
        history.cpp
        portal_test.cpp)
target_link_libraries(${test_binary}
        portal
        concurrency
        store
        replog
        raft
        common
        proto
        panel
        network
        pthread
        test_common
        ${STORAGE_LIBS}
        ${Boost_PROGRAM_OPTIONS_LIBRARY}
        ${PROTOBUF_LIBRARY}
        ${Boost_JSON_LIBRARY}
        ${Boost_UNIT_TEST_FRAMEWORK_LIBRARY}
        ${Boost_TEST_EXEC_MONITOR_LIBRARY}
        ${Boost_LOG_LIBRARY}
        ${Boost_SERIALIZATION_LIBRARY}
        ${Boost_FILESYSTEM_LIBRARY}
        ${Boost_SYSTEM_LIBRARY}
        ${Boost_THREAD_LIBRARY}
        )
add_test(NAME ${test_binary} COMMAND ${test_binary})


