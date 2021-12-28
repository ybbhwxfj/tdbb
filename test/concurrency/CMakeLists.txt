set(test_lock_mgr test_lock_mgr)
add_executable(
        ${test_lock_mgr}
        lock_mgr_test.cpp)
target_link_libraries(${test_lock_mgr}
        concurrency
        proto
        network
        common
        pthread
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
add_test(NAME ${test_lock_mgr} COMMAND ${test_lock_mgr})


