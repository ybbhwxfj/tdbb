cmake_minimum_required(VERSION 3.0)
project(BLOCK-DB)
set(CMAKE_CXX_STANDARD 20)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # using Clang
    set(CMAKE_CXX_FLAGS "-pthread")
    add_definitions(-DBOOST_ASIO_NO_DEPRECATED)
    add_definitions(-D_LIBCPP_ENABLE_CXX20_REMOVED_TYPE_TRAITS)
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # using GCC
    if (SANITIZE_ADDRESS)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
    endif ()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif ()

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

set(tkrzw tkrzw-1.0.20)
set(rocksdb rocksdb-6.26.1)


set(STORAGE_LIBS
        rocksdb
        tkrzw
        uring
        z
        dl
        lz4
        bz2
        zstd
        snappy
        tbb
        )

include_directories(
        SYSTEM third_party/include
)

link_directories(
        third_party/lib
)


set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
include(FindProtobuf)
set(Boost_USE_STATIC_LIBS ON)
find_package(Protobuf REQUIRED)
find_package(Threads REQUIRED
        pthreads
        )
find_package(Boost REQUIRED
        COMPONENTS
        system
        program_options
        date_time
        unit_test_framework
        test_exec_monitor
        log
        serialization
        thread
        json
        chrono
        )


include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${CMAKE_BINARY_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/test)


if (MSVC)
    # warning level 4 and all warnings as errors
    add_compile_options(/W4 /WX)
else ()
    # lots of warnings and all warnings as errors
    add_compile_options(-Wall -Wextra -pedantic -Werror)
endif ()
# === Compiler options ===
if (ADDRESS_SANITIZER GREATER 0)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fuse-ld=gold")
    message(STATUS "---- ADDRESS SANITIZER IS ON ----")
endif ()

if (THREAD_SANITIZER GREATER 0)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -fuse-ld=gold")
    add_definitions(-DSUPPRESS_TSAN_FALSE_ALARMS=1)
    message(STATUS "---- THREAD SANITIZER IS ON ----")
endif ()

add_subdirectory(src)
add_subdirectory(test)
