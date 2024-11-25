cmake_minimum_required(VERSION 3.16)

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_VERBOSE_MAKEFILE ON)

add_subdirectory(proto) # build proto first
add_subdirectory(access)
add_subdirectory(common)
add_subdirectory(concurrency)
add_subdirectory(network)
add_subdirectory(raft)
add_subdirectory(replog)
add_subdirectory(store)
add_subdirectory(portal)
