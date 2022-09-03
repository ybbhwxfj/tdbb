cmake_minimum_required(VERSION 3.0)
project(TPCC-MYSQL)

set(BLOCK_DB_DIR ${CMAKE_SOURCE_DIR}/../..)

include_directories(/usr/include/mysql)
include_directories(${BLOCK_DB_DIR}/third_party/include)
link_directories(${BLOCK_DB_DIR}/third_party/lib)
set(EXECUTABLE_OUTPUT_PATH ${BLOCK_DB_DIR}/bin)

add_library(tpcc_mysql
        delivery.c
        driver.c
        neword.c
        ordstat.c
        payment.c
        rthist.c
        sb_percentile.c
        sequence.c
        slev.c
        spt_proc.c
        support.c
        tpc.c
        )

add_executable(tpcc_mysql_load load.c)
add_executable(tpcc_mysql_bench main.c)

target_link_libraries(tpcc_mysql_load
        tpcc_mysql
        mysqlclient
        pthread
        m)

target_link_libraries(tpcc_mysql_bench
        tpcc_mysql
        mysqlclient
        pthread
        m)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")