#pragma once

#include "common/db_type.h"
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>

const uint32_t TEST_WAN_LATENCY_MS = 50;
const float_t TEST_CACHED_TUPLE_PERCENTAGE = 0.5;
const uint64_t NUM_ORDER_MAX = 100000000;

const uint64_t MESSAGE_BUFFER_SIZE = 8192;

const uint32_t CONNECTIONS_PER_PEER = 10;
const uint32_t NUM_TERMINAL = 10;
const uint32_t NUM_TRANSACTION = 1000000;

const uint32_t NUM_ITEM = 10000;
const uint32_t NUM_WAREHOUSE = 20;
const uint32_t NUM_ORDER_INITIALIZE = 1000;
const uint32_t NUM_DISTRICT_PER_WAREHOUSE = 10;
const uint32_t NUM_CUSTOMER_PER_DISTRICT = 100;
const uint32_t NUM_MAX_ORDER_LINE = 15;
const float PERCENT_NON_EXIST_ITEM = 0.01;

const uint32_t RAFT_LEADER_ELECTION_TICK_MILLI_SECONDS = 400;
const uint32_t RAFT_FOLLOW_TICK_NUM = 30;

const uint32_t CALVIN_EPOCH_MILLISECOND = 100;

const uint32_t THREADS_ASYNC_CONTEXT = 4;
const uint32_t THREADS_CC = 4;
const uint32_t THREADS_REPLICATION = 1;
const uint32_t THREADS_IO = 20;
const uint32_t TPM_CAL_NUM = 100;

const float PERCENT_REMOTE = 1.0;
const float PERCENT_HOT_ROW = 0.1;
const uint64_t HOT_ROW_NUM = NUM_ITEM*0.001;
const uint64_t APPEND_LOG_ENTRIES_BATCH_MIN = 1;

const uint64_t APPEND_LOG_ENTRIES_BATCH_MAX = 32;

const std::chrono::steady_clock::time_point
    EPOCH_TIME_STEADY_CLOCK(std::chrono::steady_clock::now());

const uint64_t TCP_CONNECT_TIMEOUT_MILLIS = 1000;

const uint64_t CCB_REGISTER_TIMEOUT_MILLIS = 2000;
const uint64_t DSB_REGISTER_TIMEOUT_MILLIS = 2000;
const uint64_t RLB_REPORT_MILLIS = 2000;

const uint32_t DEADLOCK_DETECTION_TIMEOUT_MILLIS = 1000;
const uint64_t LOCK_WAIT_TIMEOUT_MILLIS = 800;
const bool DEADLOCK_DETECTION = false;
const uint64_t TX_TIMEOUT_MILLIS = 40000;

const bool DIST_TX_PERCENTAGE = false;

const float PERCENTAGE_READ_ONLY = 0.1;
const uint32_t READ_ONLY_ROWS = 20;
const bool ADDITIONAL_READ_ONLY_TERMINAL = false;
