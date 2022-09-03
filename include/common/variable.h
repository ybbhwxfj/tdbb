#pragma once

#include <cstdlib>
#include <cstdint>
#include <cmath>
#include <chrono>
#include "common/db_type.h"

const uint32_t TEST_WAN_LATENCY_MS = 50;
const float_t TEST_CACHED_TUPLE_PERCENTAGE = 0.5;
const uint64_t NUM_ORDER_MAX = 100000000;

const uint64_t MESSAGE_BUFFER_SIZE = 8192;

const uint32_t CONNECTIONS_PER_PEER = 2;
const uint32_t NUM_TERMINAL = 100;
const uint32_t NUM_TRANSACTION = 100000;

const uint32_t NUM_ITEM = 100000;
const uint32_t NUM_WAREHOUSE = 40;
const uint32_t NUM_ORDER_INITIALIZE = 1000;
const uint32_t NUM_DISTRICT_PER_WAREHOUSE = 10;
const uint32_t NUM_CUSTOMER_PER_DISTRICT = 1000;
const uint32_t NUM_MAX_ORDER_LINE = 15;
const float PERCENT_NON_EXIST_ITEM = 0.01;

const uint32_t RAFT_TICK_MILLI_SECONDS = 100;
const uint32_t RAFT_FOLLOW_TICK_NUM = 10;

const uint32_t CALVIN_EPOCH_MILLISECOND = 50;

const uint32_t THREADS_ASYNC_CONTEXT = 8;
const uint32_t THREADS_IO = 20;
const uint32_t TPM_CAL_NUM = 100;

const float PERCENT_DISTRIBUTED = 0.10;
const float PERCENT_HOT_ROW = 0.01;
const uint64_t HOT_ROW_NUM = NUM_ITEM * 0.01;
const uint64_t APPEND_LOG_ENTRIES_BATCH_MIN = 1;
const uint64_t APPEND_LOG_ENTRIES_BATCH_MAX = 100;

const uint64_t DEADLOCK_DETECT_TICK_MILLIS = 200;
const uint64_t DEADLOCK_DETECT_WAIT_NEXT_TIMEOUT_MILLIS = 500;

const uint64_t STATUS_REPORT_TIMEOUT_MILLIS = 10000;

const std::chrono::steady_clock::time_point EPOCH_TIME(std::chrono::steady_clock::now());
