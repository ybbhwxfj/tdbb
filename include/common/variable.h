#pragma once

#include <cstdlib>
#include <cstdint>
#include "common/db_type.h"

const uint64_t MESSAGE_BUFFER_SIZE = 40960;

const uint32_t NUM_TERMINAL = 100;
const uint32_t NUM_NEW_ORDER_TX = 1000;

//const uint32_t RAND_CCID = 2515;
//const uint32_t RAND_CIID = 15161;
const uint32_t NUM_ITEM = 4000;
const uint32_t NUM_WAREHOUSE = 10;
const uint32_t NUM_ORDER_ITEM = 10;
const uint32_t NUM_ORDER_INITIALIZE = 3000;
const uint32_t NUM_DISTRICT = 10;
const uint32_t NUM_CUSTOMER = 100;
const uint32_t NUM_MAX_ORDER_LINE = 10;
const float PERCENT_NON_EXIST_ITEM = 0.05;

const uint32_t RAFT_TICK_MILISECONDS = 100;
const uint32_t RAFT_FOLLOW_TICK_NUM = 20;

const uint32_t CALVIN_EPOCH_MILLISECOND = 50;

const uint32_t THREAD_NUM = 1;
const uint32_t THREAD_IO = 5;
const uint32_t TPM_CAL_NUM = 50;

const float PERCENT_DISTRIBUTED = 0.05;
const float APPEND_LOG_ENTRIES_BATCH_MIN = 20;