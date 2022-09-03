#pragma once

#include <boost/asio.hpp>
#include "common/ptr.hpp"

#ifdef ENABLE_TRACE_NETWORK
#define TEST_NETWORK_TIME
#endif

// #define DEBUG_SEND_TIME
#define MS_MAX 10
// #define MULTI_THREAD_EXECUTOR
// #define TX_TRACE
// #define TEST_TRACE_LOCK
