#pragma once

#include "common/logger.hpp"
#include "common/ptr.hpp"
#include <boost/asio.hpp>

//#define ENABLE_TEST_HANDLE_TIME
//#define ENABLE_TRACE_NETWORK

#ifdef ENABLE_TRACE_NETWORK
#define TEST_NETWORK_TIME
#endif

#ifdef ENABLE_TEST_HANDLE_TIME
#define TEST_HANDLE_TIME
#endif

#define TEST_HANDLE_MAX_MS 50

#define TEST_NETWORK_MS_MAX 50
// #define MULTI_THREAD_EXECUTOR
// #define TX_TRACE
// #define TEST_TRACE_LOCK

// #define DEBUG_NETWORK_SEND_RECV
//#define TEST_APPEND_TIME
#define APPEND_MS_MAX 50

#define DIST_DEADLOCK
// #define REPLICATION_MULTIPLE_CHANNEL
