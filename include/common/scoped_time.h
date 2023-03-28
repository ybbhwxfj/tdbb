#pragma once

#include "common/define.h"
#include "common/logger.hpp"
#include "common/utils.h"
#include <boost/format.hpp>

#ifdef TEST_HANDLE_TIME
#define SCOPED_TIME(message, time)                                             \
  scoped_time((boost::format("%d %s") % __LINE__ % (message)).str(), (time))
#else
#define SCOPED_TIME(message, time) scoped_time(message, time)
#endif
class scoped_time {
#ifdef TEST_HANDLE_TIME
  uint64_t max_ms_;
  uint64_t begin_;
  std::string message_;
#endif
public:
  scoped_time(const std::string &msg) {
    POSSIBLE_UNUSED(msg);
#ifdef TEST_HANDLE_TIME
    ;
    max_ms_ = TEST_HANDLE_MAX_MS;
    begin_ = steady_clock_ms_since_epoch();
    message_ = msg;
#endif
  }
#ifndef TEST_HANDLE_TIME
  scoped_time(const std::string &, uint64_t) {
#else
    scoped_time(const std::string &_msg, uint64_t _ms) {
      max_ms_ = _ms;
      begin_ = steady_clock_ms_since_epoch();
      message_ = _msg;
#endif
  }

  ~scoped_time() {
#ifdef TEST_HANDLE_TIME
    auto end = steady_clock_ms_since_epoch();
    if (end - begin_ > max_ms_) {
      LOG(warning) << message_ << " , time:" << end - begin_ << "ms";
    }
#endif // TEST_HANDLE_TIME
  }

  uint64_t begin_ts() const {
#ifdef TEST_HANDLE_TIME
    return begin_;
#else
    return 0;
#endif
  }
};