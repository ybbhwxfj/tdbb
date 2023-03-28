#pragma once

#include "common/unused.h"
#include "common/variable.h"
#include <boost/date_time.hpp>

inline uint64_t system_clock_ms_since_epoch() {
  auto now = std::chrono::high_resolution_clock::now();
  auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch());
  return dur.count();
}

inline uint64_t steady_clock_ms_since_epoch() {
  auto now = std::chrono::steady_clock::now();
  std::chrono::duration<double_t> s = (now - EPOCH_TIME_STEADY_CLOCK);
  return uint64_t(s.count()*1000.0);
}

inline double_t to_microseconds(std::chrono::nanoseconds ns) {
  return double_t(ns.count()/1000.0);
}

inline double_t to_milliseconds(std::chrono::nanoseconds ns) {
  return double_t(ns.count()/1000000.0);
}

inline double_t to_seconds(std::chrono::nanoseconds ns) {
  return double_t(ns.count()/1000000000.0);
}