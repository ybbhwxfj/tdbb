#pragma once

#include <boost/date_time.hpp>
#include "common/variable.h"

inline uint64_t ms_since_epoch() {
  auto now = std::chrono::steady_clock::now();
  std::chrono::duration<double_t> s = (now - EPOCH_TIME);
  return uint64_t(s.count() * 1000.0);
}

inline double_t to_microseconds(std::chrono::nanoseconds ns) {
  return double_t(ns.count() / 1000.0);
}

inline double_t to_milliseconds(std::chrono::nanoseconds ns) {
  return double_t(ns.count() / 1000000.0);
}

inline double_t to_seconds(std::chrono::nanoseconds ns) {
  return double_t(ns.count() / 1000000000.0);
}