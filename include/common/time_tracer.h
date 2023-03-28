#pragma once

#include <boost/stacktrace.hpp>
#include <chrono>

class time_tracer {
  std::chrono::steady_clock::time_point begin_;
  std::chrono::nanoseconds duration_;
  bool started_;

public:
  void begin_ts(std::chrono::steady_clock::time_point begin) {
    BOOST_ASSERT(not started_);
    started_ = true;
    begin_ = begin;
  }

  void end_ts(std::chrono::steady_clock::time_point end) {
    BOOST_ASSERT(started_);
    auto dur = end - begin_;
    // LOG(info) << dur.total_milliseconds();
    duration_ += dur;
    started_ = false;
  }

  void begin() {
    //BOOST_ASSERT(not started_);
    started_ = true;
    begin_ = std::chrono::steady_clock::now();
  }

  void end() {
    BOOST_ASSERT(started_);
    auto end = std::chrono::steady_clock::now();
    auto dur = end - begin_;
    // LOG(info) << dur.total_milliseconds();
    duration_ += dur;
    started_ = false;
  }

  std::chrono::nanoseconds duration() const { return duration_; }

  uint64_t nanoseconds() const { return uint64_t(duration_.count()); }

  uint64_t microseconds() const { return uint64_t(duration_.count()/1000.0); }

  uint64_t milliseconds() const {
    return uint64_t(duration_.count()/1000000.0);
  }
  void reset() {
    started_ = false;
    duration_ = std::chrono::nanoseconds(0);
  }

  time_tracer() {
    started_ = false;
    duration_ = std::chrono::nanoseconds(0);
  }
};