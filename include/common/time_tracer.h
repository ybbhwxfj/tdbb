#pragma once

#include <boost/date_time.hpp>
class time_tracer {
  boost::posix_time::ptime begin_;
  boost::posix_time::time_duration duration_;
public:
  void begin() {
    begin_ = boost::posix_time::microsec_clock::local_time();
  }

  void end() {
    boost::posix_time::ptime end = boost::posix_time::microsec_clock::local_time();
    auto dur = end - begin_;
    //BOOST_LOG_TRIVIAL(info) << dur.total_milliseconds();
    duration_ += dur;
  }

  boost::posix_time::time_duration duration() const {
    return duration_;
  }

  void reset() {
    duration_ = boost::posix_time::microseconds(0);
  }
  time_tracer() {
    duration_ = boost::posix_time::microseconds(0);
  }
};