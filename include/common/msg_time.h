#pragma once

#include "common/logger.hpp"
#include "common/message.h"
#include <boost/date_time.hpp>

struct total_time {
  boost::posix_time::time_duration duration2;
  uint64_t count;

  total_time() : count(0) {}
};

class msg_time {
private:
  std::unordered_map<message_type, total_time> total_message_;
  total_time dispatch_time_;
  std::string name_;

public:
  msg_time() {}
  msg_time(const std::string &name) : name_(name) {}

  void set_name(const std::string &name) { name_ = name; }

  void add_dispatch(boost::posix_time::time_duration duration) {
    dispatch_time_.count++;
    dispatch_time_.duration2 += duration;
  }

  void add(message_type type, boost::posix_time::time_duration duration2) {
    auto iter = total_message_.find(type);
    if (iter!=total_message_.end()) {
      iter->second.duration2 += duration2;
      iter->second.count += 1;
      if (iter->second.count%100000==99) {
        LOG(info) << name_ << " send-receive message time "
                  << enum2str(iter->first) << " "
                  << iter->second.duration2.total_milliseconds()/
                      iter->second.count
                  << " ms";
      }
    } else {
      total_time tt;
      tt.duration2 = duration2;
      tt.count = 1;
      total_message_.insert(std::make_pair(type, tt));
    }
  }

  void print() {
    for (auto t : total_message_) {
      if (t.second.count > 0) {
        LOG(info) << name_ << " send-receive time " << enum2str(t.first) << " "
                  << t.second.duration2.total_milliseconds()/t.second.count
                  << " ms";
      }
    }
    if (dispatch_time_.count > 0) {
      LOG(info) << name_ << " dispatch time :"
                << dispatch_time_.duration2.total_milliseconds()/
                    dispatch_time_.count
                << " ms";
    }
  }
};
