#pragma once

#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include "common/id.h"

class barrier_net {
private:
  uint32_t node_num_;
  std::condition_variable var_;
  std::mutex mutex_;
  std::unordered_map<node_id_t, std::string> set_;
public:
  barrier_net(uint32_t node_num) : node_num_(node_num) {}

  void add_node_id(node_id_t id, std::string payload) {
    LOG(trace) << "add node " << id;
    std::unique_lock l(mutex_);
    if (!set_.contains(id)) {
      set_.insert(std::make_pair(id, payload));
      if (node_num_ == set_.size()) {
        LOG(trace) << "notify all " << id;
        var_.notify_all();
      }
    }
  }

  void wait() {
    std::unique_lock l (mutex_);
    auto fn = [this]() { return set_.size() == node_num_; };
    var_.wait(l, fn);
  }

  std::unordered_map<node_id_t, std::string> payload() {
    std::unique_lock l (mutex_);
    std::unordered_map<node_id_t, std::string> set;
    set = set_;
    return set;
  }
};

