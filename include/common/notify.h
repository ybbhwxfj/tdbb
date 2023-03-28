#pragma once
#include <condition_variable>
#include <mutex>

class notify {
private:
  std::mutex cond_mutex_;
  std::condition_variable cond_var_;
  bool done_;

public:
  notify() : done_(false) {}

  void wait() {
    std::unique_lock l(cond_mutex_);
    cond_var_.wait(l, [this]() { return done_; });
  }

  void notify_all() {
    std::unique_lock l(cond_mutex_);
    done_ = true;
    cond_var_.notify_all();
  }
};