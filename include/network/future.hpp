#pragma once

#include "common/ptr.hpp"
#include "common/result.hpp"
#include <condition_variable>
#include <memory>
#include <mutex>

template<typename R>
class future {
 private:
  struct context {
    context() : done_(false) {}

    std::mutex mtx_;
    std::condition_variable cv_;
    R result_;
    bool done_;
    berror ec_;
  };

  ptr<context> context_;

 public:
  future() : context_(ptr<context>(new context())) {}

  void notify_fail(berror e) {
    std::unique_lock<std::mutex> lk(context_->mtx_);
    context_->done_ = true;
    context_->ec_ = e;
    context_->cv_.notify_all();
  }

  void notify_ok(R r) {
    context_->result_ = r;
    std::unique_lock<std::mutex> lk(context_->mtx_);
    context_->done_ = true;
    context_->cv_.notify_all();
  }

  result<R> get() {
    std::unique_lock<std::mutex> lk(context_->mtx_);
    context_->cv_.wait(lk, [this] { return this->context_->done_; });
    if (context_->ec_ == EC::EC_OK) {
      return context_->result_;
    } else {
      return context_->ec_;
    }
  }
};
