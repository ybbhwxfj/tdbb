#pragma once
#include "common/logger.hpp"
#include "common/ptr.hpp"
#include "common/set_thread_name.h"
#include <boost/asio.hpp>

class timer : public std::enable_shared_from_this<timer> {
private:
  boost::asio::chrono::steady_clock::duration duration_;
  boost::asio::io_context::strand strand_;
  std::function<void()> callback_;
  enum timer_status {
    IDLE,
    ASYNC_WAIT,
    CLOSE,
  };
  ptr<boost::asio::steady_timer> timer_;
  timer_status status_;

  std::mutex mutex_;
  std::condition_variable cond_; // if stopped
  std::atomic_bool stopped_;

public:
  timer(boost::asio::io_context::strand strand,
        boost::asio::chrono::steady_clock::duration duration,
        std::function<void()> callback

  )
      : duration_(duration), strand_(strand), callback_(callback),
        status_(timer_status::IDLE), stopped_(false) {}

  timer(boost::asio::io_context &ctx,
        boost::asio::chrono::steady_clock::duration duration,
        std::function<void()> callback)
      : duration_(duration), strand_(ctx), callback_(callback),
        status_(timer_status::IDLE) {}

  void async_tick() {
    auto t = shared_from_this();
    boost::asio::post(strand_, [t] { t->strand_async_tick(); });
  }

  void cancel() {
    stopped_.store(true);
    auto t = shared_from_this();
    boost::asio::post(strand_, [t] { t->strand_cancel_timer(); });
  }

  void cancel_and_join() {
    stopped_.store(true);
    cancel_and_join_gut();
  }

  void reset_callback(std::function<void()> callback) {
    auto t = shared_from_this();
    boost::asio::post(strand_,
                      [t, callback] { t->strand_reset_callback(callback); });
  }

private:
  void strand_reset_callback(std::function<void()> callback) {

    if (status_!=timer_status::CLOSE) {
      callback_ = callback;
      strand_async_tick();
      // LOG(info) << s.get() <<  " " << get_thread_name() << " set async wait";
    } else {
      return;
    }
  }
  void strand_async_tick() {

    // LOG(trace) << this << ", status: " << status_ <<  " , timer tick";
    if (status_==timer_status::CLOSE) {
      return;
    } else { // idle or wait
      ptr<boost::asio::steady_timer> timer(
          new boost::asio::steady_timer(strand_.context(), duration_));
      auto s = shared_from_this();
      auto fn_timeout = boost::asio::bind_executor(
          strand_, [s](const boost::system::error_code &ec) {
            if (!ec.failed()) {
              std::function<void()> callback = nullptr;
              {
                if (s->status_==timer_status::IDLE) {
                  s->status_ = timer_status::ASYNC_WAIT;
                  callback = s->callback_;
                  // LOG(info) << s.get() <<  " " << get_thread_name() << " set
                  // async wait";
                } else {
                  return;
                }
              }
              // LOG(info) << s.get() << " " << get_thread_name() << " before
              // callback";
              if (callback) {
                callback();
              }

              // LOG(info) << s.get() << " " << get_thread_name() << " after
              // callback";
              {
                if (s->status_==timer_status::ASYNC_WAIT) {
                  // LOG(info) << s.get() << " " << get_thread_name() << " unset
                  // async wait";
                  s->status_ = timer_status::IDLE;
                  if (s->stopped_.load()) {
                    s->status_ = timer_status::CLOSE;
                    s->cond_.notify_all();
                  }
                } else {
                  // LOG(info) << s.get() << " " << get_thread_name() << " " <<
                  // s->status_ << " async wait";
                  return;
                }
              }
              s->async_tick();
            }
          });
      timer->async_wait(fn_timeout);
      timer_ = timer;
    }
  }

  void strand_cancel_timer() {
    if (timer_) {
      size_t size = timer_->cancel();
      if (size > 0) {
        LOG(trace) << this << ", status: " << status_
                   << " , cancel timer start";
      }

      status_ = timer_status::CLOSE;
    }

    cond_.notify_all();
  }

  void cancel_and_join_gut() {
    cancel();

    std::unique_lock l(mutex_);

    auto now = std::chrono::system_clock::now();
    cond_.wait_until(l, now + std::chrono::seconds(100), [this] {
      if (status_==CLOSE) {
        return true;
      } else if (status_==IDLE) {
        status_ = CLOSE;
        return true;
      } else if (status_==ASYNC_WAIT) {
        // LOG(info) << this << " " << get_thread_name() << " wait on async wait
        // state";
        return false;
      } else {
        status_ = CLOSE;
        return true;
      }
    });

    // LOG(trace) << this << " , cancel timer tick";
  }
};