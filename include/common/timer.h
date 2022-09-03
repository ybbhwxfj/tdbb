#pragma once

#include <boost/asio.hpp>
#include "common/set_thread_name.h"
#include "common/ptr.hpp"

class timer : public std::enable_shared_from_this<timer> {
 private:
  boost::asio::chrono::steady_clock::duration duration_;
  std::mutex mutex_;
  std::condition_variable cond_;

  boost::asio::io_context::strand strand_;
  std::function<void()> callback_;
  enum timer_status {
    IDLE,
    ASYNC_WAIT,
    CLOSE,
  };
  ptr<boost::asio::steady_timer> timer_;
  timer_status status_;
 public:
  timer(boost::asio::io_context::strand strand,
        boost::asio::chrono::steady_clock::duration duration,
        std::function<void()> callback

  ) : duration_(duration),
      strand_(strand),
      callback_(callback),
      status_(timer_status::IDLE) {
  }

  timer(
      boost::asio::io_context &ctx,
      boost::asio::chrono::steady_clock::duration duration,
      std::function<void()> callback
  ) : duration_(duration),
      strand_(ctx),
      callback_(callback),
      status_(timer_status::IDLE) {
  }

  void async_tick() {
    std::unique_lock l(mutex_);
    async_tick_gut();
  }

  void cancel() {
    std::unique_lock l(mutex_);
    cancel_timer();
  }

  void cancel_and_join() {
    cancel_and_join_gut();
  }

  void reset_status() {
    std::unique_lock l(mutex_);
    status_ = timer_status::IDLE;
  }

  void reset_callback(std::function<void()> callback) {
    std::unique_lock l(mutex_);
    if (status_ != timer_status::CLOSE) {
      callback_ = callback;
      async_tick_gut();
      // BOOST_LOG_TRIVIAL(info) << s.get() <<  " " << get_thread_name() << " set async wait";
    } else {
      return;
    }
  }
 private:

  void async_tick_gut() {

    // BOOST_LOG_TRIVIAL(trace) << this << ", status: " << status_ <<  " , timer tick";
    if (status_ == timer_status::CLOSE) {
      return;
    } else { // idle or wait
      ptr<boost::asio::steady_timer> timer(
          new boost::asio::steady_timer(strand_.context(), duration_));
      auto s = shared_from_this();
      auto fn_timeout = boost::asio::bind_executor(
          strand_,
          [s](const boost::system::error_code &ec) {
            if (!ec.failed()) {
              std::function<void()> callback = nullptr;
              {
                std::unique_lock l(s->mutex_);
                if (s->status_ == timer_status::IDLE) {
                  s->status_ = timer_status::ASYNC_WAIT;
                  callback = s->callback_;
                  // BOOST_LOG_TRIVIAL(info) << s.get() <<  " " << get_thread_name() << " set async wait";
                } else {
                  return;
                }
              }
              // BOOST_LOG_TRIVIAL(info) << s.get() << " " << get_thread_name() << " before callback";
              if (callback) {
                callback();
              }

              // BOOST_LOG_TRIVIAL(info) << s.get() << " " << get_thread_name() << " after callback";
              {
                std::unique_lock l(s->mutex_);
                if (s->status_ == timer_status::ASYNC_WAIT) {
                  // BOOST_LOG_TRIVIAL(info) << s.get() << " " << get_thread_name() << " unset async wait";
                  s->status_ = timer_status::IDLE;
                  s->cond_.notify_all();
                } else {
                  // BOOST_LOG_TRIVIAL(info) << s.get() << " " << get_thread_name() << " " << s->status_ << " async wait";
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

  void cancel_timer() {
    if (timer_) {
      status_ = timer_status::CLOSE;
      size_t size = timer_->cancel();
      if (size > 0) {
        BOOST_LOG_TRIVIAL(trace) << this << ", status: " << status_ << " , cancel timer tick";
      }
    }
  }

  void cancel_and_join_gut() {
    std::unique_lock l(mutex_);

    cond_.wait(l, [this] {
      if (status_ == CLOSE) {
        return true;
      } else if (status_ == IDLE) {
        status_ = CLOSE;
        cancel_timer();
        return true;
      } else if (status_ == ASYNC_WAIT) {
        // BOOST_LOG_TRIVIAL(info) << this << " " << get_thread_name() << " wait on async wait state";
        return false;
      } else {
        status_ = CLOSE;
        return true;
      }
    });

    // BOOST_LOG_TRIVIAL(trace) << this << " , cancel timer tick";

  }

};