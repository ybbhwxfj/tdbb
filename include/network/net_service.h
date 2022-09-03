#pragma once

#include "common/variable.h"
#include "common/block.h"
#include "common/config.h"
#include "common/define.h"
#include "common/result.hpp"
#include "common/message.h"
#include "common/ptr.hpp"
#include "network/client.h"
#include "network/connection.h"
#include "network/future.hpp"
#include "network/message_handler.h"
#include "common/set_thread_name.h"
#include "network/sender.h"
#include <atomic>
#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <boost/thread/thread.hpp>
#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <valarray>
#include <vector>
#include <memory>

//----------------------------------------------------------------------

enum service_type {
  SERVICE_ASYNC_CONTEXT = 0,
  SERVICE_IO = 1,
  SERVICE_LOCAL = 2,
};

template<> enum_strings<service_type>::e2s_t enum_strings<service_type>::enum2str;

class net_service : public sender, public std::enable_shared_from_this<net_service> {
 public:
  config conf_;
  std::atomic<bool> started_;
  std::atomic<bool> stopped_;
  std::mutex stopped_mutex_;
  std::condition_variable stopped_cond_;
  std::unordered_map<uint32_t, node_config> peers_;

  std::unordered_map<uint32_t, std::vector<ptr<client>>> out_coming_conn_;
  boost::ptr_vector<boost::asio::io_context> io_context_;
  std::vector<std::pair<service_type, uint32_t>> io_context_threads_;
  typedef boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_t;
  std::vector<work_guard_t> io_context_work_;
  boost::thread_group thread_group_;
  std::condition_variable condition_variable_;
  std::mutex condition_mutex_;
  size_t thread_running_;
  message_handler handler_;
  proto_message_handler local_handler_;
  // do not delete, thread is hold by others
  std::vector<boost::thread *> threads_;
  std::vector<block *> blocks_;

  net_service(const config &conf);

  ~net_service();

  void stop();

  bool is_sopped();

  void start();

  void join();

  void register_block(block *block);

  boost::asio::io_context &get_service(service_type type);

  void async_client_connect(const ptr<client> &client);

  void resolve_connect(ptr<client> client);

  void register_local_handler(proto_message_handler handler);

  void register_handler(message_handler handler);

  message_handler get_handler() { return handler_; }

  virtual void async_send(uint32_t id, const byte_buffer &buffer);

  virtual result<ptr<client>> get_connection(uint32_t id);

  template<typename PB_MSG>
  void conn_async_send(ptr<connection> c, message_type mt, const ptr<PB_MSG> m) {
    boost::asio::post(c->get_strand(), [c, mt, m]() {
      auto r = c->template async_send(mt, m);
      if (not r) {
        // TODO...
        BOOST_LOG_TRIVIAL(error) << "async send error ";
      }
    });
  }
  template<typename PB_MSG>
  result<void> async_send(
      uint32_t node_id,
      message_type mt,
      const ptr<PB_MSG> m) {
    fn_msg_hdr fn_set_hdr = nullptr;
#ifdef TEST_NETWORK_TIME
    {
      uint64_t ms = ms_since_epoch();
      fn_set_hdr = [ms](msg_hdr &hdr) {
        hdr.set_millis1(ms);
      };
    }
#endif
    if (conf_.node_id() == node_id && local_handler_) {
      return async_send_local(mt, m);
    } else {
      async_send_remote(node_id, mt, m, fn_set_hdr);
      return outcome::success();
    }
  }

 private:
  void service_thread(service_type st, size_t n);
  template<typename PB_MSG>
  result<void> async_send_local(message_type mt, const ptr<PB_MSG> m) {
    auto s = shared_from_this();
    auto fn = [s, mt, m]() {
      auto rh = s->local_handler_(nullptr, mt, m);
      if (not rh) {
      }
    };
    // do not use boost::asio::dispatch to avoid recursive call which possible leads to deadlock
    boost::asio::post(get_service(SERVICE_ASYNC_CONTEXT), fn);
    return outcome::success();
  }

  template<typename PB_MSG>
  void async_send_remote(uint32_t node_id, message_type mt, const ptr<PB_MSG> m, fn_msg_hdr fn_set_hdr = nullptr) {
    result<ptr<client>> r = get_connection(node_id);
    if (r) {
      ptr<client> c = r.value();
      auto service = shared_from_this();
      boost::asio::post(c->get_strand(), [service, c, mt, m, fn_set_hdr] {
        result<void> sr = c->async_send(mt, m, fn_set_hdr);
        if (sr.has_failure() && sr.error().code() == EC_NET_UNCONNECTED) {
          service->async_client_connect(c);
        }
      });

    } else {
      BOOST_LOG_TRIVIAL(error) << " cannot find connection to " << id_2_name(node_id) << " " << r.error();
    }
  }

  void handle_connect_done(ptr<client> client, ptr<tcp::socket> sock, berror err);

  void handle_resolve_done(ptr<client> client);

  // [0..n)
  static uint64_t random(uint64_t n) {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(0, n - 1);
    return distribution(generator);
  }
};
