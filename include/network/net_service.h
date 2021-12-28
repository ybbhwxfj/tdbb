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

using boost::asio::ip::tcp;
using boost::system::error_code;

//----------------------------------------------------------------------

enum service_type {
  SERVICE_HANDLE = 0,
  SERVICE_RAFT,
  SERVICE_IO,
  SERVICE_CALVIN,
  SERVICE_DEADLOCK,
  SERVICE_LOG,
};

template<> enum_strings<service_type>::e2s_t enum_strings<service_type>::enum2str;

const uint64_t SERVICE_TYPE_NUM = 5;

class net_service : public sender, public std::enable_shared_from_this<net_service> {
public:
  config conf_;
  std::atomic<bool> started_;
  std::atomic<bool> stopped_;
  std::mutex stopped_mutex_;
  std::condition_variable stopped_cond_;
  std::unordered_map<uint32_t, node_config> peers_;

  std::unordered_map<uint32_t, ptr<client>> outcoming_conn_;
  boost::ptr_vector<boost::asio::io_context> io_context_;
  typedef boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard_t;
  std::vector<work_guard_t> io_context_work_;
  boost::thread_group thread_group_;
  std::condition_variable condition_variable_;
  std::mutex condition_mutex_;
  size_t thread_running_;
  message_handler handler_;
  // do not delete, thread is hold by others
  std::vector<boost::thread *> threads_;
  std::vector<block *> blocks_;

  net_service(const config &conf);
  ~net_service();

  void stop();
  void start();

  void join();

  void register_block(block *block);

  boost::asio::io_context &get_service(service_type type);

  future<ptr<client>> connect(const std::string &host, int port);
  void async_client_connect(const ptr<client> &client);

  void resolve_connect(const std::string &host, int port,
                       const std::function<void(ptr<tcp::socket>, berror)> &func);

  void register_handler(message_handler handler);

  message_handler get_handler() { return handler_; }

  virtual void async_send(uint32_t id, const byte_buffer &buffer);

  virtual result<ptr<client>> get_connection(uint32_t id);

  template<typename PB_MSG>
  result<void> async_send(uint32_t node_id, message_type mt, PB_MSG &msg) {
    if (conf_.node_id() == node_id) {
      size_t body_size = msg.ByteSizeLong() + sizeof(msg_hdr);
      ptr<byte_buffer> buf(new byte_buffer(body_size));
      auto r = pb_body_to_buf(*buf, msg);
      if (not r) {
        return r;
      }
      auto s = shared_from_this();
      auto fn = [s, buf, mt]() {
        auto rh = s->handler_(nullptr, mt, *buf);
        if (not rh) {

        }
      };
      boost::asio::dispatch(get_service(SERVICE_HANDLE), fn);
      return outcome::success();
    } else {
      result<ptr<client>> r = get_connection(node_id);
      if (r) {
        ptr<client> c = r.value();
        result<void> sr = c->async_send(mt, msg);
        if (sr.has_failure() && sr.error().code() == EC_NET_UNCONNECTED) {
          async_client_connect(c);
        }
        return sr;
      } else {
        return r.error();
      }
    }
  }
private:
  void service_thread(service_type st, size_t n);
};
