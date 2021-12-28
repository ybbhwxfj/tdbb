#include "network/net_service.h"
#include <memory>
#include <boost/log/trivial.hpp>
#include <atomic>
#include <utility>

template<> enum_strings<service_type>::e2s_t enum_strings<service_type>::enum2str = {
    {SERVICE_HANDLE, "NETWORK"},
    {SERVICE_RAFT, "RAFT"},
    {SERVICE_IO, "IO"},
    {SERVICE_CALVIN, "CALVIN"},
    {SERVICE_DEADLOCK, "DEADLOCK"},
    {SERVICE_LOG, "LOG"},
};

std::unordered_map<service_type, uint32_t> service_thread_num = {
    {SERVICE_HANDLE, THREAD_NUM},
    {SERVICE_RAFT, 1},
    {SERVICE_IO, THREAD_IO},
    {SERVICE_CALVIN, 1},
    {SERVICE_DEADLOCK, 1},
    {SERVICE_LOG, 1},
};

net_service::net_service(const config &conf) :
    conf_(conf),
    started_(false),
    stopped_(false),
    thread_running_(0),
    handler_(nullptr) {
  for (const auto &c: conf.node_config_list()) {
    if (!c.is_client()) {
      peers_.insert(std::make_pair(c.node_id(), c));
    }
  }
  if (not conf_.panel_config().invalid()) {
    peers_.insert(std::make_pair(conf_.panel_config().node_id(), conf.panel_config()));
  }
  for (auto &i: service_thread_num) {
    uint32_t thread_num = i.second;
    io_context_.push_back(std::make_unique<boost::asio::io_context>(
        thread_num));
  }
  for (size_t i = 0; i < io_context_.size(); i++) {
    boost::asio::io_context &c = io_context_[i];
    io_context_work_.push_back(boost::asio::make_work_guard(c));

    // make work guard must come ahead io_context::run()
  }
}

net_service::~net_service() = default;

void net_service::start() {
  bool started = false;
  bool exchanged = started_.compare_exchange_strong(started, true);
  if (!exchanged) {
    // exactly once invocation
    return;
  }

  for (size_t i = 0; i < io_context_.size(); i++) {
    auto st = service_type(i);
    auto iter = service_thread_num.find(st);
    if (iter == service_thread_num.end()) {
      BOOST_ASSERT(false);
    }
    uint32_t thread_num = iter->second;
    for (size_t j = 0; j < thread_num; j++) {
      boost::thread *thd = thread_group_.create_thread(
          boost::bind(&net_service::service_thread, this, st, j));
      // DO NOT FREE thread
      threads_.push_back(thd);
    }
  }

  // wait all thread invoke io_service::run()
  {
    std::unique_lock<std::mutex> l(condition_mutex_);
    condition_variable_.wait(
        l, [this] { return this->thread_running_ == thread_group_.size(); });
  }

  for (const auto &kv: peers_) {
    uint32_t id = kv.first;
    const node_config &p = kv.second;
    ptr<client> cli = std::make_shared<client>(p);
    outcoming_conn_.insert(std::make_pair(id, cli));
    async_client_connect(cli);
  }

  // call on_start callback of all blocks
  for (auto b: blocks_) {
    b->on_start();
  }
}

void net_service::join() {
  {
    std::unique_lock<std::mutex> l(stopped_mutex_);
    stopped_cond_.wait(l, [this]() { return stopped_.load(); });
  }
  for (const auto &kv: outcoming_conn_) {
    kv.second->close();
  }
  for (auto b: blocks_) {
    if (b) {
      b->on_stop();
    }
  }
  BOOST_LOG_TRIVIAL(info) << conf_.node_name() << " is stopping ...";

  // io_service::stop must be called in from another thread
  // (not io_context::run thread)
  for (work_guard_t &w: io_context_work_) {
    w.reset();
  }
  for (boost::asio::io_context &c: io_context_) {
    c.stop();
  }
  for (auto thd: threads_) {
    thd->join();
  }

  threads_.clear();

  BOOST_LOG_TRIVIAL(info) << id_2_name(conf_.node_id()) << " stopped ...";
}

result<ptr<client>> net_service::get_connection(uint32_t id) {
  auto iter = outcoming_conn_.find(id);
  if (iter != outcoming_conn_.end()) {
    ptr<client> c = iter->second;
    if (c->is_connected()) {
      return outcome::success(c);
    } else {
      BOOST_LOG_TRIVIAL(error) << "cannot connect to peer " << id_2_name(id);
      async_client_connect(c);
      return outcome::failure(EC::EC_NET_UNCONNECTED);
    }
  } else {
    return outcome::failure(EC::EC_NET_CANNOT_FIND_CONNECTION);
  }
}

void net_service::async_send(uint32_t id, const byte_buffer &buffer) {
  auto iter = outcoming_conn_.find(id);
  if (iter != outcoming_conn_.end()) {
    iter->second->async_write(buffer);
  }
}

void net_service::async_client_connect(const ptr<client> &client) {
  const node_config &p = client->peer();
  BOOST_ASSERT(p.node_id() != 0 && p.port() != 0);
  auto s = shared_from_this();
  this->resolve_connect(p.address(), p.port(),
                        [s, client](ptr<tcp::socket> sock, berror err) {
                          if (not err.failed()) {
                            client->connected(std::move(sock), s->handler_);
                            client->async_write_done();
                          } else {

                            ptr<boost::asio::steady_timer> t(new boost::asio::steady_timer(
                                s->get_service(SERVICE_HANDLE),
                                boost::asio::chrono::milliseconds(500)));
                            auto fn = [t, s, client](const boost::system::error_code &ec) {
                              if (not s->stopped_.load()) {
                                if (ec.failed()) {

                                }
                                s->async_client_connect(client);
                              }
                            };
                            t->async_wait(fn);

                            BOOST_LOG_TRIVIAL(error) << "connect error: " << err.message();
                          }
                        });
}

boost::asio::io_context &net_service::get_service(service_type type) {
  return io_context_[uint64_t(type)];
}

void net_service::register_block(block *block) {
  blocks_.push_back(block);
}

void net_service::service_thread(service_type st, size_t n) {
  std::string name = conf_.node_debug_name();
  name += enum2str(st)[0];
  name += std::to_string(n);

  {
    std::lock_guard<std::mutex> l(condition_mutex_);
    ++thread_running_;
    if (thread_running_ == thread_group_.size()) {
      condition_variable_.notify_all();
    }
  }
  BOOST_LOG_TRIVIAL(info) << "thread running " << name;
  set_thread_name(name);
  io_context_[uint32_t(st)].run();
}

void net_service::register_handler(message_handler handler) {
  handler_ = std::move(handler);
}

void net_service::stop() {
  std::unique_lock<std::mutex> l(stopped_mutex_);
  stopped_.store(true);
  stopped_cond_.notify_all();
}

void net_service::resolve_connect(
    const std::string &host, int port,
    const std::function<void(ptr<tcp::socket>, berror)> &func) {

  // resolver pointer must not be destructed until handler invoke
  ptr<tcp::resolver> resolver(new tcp::resolver(get_service(SERVICE_HANDLE)));
  auto s = shared_from_this();

  tcp::endpoint ep(boost::asio::ip::make_address(host), port);

  resolver->async_resolve(ep, [resolver, func,
      s](const boost::system::error_code &ec,
         const tcp::resolver::results_type &results) {
    if (not ec.failed()) {
      ptr<tcp::socket> sock(new tcp::socket(s->get_service(SERVICE_HANDLE)));
      boost::asio::async_connect(
          *sock, results,
          [s, sock, func](const boost::system::error_code &ec,
                          const tcp::endpoint &) {
            if (not s->stopped_.load()) {
              func(sock, berror(ec));
            }
          });
    } else {
      func(ptr<tcp::socket>(), berror(EC::EC_NET_RESOLVE_ADDRESS_FAIL));
      BOOST_LOG_TRIVIAL(info) << "async resolve error " << ec.message();
      // this->process_error(ec);
    }
    resolver.get(); // forbiden remove resolver from lambda parameter
  });
}

future<ptr<client>>
net_service::connect(const std::string &host, int port) {
  future<ptr<client>> f;
  this->resolve_connect(host, port,
                        [&f, this](ptr<tcp::socket> sock, berror err) {
                          ptr<client> cli(new client(std::move(sock), handler_));
                          if (not err) {
                            BOOST_LOG_TRIVIAL(debug) << "async connect done ";
                            cli->async_read();
                            f.notify_ok(cli);
                          } else {
                            f.notify_fail(err);
                          }
                        });
  return f;
}