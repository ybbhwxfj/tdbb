#include "network/net_service.h"
#include "common/logger.hpp"
#include <atomic>
#include <memory>
#include <utility>

template<>
enum_strings<service_type>::e2s_t enum_strings<service_type>::enum2str = {
    {SERVICE_ASYNC_CONTEXT, "ASYNC"},
    {SERVICE_IO, "IO"},
    {SERVICE_CC, "CC"},
    {SERVICE_REPLICATION, "REPLICATION"}};

std::unordered_map<service_type, uint32_t> service_thread_num = {
    {SERVICE_ASYNC_CONTEXT, THREADS_ASYNC_CONTEXT},
    {SERVICE_IO, THREADS_IO},
    {SERVICE_CC, THREADS_CC},
    {SERVICE_REPLICATION, THREADS_REPLICATION}};

net_service::net_service(const config &conf)
    : conf_(conf), started_(false), stopped_(false), thread_running_(0),
      handler_(nullptr), local_handler_(nullptr) {
  for (const auto &c : conf.node_server_list()) {
    if (!c.is_client()) {
      peers_.insert(std::make_pair(c.node_id(), c));
    }
  }

  for (auto &i : service_thread_num) {
    uint32_t thread_num = 0;
    service_type service = i.first;
    switch (service) {
    case service_type::SERVICE_ASYNC_CONTEXT: {
      thread_num = conf_.get_block_config().threads_async_context();
      break;
    }
    case service_type::SERVICE_IO: {
      thread_num = conf_.get_block_config().threads_io();
      break;
    }
    case service_type::SERVICE_CC: {
      thread_num = conf_.get_block_config().threads_cc();
      break;
    }
    case service_type::SERVICE_REPLICATION: {
      thread_num = conf_.get_block_config().threads_replication();
      break;
    }
    default:break;
    }
    if (thread_num == 0) {
      // default thread num
      thread_num = i.second;
    }

    io_context_.push_back(
        std::make_unique<boost::asio::io_context>(thread_num));
    io_context_threads_.push_back(std::make_pair(service, thread_num));
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
    service_type service = io_context_threads_[i].first;
    uint32_t thread_num = io_context_threads_[i].second;
    for (size_t j = 0; j < thread_num; j++) {
      boost::thread *thd = thread_group_.create_thread(
          [this, service, j] { service_thread(service, j); });
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
  uint64_t connections = conf_.get_block_config().connections_per_peer();

  for (const auto &kv : peers_) {
    boost::asio::io_context::strand s(get_service(SERVICE_ASYNC_CONTEXT));
    uint32_t id = kv.first;
    const node_config &p = kv.second;
    std::vector<ptr<client>> clients;
    std::string address = p.address_public_or_private(conf_.az_id());
    node_peer peer(p.node_id(), address, p.port());
    for (size_t i = 0; i < connections; i++) {
      ptr<client> cli = std::make_shared<client>(s, peer);
      clients.push_back(cli);
    }

    out_coming_conn_.insert(std::make_pair(id, clients));
    for (auto cli : clients) {
      async_client_connect(cli);
    }
  }

  // call on_start callback of all blocks
  for (auto b : blocks_) {
    b->on_start();
  }
}

void net_service::join() {
  {
    std::unique_lock<std::mutex> l(stopped_mutex_);
    stopped_cond_.wait(l, [this]() { return stopped_.load(); });
  }
  LOG(info) << conf_.node_name() << " is stopping ...";

  for (const auto &kv : out_coming_conn_) {
    for (auto &c : kv.second) {
      c->close();
    }
  }

  // io_service::cancel_and_join must be called in from another thread
  // (not io_context::run thread)
  for (work_guard_t &w : io_context_work_) {
    w.reset();
  }
  for (boost::asio::io_context &c : io_context_) {
    c.stop();
  }
  for (auto thd : threads_) {
    thd->join();
  }
  // call on_stop callback after join processing thread
  for (auto b : blocks_) {
    if (b) {
      b->on_stop();
    }
  }

  threads_.clear();
  out_coming_conn_.clear();
  LOG(info) << id_2_name(conf_.node_id()) << " stopped ...";
}

result<ptr<client>> net_service::get_connection(uint32_t id) {
  auto iter = out_coming_conn_.find(id);
  if (iter != out_coming_conn_.end()) {
    size_t n = iter->second.size();
    uint64_t i = random(n);
    ptr<client> c = iter->second[i];
    return outcome::success(c);
  } else {
    return outcome::failure(EC::EC_NET_CANNOT_FIND_CONNECTION);
  }
}

void net_service::async_client_connect(const ptr<client> &client) {
  auto s = shared_from_this();
  this->resolve_connect(client);
}

boost::asio::io_context &net_service::get_service(service_type type) {
  return io_context_[uint64_t(type)];
}

void net_service::register_block(block *block) { blocks_.push_back(block); }

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
  LOG(trace) << "thread running " << name;
  set_thread_name(name);
  io_context_[uint32_t(st)].run();
}

void net_service::register_handler(message_handler handler) {
  handler_ = handler;
}

void net_service::register_local_handler(proto_message_handler handler) {
  local_handler_ = handler;
}

node_id_t net_service::node_id() {
  return conf_.node_id();
}

void net_service::stop() {
  std::unique_lock<std::mutex> l(stopped_mutex_);
  stopped_.store(true);
  stopped_cond_.notify_all();
}

bool net_service::is_sopped() { return stopped_.load(); }

void net_service::resolve_connect(ptr<client> client) {

  // resolver pointer must not be destructed until handler invoke
  ptr<tcp::resolver> resolver(
      new tcp::resolver(get_service(SERVICE_ASYNC_CONTEXT)));
  auto s = shared_from_this();

  tcp::endpoint ep(boost::asio::ip::make_address(client->peer().address_),
                   client->peer().port_);

  resolver->async_resolve(ep, [client, resolver,
      s](const boost::system::error_code &ec,
         const tcp::resolver::results_type &results) {
    if (not ec.failed()) {
      ptr<tcp::socket> sock(new tcp::socket(client->get_strand().context()));
      auto handler = boost::asio::bind_executor(
          client->get_strand(),
          [client, s, sock](const boost::system::error_code &ec,
                            const tcp::endpoint &) {
            if (not s->stopped_.load()) {
              s->handle_connect_done(client, sock, berror(ec));
            }
          });
      boost::asio::async_connect(*sock, results, handler);
    } else {
      s->handle_connect_done(client, ptr<tcp::socket>(),
                             berror(EC::EC_NET_RESOLVE_ADDRESS_FAIL));
      LOG(info) << "async resolve error " << id_2_name(client->peer().node_id_)
                << ec.message();
      // this->process_error(ec);
    }
    resolver.get(); // forbiden remove resolver from lambda parameter
  });
}

void net_service::handle_connect_done(ptr<client> client, ptr<tcp::socket> sock,
                                      berror err) {
  if (not err.failed()) {
    client->connected(std::move(sock), handler_);
    client->async_write_done();
  } else {
    ptr<boost::asio::steady_timer> t(new boost::asio::steady_timer(
        client->get_strand().context(),
        boost::asio::chrono::milliseconds(TCP_CONNECT_TIMEOUT_MILLIS)));
    auto s = shared_from_this();
    auto wait_timeout_handler = boost::asio::bind_executor(
        client->get_strand(),
        [t, s, client](const boost::system::error_code &ec) {
          if (not s->stopped_.load()) {
            if (ec.failed()) {
              LOG(error) << "async wait error: " << ec.message();
            }
            s->async_client_connect(client);
          }
        });
    t->async_wait(wait_timeout_handler);

    LOG(trace) << "connect to node name: " << id_2_name(client->peer().node_id_)
               << ", peer: " << client->peer().address_ << ","
               << client->peer().port_ << " error: " << err.message();
  }
}