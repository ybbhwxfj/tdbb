#include "network/sock_server.h"
#include "common/logger.hpp"
#include "network/future.hpp"
#include <mutex>
using std::size_t;

//----------------------------------------------------------------------

sock_server::sock_server(const config &conf, ptr<net_service> service)
    : service_(service), stopped_(false), conf_(conf) {}

sock_server::~sock_server() {}

void sock_server::async_accept_connection(ptr<tcp::acceptor> acceptor,
                                          service_type st) {
  boost::asio::io_context::strand strand(service_->get_service(st));
  ptr<tcp::socket> socket(new tcp::socket(strand.context()));
  auto fn_handle_accept = [this, strand, socket, acceptor, st](boost_ec ec) {
    if (stopped_.load()) {
      LOG(info) << id_2_name(conf_.node_id())
                << " cancel_and_join accept connection";
      return;
    }
    if (not ec) {
      ptr<connection> ctx =
          cs_new<connection>(strand, socket, service_->get_handler(), false);
      ctx->process_error(berror(ec));
      boost::asio::ip::tcp::endpoint ep = socket->remote_endpoint();
      std::string ip =
          ep.address().to_string() + ":" + std::to_string(ep.port());
      client_conn_mutex_.lock();
      incomming_conn_.insert(std::make_pair(ip, ctx));
      client_conn_mutex_.unlock();
      ctx->connected();
      async_accept_connection(acceptor, st);
      async_accept_new_connection_done(ctx);
    } else {
      LOG(info) << id_2_name(conf_.node_id()) << " async accept error ..";
    }
  };
  acceptor->async_accept(*socket, fn_handle_accept);
}

void sock_server::async_accept_new_connection_done(ptr<connection> ctx) {
  ctx->async_read();
}

bool sock_server::start() {
  service_->start();
  uint32_t port = conf_.this_node_config().port();
  uint32_t repl_port = conf_.this_node_config().repl_port();
  tcp::endpoint ep(tcp::v4(), port);
  tcp::endpoint repl_ep(tcp::v4(), repl_port);
  acceptor_.reset(new boost::asio::ip::tcp::acceptor(
      service_->get_service(SERVICE_ASYNC_CONTEXT), ep));

  repl_acceptor_.reset(new boost::asio::ip::tcp::acceptor(
      service_->get_service(SERVICE_ASYNC_CONTEXT), repl_ep));
  async_accept_connection(acceptor_, SERVICE_ASYNC_CONTEXT);
  async_accept_connection(repl_acceptor_, SERVICE_ASYNC_CONTEXT);
  LOG(info) << id_2_name(conf_.node_id()) << " server listen on port [" << port
            << "].";
  LOG(info) << id_2_name(conf_.node_id())
            << " server listen on replication port [" << repl_port << "].";
  LOG(info) << id_2_name(conf_.node_id()) << " begin accept connection ...";
  return true;
}

void sock_server::stop() {
  if (stopped_.load()) {
    return;
  }
  client_conn_mutex_.lock();
  for (auto kv : incomming_conn_) {
    ptr<connection> c = kv.second;
    if (c) {
      c->close();
    }
  }
  incomming_conn_.clear();

  LOG(info) << "close incoming connections";

  client_conn_mutex_.unlock();
  stopped_.store(true);

  service_->stop();

  if (acceptor_) {
    acceptor_->close();
  }
  if (repl_acceptor_) {
    repl_acceptor_->close();
  }
}

void sock_server::join() {
  if (service_) {
    service_->join();
  }
}