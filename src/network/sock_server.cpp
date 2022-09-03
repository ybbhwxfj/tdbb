#include "network/sock_server.h"
#include "network/future.hpp"
#include <boost/log/trivial.hpp>
#include <mutex>
using std::size_t;

//----------------------------------------------------------------------

sock_server::sock_server(const config &conf, ptr<net_service> service)
    :
    port_(conf.this_node_config().port()),
    endpoint_(tcp::v4(), conf.this_node_config().port()),

    service_(service),
    stopped_(false),
    conf_(conf) {}

sock_server::~sock_server() {}

void sock_server::async_accept_connection() {
  boost::asio::io_context::strand strand(service_->get_service(SERVICE_ASYNC_CONTEXT));
  ptr<tcp::socket> socket(new tcp::socket(strand.context()));
  acceptor_->async_accept(*socket, [this, strand, socket](boost_ec ec) {
    if (stopped_.load()) {
      BOOST_LOG_TRIVIAL(info) << id_2_name(conf_.node_id()) << " cancel_and_join accept connection";
      return;
    }
    if (not ec) {
      ptr<connection> ctx = cs_new<connection>(strand, socket, service_->get_handler(), false);
      ctx->process_error(berror(ec));
      boost::asio::ip::tcp::endpoint ep = socket->remote_endpoint();
      std::string ip = ep.address().to_string() + ":" + std::to_string(ep.port());
      client_conn_mutex_.lock();
      incomming_conn_.insert(std::make_pair(ip, ctx));
      client_conn_mutex_.unlock();
      ctx->connected();
      async_accept_connection();
      async_accept_new_connection_done(ctx);
    } else {
      BOOST_LOG_TRIVIAL(info) << id_2_name(conf_.node_id()) << " async accept error ..";
    }
  });
}

void sock_server::async_accept_new_connection_done(
    ptr<connection> ctx) {
  ctx->async_read();
}

bool sock_server::start() {
  service_->start();

  acceptor_.reset(new boost::asio::ip::tcp::acceptor(service_->get_service(SERVICE_ASYNC_CONTEXT),
                                                     endpoint_));
  socket_.reset(new boost::asio::ip::tcp::socket(service_->get_service(SERVICE_ASYNC_CONTEXT)));

  async_accept_connection();
  BOOST_LOG_TRIVIAL(info) << id_2_name(conf_.node_id()) << " server listen on port [" << port_ << "].";
  BOOST_LOG_TRIVIAL(info) << id_2_name(conf_.node_id()) << " begin accept connection ...";
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

  BOOST_LOG_TRIVIAL(info) << "close incoming connections";

  client_conn_mutex_.unlock();
  stopped_.store(true);

  service_->stop();

  if (acceptor_) {
    acceptor_->close();
  }
  if (socket_) {
    socket_->close();
  }

}

void sock_server::join() {
  if (service_) {
    service_->join();
  }
}