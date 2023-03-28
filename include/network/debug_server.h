#pragma once

#include "common/ptr.hpp"
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/config.hpp>
#include <boost/thread.hpp>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

namespace beast = boost::beast;   // from <boost/beast.hpp>
namespace http = beast::http;     // from <boost/beast/http.hpp>
namespace net = boost::asio;      // from <boost/asio.hpp>
using tcp = boost::asio::ip::tcp; // from <boost/asio/ip/tcp.hpp>

typedef std::function<void(const std::string &path, std::ostream &body)>
    http_handler;

class debug_server {
private:
  std::string address_;
  uint16_t port_;
  std::vector<ptr<boost::thread>> thd_;
  ptr<net::io_context> ctx_;
  typedef boost::asio::executor_work_guard<
      boost::asio::io_context::executor_type>
      work_guard_t;
  std::vector<work_guard_t> work_;
  ptr<tcp::acceptor> acceptor_;
  http_handler handler_;
  std::recursive_mutex mutex_;
  std::map<tcp::socket *, ptr<tcp::socket>> socket_;

public:
  debug_server(std::string addr, uint16_t port, http_handler handler)
      : address_(std::move(addr)), port_(port), handler_(std::move(handler)) {}

  void start();

  void join();

  void stop();

private:
  void session(const ptr<tcp::socket> &sock);

  void async_accept();

  void handle_accept(ptr<tcp::socket> sock);

  void start_thd();
};