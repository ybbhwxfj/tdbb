#include "network/debug_server.h"
#include <memory>
#include <boost/log/trivial.hpp>
#include <sstream>
//------------------------------------------------------------------------------

// Return a reasonable mime type based on the extension of a file.
beast::string_view mime_type(beast::string_view path) {
  using beast::iequals;
  auto const ext = [&path] {
    auto const pos = path.rfind(".");
    if (pos == beast::string_view::npos)
      return beast::string_view{};
    return path.substr(pos);
  }();
  if (iequals(ext, ".htm"))
    return "text/html";
  if (iequals(ext, ".html"))
    return "text/html";
  if (iequals(ext, ".php"))
    return "text/html";
  if (iequals(ext, ".css"))
    return "text/css";
  if (iequals(ext, ".txt"))
    return "text/plain";
  if (iequals(ext, ".js"))
    return "application/javascript";
  if (iequals(ext, ".json"))
    return "application/json";
  if (iequals(ext, ".xml"))
    return "application/xml";
  if (iequals(ext, ".swf"))
    return "application/x-shockwave-flash";
  if (iequals(ext, ".flv"))
    return "video/x-flv";
  if (iequals(ext, ".png"))
    return "image/png";
  if (iequals(ext, ".jpe"))
    return "image/jpeg";
  if (iequals(ext, ".jpeg"))
    return "image/jpeg";
  if (iequals(ext, ".jpg"))
    return "image/jpeg";
  if (iequals(ext, ".gif"))
    return "image/gif";
  if (iequals(ext, ".bmp"))
    return "image/bmp";
  if (iequals(ext, ".ico"))
    return "image/vnd.microsoft.icon";
  if (iequals(ext, ".tiff"))
    return "image/tiff";
  if (iequals(ext, ".tif"))
    return "image/tiff";
  if (iequals(ext, ".svg"))
    return "image/svg+xml";
  if (iequals(ext, ".svgz"))
    return "image/svg+xml";
  return "application/text";
}

// Append an HTTP rel-path to a local filesystem path.
// The returned path is normalized for the platform.
std::string path_cat(beast::string_view base, beast::string_view path) {
  if (base.empty())
    return std::string(path);
  std::string result(base);
#ifdef BOOST_MSVC
  char constexpr path_separator = '\\';
  if (result.back() == path_separator)
    result.resize(result.size() - 1);
  result.append(path.data(), path.size());
  for (auto &c : result)
    if (c == '/')
      c = path_separator;
#else
  char constexpr path_separator = '/';
  if (result.back() == path_separator)
    result.resize(result.size() - 1);
  result.append(path.data(), path.size());
#endif
  return result;
}

// This function produces an HTTP response for the given
// request. The type of the response object depends on the
// contents of the request, so the interface requires the
// caller to pass a generic lambda for receiving the response.
template<class Body, class Allocator, class Send>
void handle_request(const http_handler &handler,
                    http::request<Body, http::basic_fields<Allocator>> &&req,
                    Send &&send) {
  // Returns a bad request response
  auto const bad_request = [&req](beast::string_view why) {
    http::response<http::string_body> res{http::status::bad_request,
                                          req.version()};
    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(http::field::content_type, "text/html");
    res.keep_alive(req.keep_alive());
    res.body() = std::string(why);
    res.prepare_payload();
    return res;
  };

  // Returns a not found response
  auto const not_found = [&req](beast::string_view target) {
    http::response<http::string_body> res{http::status::not_found,
                                          req.version()};
    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(http::field::content_type, "text/html");
    res.keep_alive(req.keep_alive());
    res.body() = "The resource '" + std::string(target) + "' was not found.";
    res.prepare_payload();
    return res;
  };

  // Returns a server error response
  auto const server_error = [&req](beast::string_view what) {
    http::response<http::string_body> res{http::status::internal_server_error,
                                          req.version()};
    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(http::field::content_type, "text/html");
    res.keep_alive(req.keep_alive());
    res.body() = "An error occurred: '" + std::string(what) + "'";
    res.prepare_payload();
    return res;
  };

  // Make sure we can handle the method
  if (req.method() != http::verb::get && req.method() != http::verb::head)
    return send(bad_request("Unknown HTTP-method"));

  // Request path must be absolute and not contain "..".
  if (req.target().empty() || req.target()[0] != '/' ||
      req.target().find("..") != beast::string_view::npos)
    return send(bad_request("Illegal request-target"));

  // Build the path to the requested file
  std::string path = std::string(req.target());


  // Attempt to open the file
  beast::error_code ec;
  std::string body;
  if (req.target().back() == '/') {
    body = "hello world!";
  }

  // Handle the case where the file doesn't exist
  if (ec == beast::errc::no_such_file_or_directory)
    return send(not_found(req.target()));

  // Handle an unknown error
  if (ec)
    return send(server_error(ec.message()));

  std::stringstream ssm;
  handler(path, ssm);
  body = ssm.str();
  //BOOST_ASSERT(!body.empty());

  // Cache the size since we need it after the move
  auto const size = body.size();

  // Respond to HEAD request
  if (req.method() == http::verb::head) {
    http::response<http::empty_body> res{http::status::ok, req.version()};
    res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    res.set(http::field::content_type, mime_type(path));
    res.content_length(size);
    res.keep_alive(req.keep_alive());
    return send(std::move(res));
  }


  // Respond to GET request
  http::response<http::string_body> res{
      std::piecewise_construct, std::make_tuple(std::move(body)),
      std::make_tuple(http::status::ok, req.version())};
  res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
  res.set(http::field::content_type, "text/plain");
  res.content_length(size);
  res.keep_alive(req.keep_alive());

  return send(std::move(res));
}

//------------------------------------------------------------------------------

// Report a failure
void fail(beast::error_code ec, char const *what) {
  std::cerr << what << ": " << ec.message() << "\n";
}

// This is the C++11 equivalent of a generic lambda.
// The function object is used to send an HTTP message.
template<class Stream> struct send_lambda {
  Stream &stream_;
  bool &close_;
  beast::error_code &ec_;

  explicit send_lambda(Stream &stream, bool &close, beast::error_code &ec)
      : stream_(stream), close_(close), ec_(ec) {}

  template<bool isRequest, class Body, class Fields>
  void operator()(http::message<isRequest, Body, Fields> &&msg) const {
    // Determine if we should close the connection after
    close_ = msg.need_eof();

    // We need the serializer here because the serializer requires
    // a non-const file_body, and the message oriented version of
    // http::write only works with const messages.
    http::serializer<isRequest, Body, Fields> sr{msg};
    http::write(stream_, sr, ec_);
  }
};

// Handles an HTTP server connection
void do_session(const http_handler &handler,
                const ptr<tcp::socket> &socket
) {
  bool close = false;
  beast::error_code ec;

  // This buffer is required to persist across reads
  beast::flat_buffer buffer;

  // This lambda is used to send messages
  send_lambda<tcp::socket> lambda{*socket, close, ec};

  for (;;) {
    // Read a request
    http::request<http::string_body> req;
    http::read(*socket, buffer, req, ec);
    if (ec == http::error::end_of_stream)
      break;
    if (ec)
      return fail(ec, "read");

    // Send the response
    handle_request(handler, std::move(req), lambda);
    if (ec)
      return fail(ec, "write");
    if (close) {
      // This means we should close the connection, usually because
      // the response indicated the "Connection: close" semantic.
      break;
    }
  }

  // Send a TCP shutdown
  socket->shutdown(tcp::socket::shutdown_send, ec);
  // At this point the connection is closed gracefully
}

//------------------------------------------------------------------------------
void debug_server::start_thd() {
  ctx_->run();
  BOOST_LOG_TRIVIAL(info) << " debug server stopped";
}

void debug_server::async_accept() {
  // This will receive the new connection
  ptr<tcp::socket> socket(new tcp::socket{*ctx_});

  // Async accept
  acceptor_->async_accept(*socket, [socket, this](boost::system::error_code ec) {
    if (!ec.failed()) {
      // Launch the session, transferring ownership of the socket
      handle_accept(socket);
    } else {
      BOOST_LOG_TRIVIAL(error) << "accept error " << ec;
    }
  });
}

void debug_server::handle_accept(ptr<tcp::socket> sock) {
  std::scoped_lock l(mutex_);
  socket_.insert(std::make_pair(sock.get(), sock));
  // Launch the session, transferring ownership of the socket
  thd_.emplace_back(std::make_shared<boost::thread>(
      std::bind(&debug_server::session, this, std::move(sock))));
  async_accept();
}

void debug_server::session(const ptr<tcp::socket> &sock) {
  do_session(handler_, sock);
  std::scoped_lock l(mutex_);
  socket_.erase(sock.get());
}

void debug_server::start() {
  // The io_context is required for all I/O
  ctx_ = std::make_shared<net::io_context>(1);
  work_.push_back(boost::asio::make_work_guard(*ctx_));

  acceptor_ = std::make_shared<tcp::acceptor>(*ctx_);

  boost::asio::ip::tcp::endpoint endpoint(boost::asio::ip::tcp::v4(), port_);
  acceptor_->open(endpoint.protocol());
  acceptor_->bind(endpoint);
  acceptor_->listen();
  // The acceptor receives incoming connections

  thd_.emplace_back(std::make_shared<boost::thread>(
      boost::bind(&debug_server::start_thd, this)));
  async_accept();
}

void debug_server::stop() {
  if (acceptor_) {
    acceptor_->close();
  }
  if (ctx_) {
    ctx_->stop();
  }
  {
    std::scoped_lock l(mutex_);
    for (auto &s: socket_) {
      s.second->close();
    }
  }
}
void debug_server::join() {
  for (const auto &t: thd_) {
    t->join();
  }
}