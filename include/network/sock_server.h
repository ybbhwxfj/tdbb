#include "network/client.h"
#include "network/connection.h"
#include "common/define.h"
#include "common/result.hpp"
#include "network/future.hpp"
#include "common/message.h"
#include "network/message_handler.h"
#include "network/net_service.h"
#include "common/config.h"
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <thread>
#include <valarray>
#include <atomic>

using boost::asio::ip::tcp;
using boost::system::error_code;
//----------------------------------------------------------------------

class sock_server {
public:
  sock_server(const config &conf, net_service *service);
  ~sock_server();
  bool start();

  void stop();
  void join();
private:

  void async_accept_connection();

  uint16_t port_;

  // take care of the order ...
  // endpoint_ , io_service_ must be initialized before acceptor_
  // io_service_ must be initialized before socket_
  tcp::endpoint endpoint_;

  std::unique_ptr<tcp::acceptor> acceptor_;
  std::unique_ptr<tcp::socket> socket_;
  net_service *service_;
  std::mutex client_conn_mutex_;
  // client to server connections ...
  std::unordered_map<std::string, ptr<connection>> incomming_conn_;
  std::atomic_bool stopped_;

  config conf_;
  void async_accept_new_connection_done(ptr<connection>);
};
