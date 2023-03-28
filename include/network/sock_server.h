#include "common/config.h"
#include "common/define.h"
#include "common/message.h"
#include "common/result.hpp"
#include "network/client.h"
#include "network/connection.h"
#include "network/future.hpp"
#include "network/message_handler.h"
#include "network/net_service.h"
#include <atomic>
#include <boost/asio.hpp>
#include <boost/thread/thread.hpp>
#include <condition_variable>
#include <deque>
#include <functional>
#include <list>
#include <memory>
#include <thread>
#include <valarray>

using boost::asio::ip::tcp;
using boost::system::error_code;
//----------------------------------------------------------------------

class sock_server {
public:
  sock_server(const config &conf, ptr<net_service> service);

  ~sock_server();

  bool start();

  void stop();

  void join();

private:
  void async_accept_connection(ptr<tcp::acceptor> acceptor, service_type st);

  ptr<tcp::acceptor> acceptor_;
  ptr<tcp::acceptor> repl_acceptor_;
  ptr<net_service> service_;
  std::mutex client_conn_mutex_;
  // client to server connections ...
  std::unordered_map<std::string, ptr<connection>> incomming_conn_;
  std::atomic_bool stopped_;

  config conf_;

  void async_accept_new_connection_done(ptr<connection>);
};
