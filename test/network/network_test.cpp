#define BOOST_TEST_MODULE NETWORK_TEST

#include "common/buffer.h"
#include "common/gen_config.h"
#include "common/ptr.hpp"
#include "network/db_client.h"
#include "network/net_service.h"
#include "network/sock_server.h"
#include <boost/test/unit_test.hpp>

std::string HELLO_MESSAGE = "\
1abcdefghijklmnopqrstuvwxyz;\
2abcdefghijklmnopqrstuvwxyz;\
3abcdefghijklmnopqrstuvwxyz;\
4abcdefghijklmnopqrstuvwxyz;\
5abcdefghijklmnopqrstuvwxyz;\
6abcdefghijklmnopqrstuvwxyz;\
7abcdefghijklmnopqrstuvwxyz;\
8abcdefghijklmnopqrstuvwxyz;\
9abcdefghijklmnopqrstuvwxyz;\
10abcdefghijklmnopqrstuvwxyz;\
";

std::string HELLO_MESSAGE_LARGE;

std::map<int, std::string> MESSAGE;

class test_server {
 public:
  test_server(const config &conf) : conf_(conf) {}

  void close() {
    server_->stop();
  }

  void join() {
    server_->join();
  }

  void start() {
    service_ = ptr<net_service>(new net_service(conf_));
    server_ = ptr<sock_server>(new sock_server(conf_, service_));

    message_handler handler = [this](
        ptr<connection> conn,
        message_type id,
        byte_buffer &buffer,
        msg_hdr *
    ) -> result<void> {
      switch (id) {
        case message_type::REQUEST_HELLO: {
          auto msg = std::make_shared<hello>();
          to_pb(buffer, *msg);
          BOOST_CHECK(MESSAGE[msg->id()] == msg->message());

          for (const node_config &c : conf_.node_config_list()) {
            if (c.is_client()) {
              continue;
            }
            node_id_t node_id = c.node_id();
            auto r = service_->async_send(node_id, RESPONSE_HELLO, msg);
            if (!r) {
              BOOST_LOG_TRIVIAL(error) << "async send error";
            } else {

            }
          }

          if (conn) {

          }
          service_->conn_async_send(conn, RESPONSE_HELLO, msg);
          break;
        }
        case message_type::RESPONSE_HELLO: {
          auto response = std::make_shared<hello>();
          to_pb(buffer, *response);
          BOOST_CHECK(MESSAGE[response->id()] == response->message());
          break;
        }
        default:BOOST_ASSERT(false);
      }

      return outcome::success();
    };

    service_->register_handler(handler);
    server_->start();
  }
 private:
  config conf_;
  ptr<net_service> service_;
  ptr<sock_server> server_;
};

void fn_thread_request(ptr<db_client> c, int n) {
  for (int i = 0; i < n * 2; i++) {
    hello msg;
    auto iter = MESSAGE.find(i % n + 1);
    if (iter == MESSAGE.end()) {
      BOOST_ASSERT(false);
    }

    msg.set_id(iter->first);
    msg.set_message(iter->second);
    auto r = c->send_message(REQUEST_HELLO, msg);
    if (!r) {
      BOOST_LOG_TRIVIAL(error) << "send message error";
      sleep(1);
      continue;
    }

    hello response;
    auto r2 = c->recv_message(RESPONSE_HELLO, response);
    if (!r2) {
      BOOST_LOG_TRIVIAL(error) << "recv message error: " << r2.error().message();
    } else {
      //BOOST_LOG_TRIVIAL(trace) << "client receive message :" << response.message();
      BOOST_CHECK(MESSAGE[response.id()] == response.message());
    }
  }
}

BOOST_AUTO_TEST_CASE(network_test) {
  boost::unit_test::unit_test_log_t::instance().set_threshold_level(boost::unit_test::log_fatal_errors);
  int n = 0;
  do {
    n++;
    HELLO_MESSAGE_LARGE += HELLO_MESSAGE;
    MESSAGE[n] = HELLO_MESSAGE_LARGE;
  } while (HELLO_MESSAGE_LARGE.size() < MESSAGE_BUFFER_SIZE);

  config_option option;
  option.num_az = 2;
  option.num_shard = 1;
  std::vector<config> conf = generate_config(option);
  std::vector<ptr<test_server>> server;
  for (auto c : conf) {
    ptr<test_server> s(new test_server(c));
    server.push_back(s);
    s->start();
  }

  std::vector<ptr<db_client>> clients;

  for (size_t i = 0; i < conf.size(); i++) {
    for (size_t j = 0; j < 2; j++) {
      ptr<db_client> c(new db_client(conf[i].this_node_config()));
      for (;;) {
        bool ok = c->connect();
        if (ok) {
          break;
        }
        sleep(1);
      }
      clients.push_back(c);
    }
  }

  std::vector<ptr<std::thread>> thds;
  for (size_t i = 0; i < clients.size(); i++) {
    ptr<std::thread> thd(new std::thread(fn_thread_request, clients[i], n));
    thds.push_back(thd);
  }

  for (auto t : thds) {
    t->join();
  }

  for (auto s : server) {
    s->close();
  }
  for (auto s : server) {
    s->join();
  }

}