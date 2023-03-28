#pragma once

#include "common/config.h"
#include "common/ptr.hpp"
#include "network/client.h"
#include <boost/asio.hpp>

class db_client {
private:
  az_id_t az_id_;
  boost::asio::io_context io_context_;
  boost::asio::io_context::strand strand_;
  node_config conf_;
  ptr<client> cli_;

public:
  explicit db_client(az_id_t az_id, node_config conf);

  ptr<client> client_ptr() { return cli_; }

  bool connect();

  template<typename M>
  result<void> send_message(message_type id, const M &msg) {
    if (cli_) {
      return cli_->send_message(id, msg);
    } else {
      return outcome::failure(EC::EC_NET_UNCONNECTED);
    }
  }

  template<typename M> result<void> recv_message(message_type id, M &msg) {
    if (cli_) {
      return cli_->recv_message(id, msg);
    } else {
      return outcome::failure(EC::EC_NET_UNCONNECTED);
    }
  }

private:
};