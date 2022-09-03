#pragma once

#include <utility>

#include "common/config.h"
#include "network/connection.h"
#include "common/define.h"
#include "common/message.h"
#include "network/message_handler.h"

using boost::asio::ip::tcp;

class client : public connection {
 public:
  explicit client(boost::asio::io_context::strand s, const node_config &peer) :
      connection(s, peer.node_id()), peer_(peer) {
    BOOST_ASSERT(peer.node_id() != 0 && peer.port() != 0);
  }

  explicit client(boost::asio::io_context::strand st, ptr<tcp::socket> sock) : connection(st,
                                                                                          std::move(sock),
                                                                                          nullptr,
                                                                                          true) {}

  client(boost::asio::io_context::strand st, ptr<tcp::socket> socket, message_handler handler)
      : connection(st, std::move(socket), std::move(handler), true) {}

  void connected(ptr<tcp::socket> sock, message_handler handler);

  const node_config &peer() const { return peer_; }

 private:
  node_config peer_;
};
