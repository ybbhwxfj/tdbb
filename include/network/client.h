#pragma once

#include <utility>

#include "common/config.h"
#include "common/define.h"
#include "common/message.h"
#include "network/connection.h"
#include "network/message_handler.h"

using boost::asio::ip::tcp;

struct node_peer {
  node_id_t node_id_;
  std::string address_;
  uint32_t port_;
  node_peer(node_id_t node_id, const std::string &address, uint32_t port)
      : node_id_(node_id), address_(address), port_(port) {}
};

class client : public connection {
public:
  explicit client(boost::asio::io_context::strand s, const node_peer &peer)
      : connection(s, peer.node_id_), peer_(peer) {
    BOOST_ASSERT(peer.node_id_!=0 && peer.port_!=0);
  }

  explicit client(boost::asio::io_context::strand st, ptr<tcp::socket> sock,
                  const node_peer &peer)
      : connection(st, std::move(sock), nullptr, true), peer_(peer) {}

  client(boost::asio::io_context::strand st, ptr<tcp::socket> socket,
         message_handler handler, const node_peer &peer)
      : connection(st, std::move(socket), std::move(handler), true),
        peer_(peer) {}

  void connected(ptr<tcp::socket> sock, message_handler handler);

  const node_peer &peer() const { return peer_; }

private:
  node_peer peer_;
};
