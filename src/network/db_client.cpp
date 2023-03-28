#include "network/db_client.h"

#include <utility>

db_client::db_client(az_id_t az_id, node_config conf)
    : az_id_(az_id), io_context_(boost::asio::io_context()), strand_(io_context_),
      conf_(std::move(conf)) {}

bool db_client::connect() {
  ptr<tcp::socket> s(new tcp::socket(io_context_));
  tcp::resolver resolver(io_context_);
  boost::system::error_code ec;
  std::string address = conf_.address_public_or_private(az_id_);
  boost::asio::connect(
      *s, resolver.resolve(address, std::to_string(conf_.port())), ec);
  if (ec.failed()) {
    if (not io_context_.stopped()) {
      LOG(error) << "connect ip:" << address << " port:" << conf_.port()
                 << " failed";
    }
    return false;
  } else {
    node_peer peer(conf_.node_id(), address, conf_.port());
    cli_.reset(new client(strand_, s, peer));
    return true;
  }
}