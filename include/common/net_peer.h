#pragma once

#include <string>

class net_peer {
public:
  net_peer() {}

  net_peer(uint32_t _id, std::string _addr, uint32_t _port)
      : id_(_id), addr_(_addr), port_(_port) {}

  uint32_t node_id() const { return id_; };

  const std::string &name() const { return name_; }

  uint32_t port() const { return port_; }

  const std::string &addr() const { return addr_; }

private:
  uint32_t id_;
  std::string name_;
  uint32_t port_;
  std::string addr_;
};