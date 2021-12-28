#pragma once

#include "common/id.h"
#include <boost/json.hpp>
#include <boost/assert.hpp>
#include <set>
#include <map>
#include "common/block_type.h"

inline block_type_id_t node_type_name_2_id(block_type_t name) {

  auto iter = __node_type_name_2_id.find(name);
  if (iter == __node_type_name_2_id.end()) {
    BOOST_ASSERT(__node_type_name_2_id.contains(name));
    return block_type_id_t(0);
  } else {
    return iter->second;
  }
}

class node_config {
private:
  uint32_t node_id_;
  //uint32_t register_to_node_id_;
  uint32_t az_id_;
  uint32_t rg_id_;
  std::string node_name_;
  // the node name of the RLB to register

  std::string az_name_;
  std::string rg_name_;
  std::string address_;
  uint32_t port_;
  uint32_t priority_;
  std::set<block_type_t> block_type_;
public:
  node_config() :
      node_id_(0),
      az_id_(0),
      rg_id_(0),
      port_(0),
      priority_(0) {}

  bool invalid() const { return node_id_ == 0; }
  void set_invalid() { node_id_ = 0; }
  node_id_t node_id() const { return node_id_; }
  //node_id_t register_to_node_id() const { return register_to_node_id_; }
  az_id_t az_id() const { return az_id_; }
  shard_id_t rg_id() const { return rg_id_; }
  uint32_t port() const { return port_; }
  uint32_t priority() const { return priority_; }
  const std::string &node_name() const { return node_name_; }
  const std::string &az_name() const { return az_name_; }
  const std::string &rg_name() const { return rg_name_; }
  const std::string &address() const { return address_; }

  //const std::string &register_to() const { return register_to_name_node_; }
  const std::set<block_type_t> &block_type_list() const {
    BOOST_ASSERT(block_type_.size() > 0);
    return block_type_;
  }
  void set_priority(uint32_t v) { priority_ = v; }
  void set_node_id(node_id_t v) {
    BOOST_ASSERT(v != 0);
    node_id_ = v;
  }
  //void set_register_to_node_id(node_id_t v) { register_to_node_id_ = v; }
  void set_az_id(az_id_t v) { az_id_ = v; }
  void set_rg_id(shard_id_t v) { rg_id_ = v; }
  void set_port(uint32_t v) { port_ = v; }
  void set_node_name(const std::string &v) { node_name_ = v; }
  void set_az_name(const std::string &v) { az_name_ = v; }
  void set_rg_name(const std::string &v) { rg_name_ = v; }
  void set_address(const std::string &v) { address_ = v; }
  void add_node_type(const block_type_t &type) { block_type_.insert(type); }

  std::string node_peer() const {
    return address() + ":" + std::to_string(port());
  }
  bool is_client() const {
    for (const block_type_t &type: block_type_) {
      if (type == BLOCK_CLIENT) {
        BOOST_ASSERT(block_type_.size() == 1);
        return true;
      }
    }
    return false;
  }
  //void set_register_to(const std::string &name) { register_to_name_node_ = name; }
  boost::json::object to_json() const {
    boost::json::object j;
    //j["node_id_"] = node_id_;
    //j["zone_id"] = az_id_;
    //j["shard_id"] = rg_id_;
    j["node_name"] = node_name_;
    j["zone_name"] = az_name_;
    j["shard_name"] = rg_name_;
    j["address"] = address_;
    j["port"] = port_;
    j["priority"] = priority_;
    //j["register_to"] = register_to_name_node_;
    boost::json::array node_type_array;
    for (auto i: block_type_) {
      node_type_array.emplace_back(i);
    }
    j["block_type"] = node_type_array;
    return j;
  }

  void from_json(boost::json::object &j) {
    //node_id_ = (uint32_t)boost::json::value_to<int64_t>(j["node_id_"]);
    //az_id_ = (uint32_t)boost::json::value_to<int64_t>(j["zone_id"]);
    //rg_id_ = (uint32_t)boost::json::value_to<int64_t>(j["shard_id"]);
    node_name_ = boost::json::value_to<std::string>(j["node_name"]).c_str();
    az_name_ = boost::json::value_to<std::string>(j["zone_name"]).c_str();
    rg_name_ = boost::json::value_to<std::string>(j["shard_name"]).c_str();
    address_ = boost::json::value_to<std::string>(j["address"]).c_str();
    port_ = (uint32_t)boost::json::value_to<int64_t>(j["port"]);
    if (j.contains("priority")) {
      priority_ = (uint32_t)boost::json::value_to<int64_t>(j["priority"]);
    }
    //register_to_name_node_ = boost::json::value_to<std::string>(j["register_to"]).c_str();
    boost::json::array node_type_array = j["block_type"].as_array();
    for (auto i: node_type_array) {
      block_type_t t = i.as_string().c_str();
      block_type_.insert(t);
    }
  }
};