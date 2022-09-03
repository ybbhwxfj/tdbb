#pragma once

#include "common/id.h"
#include "common/node_config.h"
#include "common/result.hpp"
#include "common/tpcc_config.h"
#include "common/block_config.h"
#include "common/test_config.h"
#include "common/schema_mgr.h"
#include <boost/json.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <list>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

class config {
 private:
  node_config node_conf_;
  tpcc_config tpcc_config_;
  block_config block_config_;
  node_config client_config_;
  node_config panel_config_;
  std::vector<node_config> node_config_list_;
  test_config test_config_;
  std::map<az_id_t, std::map<shard_id_t, std::vector<node_id_t>>> az2rg2node_;
  std::map<shard_id_t, std::map<az_id_t, std::vector<node_id_t>>> rg2az2node_;
  std::map<shard_id_t, std::map<uint32_t, std::vector<node_id_t>>> priority_;
  std::map<node_id_t, node_config> node2conf_;
  uint32_t num_rep_group_;
  uint32_t num_replica_;
  bool mapping_;
  schema_mgr schema_mgr_;
  node_id_t register_node_id_;
 public:
  config() : num_rep_group_(0), num_replica_(0), mapping_(false), register_node_id_(0) {}

  void set_node_name(const std::string &name) {
    block_config_.set_node_name(name);
  }

  void set_schema_path(const std::string &path) {
    block_config_.set_schema_path(path);
  }

  void set_register_node(const std::string &name) {
    block_config_.set_register_node(name);
  }

  void set_threads_io(uint64_t n) {
    block_config_.set_threads_io(n);
  }

  void set_threads_async_context(uint64_t n) {
    block_config_.set_threads_async_context(n);
  }

  const std::string &get_schema_path() const {
    return block_config_.schema_path();
  }

  const schema_mgr &schema_manager() const { return schema_mgr_; }

  schema_mgr &schema_manager() { return schema_mgr_; }

  void set_db_path(const std::string &path) { block_config_.set_db_path(path); }

  const std::string &register_node() const {
    return block_config_.register_node();
  }

  uint32_t node_id() const { return node_conf_.node_id(); }

  uint32_t register_to_node_id() const { return register_node_id_; }

  az_id_t az_id() const { return node_conf_.az_id(); }

  shard_id_t rg_id() const { return node_conf_.rg_id(); }

  const std::string &db_path() const { return block_config_.db_path(); };

  const std::string &node_name() const { return block_config_.node_name(); }

  const node_config &this_node_config() const { return node_conf_; }

  const node_config &panel_config() const { return panel_config_; }

  node_config &panel_config() { return panel_config_; }

  node_config &mutable_this_node_config() { return node_conf_; }

  const tpcc_config &get_tpcc_config() const { return tpcc_config_; }

  const block_config &get_block_config() const { return block_config_; }

  block_config &get_block_config() { return block_config_; }

  const test_config &get_test_config() const { return test_config_; }

  test_config &get_test_config() { return test_config_; }

  tpcc_config &mutable_tpcc_config() { return tpcc_config_; };

  const node_config &client_config() const { return client_config_; }

  node_config &mutable_client_config() { return client_config_; };

  void const_cast_generate_mapping() const;

  uint32_t num_terminal() const { return get_tpcc_config().num_terminal(); }

  void generate_mapping();

  uint32_t num_rg() const;

  uint32_t num_az() const;

  std::set<shard_id_t> shard_id_set() const;

  node_config get_node_conf(node_id_t id);

  std::vector<node_id_t> get_rg_nodes(shard_id_t rg_id) const;

  std::vector<node_id_t> get_rg_block_nodes(shard_id_t rg_id, block_type_id_t block_type) const;

  uint32_t get_largest_priority_node_of_shard(shard_id_t shard) const;

  const std::vector<node_config> &node_config_list() const;

  std::vector<node_config> &mutable_node_config_list();

  void re_generate_id();

  boost::json::object to_json();

  void from_json(boost::json::object j);

  result<void> from_json_string(const std::string &json_string);

  std::string node_debug_name();

  bool valid_check();

 private:
};