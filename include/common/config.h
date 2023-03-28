#pragma once

#include "common/block_config.h"
#include "common/id.h"
#include "common/node_config.h"
#include "common/result.hpp"
#include "common/schema_mgr.h"
#include "common/test_config.h"
#include "common/tpcc_config.h"
#include <boost/json.hpp>
#include <fstream>
#include <iostream>
#include <list>
#include <map>
#include <unordered_set>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <vector>

class config {
private:
  node_config node_conf_;
  tpcc_config tpcc_config_;
  block_config block_config_;
  std::vector<node_config> node_client_list_;
  // the order of node_config in each node would be consistent
  std::vector<node_config> node_server_list_;
  test_config test_config_;
  std::map<az_id_t, std::map<shard_id_t, std::vector<node_config>>> az2rg2node_;
  std::map<shard_id_t, std::map<az_id_t, std::vector<node_config>>> rg2az2node_;
  std::map<shard_id_t, std::map<uint32_t, std::vector<node_id_t>>> priority_;
  std::map<node_id_t, node_config> node2conf_;
  uint32_t num_rep_group_;
  uint32_t num_replica_;

  std::unordered_map<shard_id_t, node_id_t> ccb_shard_;
  std::unordered_map<shard_id_t, node_id_t> dsb_shard_;
  std::unordered_map<shard_id_t, node_id_t> rlb_shard_;
  bool mapping_;
  schema_mgr schema_mgr_;
  node_id_t register_to_rlb_node_id;

public:
  config();

  void set_node_name(const std::string &name) {
    block_config_.set_node_name(name);
  }

  void set_schema_path(const std::string &path) {
    block_config_.set_schema_path(path);
  }

  void set_register_node(const std::string &name) {
    block_config_.set_register_node(name);
  }

  void set_threads_io(uint64_t n) { block_config_.set_threads_io(n); }

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

  uint32_t register_to_node_id() const { return register_to_rlb_node_id; }

  az_id_t az_id() const { return node_conf_.az_id(); }

  std::vector<shard_id_t> shard_ids() const { return node_conf_.shard_ids(); }

  const std::string &db_path() const { return block_config_.db_path(); };

  const std::string &node_name() const { return block_config_.node_name(); }

  const node_config &this_node_config() const { return node_conf_; }

  node_config &mutable_this_node_config() { return node_conf_; }

  const tpcc_config &get_tpcc_config() const { return tpcc_config_; }

  const block_config &get_block_config() const { return block_config_; }

  block_config &get_block_config() { return block_config_; }

  const test_config &get_test_config() const { return test_config_; }

  test_config &get_test_config() { return test_config_; }

  tpcc_config &mutable_tpcc_config() { return tpcc_config_; };

  const std::vector<node_config> &node_client_list() const { return node_client_list_; }

  std::vector<node_config> &mutable_client_config() { return node_client_list_; };

  void const_cast_generate_mapping() const;

  uint32_t num_terminal() const { return get_tpcc_config().num_terminal(); }

  uint32_t final_num_terminal() const {
    if (tpcc_config_.additional_read_only_terminal()) {
      return num_terminal() + additional_terminal();
    } else {
      return num_terminal();
    }
  }

  uint32_t additional_terminal() const {
    if (tpcc_config_.additional_read_only_terminal()) {
      return tpcc_config_.num_terminal() * tpcc_config_.percent_read_only();
    } else {
      return 0;
    }
  }

  void generate_mapping();

  uint32_t num_rg() const;

  uint32_t num_az() const;

  std::vector<shard_id_t> all_shard_ids() const;

  node_config get_node_conf(node_id_t id);

  std::vector<node_id_t> get_rg_nodes(shard_id_t sd_id) const;

  std::vector<node_id_t> get_rg_block_nodes(shard_id_t sd_id,
                                            block_type_id_t block_type) const;

  uint32_t get_largest_priority_node_of_shard(shard_id_t shard) const;

  const std::vector<node_config> &node_server_list() const;

  std::vector<node_config> &mutable_node_server_list();

  void re_generate_id();

  boost::json::object to_json();

  void from_json(boost::json::object j);

  result<void> from_json_string(const std::string &json_string);

  std::string node_debug_name();

  bool valid_check();

  std::unordered_map<shard_id_t, node_id_t> priority_lead_nodes() const;

  const std::unordered_map<shard_id_t, node_id_t> &dsb_shards() const;

  const std::unordered_map<shard_id_t, node_id_t> &ccb_shards() const;

  const std::unordered_map<shard_id_t, node_id_t> &rlb_shards() const;
private:

  // set1 \subset set2
  static bool sub_set_of(const std::vector<shard_id_t> &set1, const std::vector<shard_id_t> &set2) {
    for (auto i : set1) {
      uint64_t n = 0;
      for (auto j : set2) {
        if (j==i) { // i in set2
          break;
        } else {
          n++;
        }
      }
      if (n==set2.size()) {
        // i not in set2
        return false;
      }
    }
    return true;
  }
};