#pragma once
#include "common/block_exception.h"
#include "common/block_type.h"
#include "common/config.h"
#include "common/db_type.h"
#include "common/variable.h"
#include "common/config_option.h"
#include "portal/portal.h"
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/date_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include "common/panic.h"

extern std::string global_test_db_path;

const static std::string TMP_DB_PATH = "test_db";

inline std::string schema_json_path() {
  boost::filesystem::path p(__FILE__);
  // block-db/test/portal/__FILE__
  // project path
  return p.parent_path()
      .parent_path()
      .parent_path()
      .append("conf/tpcc_schema.json")
      .c_str();
}

inline void init_config(config &c, std::string node_name, std::string path) {
  c.set_node_name(node_name);

  c.set_register_node(node_name);
  std::string json_path = schema_json_path();

  c.get_test_config().set_wan_latency_ms(TEST_WAN_LATENCY_MS);
  c.get_test_config().set_cached_tuple_percentage(TEST_CACHED_TUPLE_PERCENTAGE);
  c.get_test_config().set_label("test-block-db");
  c.set_schema_path(json_path);

  auto db_path = boost::filesystem::path(path).append(node_name);
  c.set_db_path(db_path.c_str());

  tpcc_config &tc = c.mutable_tpcc_config();
  tc.set_num_warehouse(NUM_WAREHOUSE);
  tc.set_num_district_per_warehouse(NUM_DISTRICT_PER_WAREHOUSE);
  tc.set_num_item(NUM_ITEM);
  tc.set_num_order_initialize(NUM_ORDER_INITIALIZE);
  tc.set_num_customer_per_district(NUM_CUSTOMER_PER_DISTRICT);
  tc.set_num_max_order_line(NUM_MAX_ORDER_LINE);
  tc.set_num_terminal(NUM_TERMINAL);
  tc.set_num_new_order(NUM_TRANSACTION);
  tc.set_percent_hot_row(PERCENT_HOT_ROW);
  tc.set_hot_item_num(HOT_ROW_NUM);
  tc.set_percent_non_exist_item(PERCENT_NON_EXIST_ITEM);
  if (block_db_type() == DB_S) {
    tc.set_percent_remote_wh(0.2);
  } else {
    tc.set_percent_remote_wh(PERCENT_REMOTE);
  }
}

inline std::string to_node_name(
    az_id_t az,
    std::vector<shard_id_t> shard_ids,
    std::vector<block_type_t> block_types) {
  std::stringstream ssm;
  ssm << "node_";
  ssm << "az_" << az << "_" << "sd";
  for (shard_id_t id : shard_ids) {
    ssm << "_" << id;
  }
  ssm << "_bt";
  for (block_type_t bt : block_types) {
    ssm << "_" << boost::algorithm::to_lower_copy(bt);
  }
  return ssm.str();
}

inline std::pair<std::vector<config>, std::vector<config>>
generate_config(const config_option &option) {
  bool has_priority = option.priority;
  uint32_t num_az = option.num_az;
  BOOST_ASSERT(option.binding.size() > 0);
  std::vector<config> server_config;
  std::vector<node_config> server_list;
  std::map<std::string, uint32_t> az2priority;

  std::string path = global_test_db_path;

  for (uint32_t h = 0; h < num_az; h++) {
    // AZ naming rule must be modified
    // GOTO AZ_NAMING_RULE
    az_id_t az_id = h + 1;
    std::string az_name = "az" + std::to_string(az_id);
    az2priority.insert(std::make_pair(az_name, 0));
    for (size_t i = 0; i < option.binding.size(); i++) {
      auto bind = option.binding[i];
      std::vector<std::string> block_types;

      for (auto block : bind) {
        block_types.push_back(block);
      }
      BOOST_ASSERT(bind.size() > 0);
      uint32_t shard_id = 0;
      uint32_t num_shards = option.get_num_shard(bind[0]);
      uint32_t max_shard = option.get_max_num_shard();
      uint32_t num_shard_per_group = max_shard / num_shards;
      for (uint32_t j = 0; j < num_shards;) {
        std::vector<std::string> shard_names;
        std::vector<shard_id_t> shard_ids;
        node_config nc;
        nc.set_address("127.0.0.1");
        nc.set_az_name(az_name);
        for (uint32_t k = 0; k < num_shard_per_group; k++, j++) {
          shard_id += 1;
          std::string rg_name = "sd_id" + std::to_string(shard_id);
          nc.add_rg_name(rg_name);
          shard_ids.push_back(shard_id);
        }
        std::string node_name = to_node_name(az_id, shard_ids, block_types);
        nc.set_node_name(node_name);
        nc.set_port(8000 + az_id + shard_ids[0] * 10 + (i + 1) * 100);
        nc.set_repl_port(7000 + az_id + shard_ids[0] * 10 + (i + 1) * 100);
        for (auto b : block_types) {
          nc.add_node_type(b);
        }
        server_list.push_back(nc);
        config c;
        init_config(c, node_name, path);
        server_config.push_back(c);
      }
    }
  }
  BOOST_ASSERT(server_list.size() > 0);
  uint32_t priority = 0;
  for (auto iter = az2priority.begin(); iter != az2priority.end(); iter++) {
    if (has_priority) {
      ++priority;
    }
    iter->second = priority;
  }

  for (node_config &nc : server_list) {
    nc.set_priority(az2priority[nc.az_name()]);
  }

  for (auto &c : server_config) {
    std::copy(server_list.begin(), server_list.end(),
              std::back_inserter(c.mutable_node_server_list()));
  }

  std::vector<node_config> client_list;
  std::vector<config> client_config;
  for (uint32_t i = 0; i < option.num_client; i++) {
    uint32_t id = i + 1;
    node_config node_cli_conf;
    {
      node_cli_conf.set_node_name((boost::format("client_%d") % id).str());
      node_cli_conf.add_rg_name((boost::format("id%d") % id).str());
      node_cli_conf.set_port(9000 + id);
      node_cli_conf.set_repl_port(10100 + id);
      node_cli_conf.set_az_name("az3");
      node_cli_conf.add_node_type(BLOCK_CLIENT);
      client_list.push_back(node_cli_conf);
    }
  }

  for (config &c : server_config) {
    std::copy(client_list.begin(), client_list.end(),
              std::back_inserter(c.mutable_client_config()));
  }
  for (config &c : server_config) {
    c.re_generate_id();
  }

  for (const node_config &c : client_list) {
    config cli_conf;
    init_config(cli_conf, c.node_name(), path);
    cli_conf.mutable_this_node_config() = c;
    std::copy(server_list.begin(), server_list.end(),
              std::back_inserter(cli_conf.mutable_node_server_list()));
    std::copy(client_list.begin(), client_list.end(),
              std::back_inserter(cli_conf.mutable_client_config()));
    client_config.push_back(cli_conf);
    BOOST_ASSERT(cli_conf.node_server_list().size() > 0);
  }

  for (auto &c : client_config) {
    c.re_generate_id();
  }
  return std::make_pair(client_config, server_config);
}

// pair::first client configure
// pair::second server configure
inline std::pair<std::vector<std::string>, std::vector<std::string>>
generate_config_json_file(const config_option &option) {
  std::vector<std::string> json_path;
  std::pair<std::vector<config>, std::vector<config>> cs_config =
      generate_config(option);
  std::vector<std::string> client_path;
  std::vector<std::string> server_path;
  std::vector<std::vector<config> *> vec;
  vec.push_back(&cs_config.first);
  vec.push_back(&cs_config.second);
  for (auto ptr : vec) {
    for (auto &c : *ptr) { // for each server
      boost::json::object j = c.to_json();
      std::stringstream ssm;
      ssm << j;
      std::string json_str = ssm.str();
      boost::filesystem::create_directories(c.db_path());
      std::string file_name =
          boost::filesystem::path(c.db_path()).append("conf.json").c_str();
      if (ptr == &cs_config.first) {
        client_path.push_back(file_name);
      } else {
        server_path.push_back(file_name);
      }
      std::ofstream f(file_name);
      f << json_str;
    }
  }
  return std::make_pair(client_path, server_path);
}