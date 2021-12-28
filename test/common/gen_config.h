#pragma once
#include "common/db_type.h"
#include "common/variable.h"
#include "portal/portal.h"
#include "common/block_exception.h"
#include "common/config.h"
#include "common/block_type.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/date_time.hpp>
#include <string>

#define NUM_AZ 3
#define NUM_RG 5

extern std::string global_test_db_path;

const static std::string TMP_DB_PATH = "/tmp/test_db";
const static char *BLOCK_ARRAY[] = {CCB, RLB, DSB};

struct config_option {
  config_option() :
      loose_bind(false),
      num_az(NUM_AZ),
      num_shard(NUM_RG) {}
  bool loose_bind;
  uint32_t num_az;
  uint32_t num_shard;
};
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

  c.get_test_config().set_wan_latency_ms(50);
  c.get_test_config().set_label("test-block-db");
  c.set_schema_path(json_path);

  auto db_path = boost::filesystem::path(path).append(node_name);
  c.set_db_path(db_path.c_str());

  tpcc_config &tc = c.mutable_tpcc_config();
  tc.set_num_warehouse(NUM_WAREHOUSE);
  tc.set_num_order_item(NUM_ORDER_ITEM);
  tc.set_num_district(NUM_DISTRICT);
  tc.set_num_item(NUM_ITEM);
  tc.set_num_order_initalize(NUM_ORDER_INITIALIZE);
  tc.set_num_customer(NUM_CUSTOMER);
  tc.set_num_max_order_line(NUM_MAX_ORDER_LINE);
  tc.set_num_terminal(NUM_TERMINAL);
  tc.set_num_new_order(NUM_NEW_ORDER_TX);
  tc.set_percent_non_exist_item(PERCENT_NON_EXIST_ITEM);
  if (block_db_type() == DB_S) {
    tc.set_percent_distributed(0.0);
  } else {
    tc.set_percent_distributed(PERCENT_DISTRIBUTED);
  }
}

inline std::vector<config> generate_config(const config_option &option) {
  bool loose_bind = option.loose_bind;
  uint32_t num_az = option.num_az;
  uint32_t num_shard = option.num_shard;
  std::vector<config> vec_config;
  std::vector<node_config> nc_list;
  std::map<std::string, uint32_t> az2priority;

  std::string path = global_test_db_path;

  for (uint32_t i = 0; i < num_az; i++) {
    // AZ naming rule must be modified
    // GOTO AZ_NAMING_RULE
    az_id_t az_id = i + 1;
    std::string az_name = "az" + std::to_string(az_id);
    az2priority.insert(std::make_pair(az_name, 0));
    for (uint32_t j = 0; j < num_shard; j++) {
      shard_id_t rg_id = j + 1;
      std::string rg_name = "rg" + std::to_string(rg_id);
      int num = int(sizeof(BLOCK_ARRAY) / sizeof(BLOCK_ARRAY[0]));
      for (int k = 0; k < num; k++) {
        std::string node_name(std::string("node_") + "az" +
            std::to_string(az_id) + "rg" +
            std::to_string(rg_id));
        node_config nc;
        nc.set_address("127.0.0.1");
        nc.set_az_name(az_name);
        nc.set_rg_name(rg_name);

        if (loose_bind) {
          block_type_t bt = BLOCK_ARRAY[k];
          nc.add_node_type(bt);
          std::string name = node_name + boost::algorithm::to_lower_copy(bt);

          nc.set_node_name(name);
          nc.set_port(8000 + az_id + rg_id * 10 + (k + 1) * 100);
          nc_list.push_back(nc);
          config c;
          init_config(c, name, path);
          vec_config.push_back(c);
        } else {
          nc.add_node_type(BLOCK_CCB);
          nc.add_node_type(BLOCK_RLB);
          nc.add_node_type(BLOCK_DSB);
          nc.set_node_name(node_name);
          nc.set_port(8000 + az_id + rg_id * 10);
          nc_list.push_back(nc);
          config c;
          init_config(c, node_name, path);
          vec_config.push_back(c);
          break;
        }
      }
    }
  }
  uint32_t priority = 0;
  for (auto iter = az2priority.begin(); iter != az2priority.end(); iter++) {
    iter->second = ++priority;
  }

  for (node_config &nc: nc_list) {
    nc.set_priority(az2priority[nc.az_name()]);
  }
  for (auto &c: vec_config) {
    std::copy(nc_list.begin(), nc_list.end(),
              std::back_inserter(c.mutable_node_config_list()));
  }

  node_config node_panel_conf;
  {
    node_panel_conf.set_address("127.0.0.1");
    node_panel_conf.set_node_name("panel");
    node_panel_conf.set_rg_name("");
    node_panel_conf.set_port(8100);
    node_panel_conf.set_az_name("az3");
    node_panel_conf.add_node_type(BLOCK_PNB);
    config panel_conf;
    init_config(panel_conf, node_panel_conf.node_name(), path);
    std::copy(nc_list.begin(), nc_list.end(),
              std::back_inserter(panel_conf.mutable_node_config_list()));
    panel_conf.mutable_this_node_config() = node_panel_conf;
    vec_config.push_back(panel_conf);
  }

  node_config node_cli_conf;
  {
    node_cli_conf.set_node_name("client");
    node_cli_conf.set_rg_name("");
    node_cli_conf.set_port(8000);
    node_cli_conf.set_az_name("az3");
    node_cli_conf.add_node_type(BLOCK_CLIENT);
    config cli_conf;
    init_config(cli_conf, node_cli_conf.node_name(), path);
    std::copy(nc_list.begin(), nc_list.end(),
              std::back_inserter(cli_conf.mutable_node_config_list()));
    cli_conf.mutable_this_node_config() = node_cli_conf;
    vec_config.push_back(cli_conf);
  }

  for (auto &c: vec_config) {
    c.mutable_client_config() = node_cli_conf;
    c.panel_config() = node_panel_conf;
    BOOST_ASSERT(c.client_config().node_name() == "client");
    BOOST_ASSERT(c.panel_config().node_name() == "panel");
    c.re_generate_id();
  }
  return vec_config;
}

// the last vector is block client configure file path
inline std::vector<std::string> generate_config_json_file(const config_option &option) {
  std::vector<std::string> json_path;
  std::vector<config> vec_config = generate_config(option);
  for (auto &c: vec_config) {
    boost::json::object j = c.to_json();
    std::stringstream ssm;
    ssm << j;
    std::string json_str = ssm.str();
    boost::filesystem::create_directories(c.db_path());
    std::string file_name =
        boost::filesystem::path(c.db_path()).append("conf.json").c_str();
    json_path.push_back(file_name);
    std::ofstream f(file_name);
    f << json_str;
  }
  return json_path;
}