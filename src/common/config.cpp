#include "common/config.h"
#include <boost/log/trivial.hpp>

void config::const_cast_generate_mapping() const {
  const_cast<config *>(this)->generate_mapping();
}

void config::generate_mapping() {
  if (mapping_) {
    return;
  }
  re_generate_id();
  std::map<shard_id_t, std::map<node_id_t, uint32_t>> priority;
  for (auto nc : node_config_list_) {
    if (nc.is_client()) {
      continue;
    }
    az2rg2node_[nc.az_id()][nc.rg_id()].push_back(nc.node_id());
    rg2az2node_[nc.rg_id()][nc.az_id()].push_back(nc.node_id());
    node2conf_[nc.node_id()] = nc;
    priority[nc.rg_id()][nc.node_id()] = nc.priority();
  }
  for (auto kv : priority) {
    shard_id_t sid = kv.first;
    for (auto p : kv.second) {
      node_id_t node_id = p.first;
      uint32_t p_no = p.second;
      priority_[sid][p_no].push_back(node_id);
    }
  }
  num_replica_ = az2rg2node_.size();
  num_rep_group_ = rg2az2node_.size();
  mapping_ = true;
  BOOST_ASSERT(num_replica_ > 0);
  BOOST_ASSERT(num_rep_group_ > 0);
}

uint32_t config::num_rg() const {
  if (num_rep_group_ == 0) {
    const_cast_generate_mapping();
    return num_rep_group_;
  } else {
    return num_rep_group_;
  }
}

std::set<shard_id_t> config::shard_id_set() const {
  std::set<shard_id_t> set;
  const_cast_generate_mapping();
  for (auto i = rg2az2node_.begin(); i != rg2az2node_.end(); i++) {
    set.insert(i->first);
  }
  return set;
}

uint32_t config::num_az() const {
  if (num_replica_ == 0) {
    const_cast_generate_mapping();
    return num_replica_;
  } else {
    return num_replica_;
  }
}

node_config config::get_node_conf(node_id_t id) {
  const_cast_generate_mapping();

  auto i = node2conf_.find(id);
  if (i != node2conf_.end()) {
    BOOST_ASSERT(i->second.node_id() != 0);
    return i->second;
  }
  BOOST_ASSERT(false);
  return node_config();
}

std::vector<node_id_t> config::get_rg_nodes(shard_id_t rg_id) const {
  std::vector<node_id_t> vec;
  const_cast_generate_mapping();
  auto i = rg2az2node_.find(rg_id);
  if (i != rg2az2node_.end()) {
    for (auto kv : i->second) {
      for (node_id_t id : kv.second) {
        BOOST_ASSERT(rg_id == TO_RG_ID(id));
        vec.push_back(id);
      }

    }
  }
  if (!vec.empty()) {
    std::random_device rd;
    std::default_random_engine rng{rd()};
    std::shuffle(std::begin(vec), std::end(vec), rng);
  }
  return vec;
}

node_id_t config::get_largest_priority_node_of_shard(shard_id_t shard) const {
  const_cast_generate_mapping();
  auto i = priority_.find(shard);
  if (i == priority_.end()) {
    return 0;
  }
  if (i->second.size() == 0) {
    return 0;
  }

  for (auto nid : i->second.rbegin()->second) {
    if (is_ccb_block(nid)) {
      return nid;
    }
  }
  return 0;
}

std::vector<node_id_t> config::get_rg_block_nodes(shard_id_t rg_id, block_type_id_t block_type) const {
  std::vector<node_id_t> ret;
  std::vector<node_id_t> vec = get_rg_nodes(rg_id);
  for (node_id_t i : vec) {
    if (is_block_type(i, block_type)) {
      ret.push_back(i);
    }
  }
  return ret;
}

const std::vector<node_config> &config::node_config_list() const {
  return node_config_list_;
}
std::vector<node_config> &config::mutable_node_config_list() {
  return node_config_list_;
}

void config::re_generate_id() {
  typedef std::set<block_type_t> block_type_set;

  std::map<std::string, std::map<std::string, std::map<block_type_set, node_config *>>> rg2az2bt2node;
  std::map<std::string, node_config *> name2node;
  for (node_config &c : node_config_list_) {
    if (c.is_client()) {
      continue;
    }
    rg2az2bt2node[c.rg_name()][c.az_name()].insert(std::make_pair(c.block_type_list(), &c));
    name2node[c.node_name()] = &c;
  }

  uint32_t rg_id = 1;
  std::map<std::string, az_id_t> az2id;
  node_config *this_node = nullptr;
  std::string nname = node_name();
  std::vector<node_config *> register_rlb;
  for (auto &rg2 : rg2az2bt2node) {
    uint32_t az_id = 1;
    for (auto &az2 : rg2.second) {
      for (auto &bt2 : az2.second) {
        node_config *c = bt2.second;
        az2id.insert(std::make_pair(c->az_name(), az_id));
        c->set_az_id(az_id);
        c->set_rg_id(rg_id);
        node_id_t node_id = MAKE_NODE_ID(az_id, rg_id, c->block_type_list());
        c->set_node_id(node_id);

        for (block_type_t bt : bt2.first) {
          if (bt == BLOCK_RLB) { // the node is a RLB node
            register_rlb.push_back(c);
          }
        }
        if (c->node_name() == nname) { // this node configure
          this_node = c;
        }
      }

      az_id++;
    }
    rg_id++;
  }
  if (nname != client_config_.node_name() && nname != panel_config_.node_name()) {
    BOOST_ASSERT(this_node != nullptr);
    node_conf_ = *this_node;
    for (node_config *c : register_rlb) {
      if (c->rg_name() == this_node->rg_name() && c->az_name() == this_node->az_name()) {
        register_node_id_ = c->node_id();
        BOOST_ASSERT(is_rlb_block(register_node_id_));
      }
    }
    BOOST_ASSERT(register_node_id_ != 0);
  }
  {
    az_id_t az_id = az2id[client_config_.az_name()];
    shard_id_t sd_id = 0;
    client_config_.set_az_id(az_id);
    client_config_.set_rg_id(sd_id);
    node_id_t node_id = MAKE_NODE_ID(az_id, rg_id, client_config_.block_type_list());
    client_config_.set_node_id(node_id);
  }
  {
    az_id_t az_id = az2id[panel_config_.az_name()];
    shard_id_t sd_id = 0;
    panel_config_.set_az_id(az_id);
    panel_config_.set_rg_id(sd_id);
    node_id_t node_id = MAKE_NODE_ID(az_id, rg_id, panel_config_.block_type_list());
    panel_config_.set_node_id(node_id);
  }
  if (block_config_.node_name() == client_config_.node_name()) {
    node_conf_ = client_config_;
  } else if (block_config_.node_name() == panel_config_.node_name()) {
    node_conf_ = panel_config_;
  }
}

boost::json::object config::to_json() {
  boost::json::object j;
  j["param"] = tpcc_config_.to_json();
  j["block"] = block_config_.to_json();
  j["test"] = test_config_.to_json();

  boost::json::array nc_list;
  for (auto nc : node_config_list_) {
    if (nc.is_client()) {
      continue;
    }
    nc_list.emplace_back(nc.to_json());
  }
  j["node_client"] = client_config_.to_json();

  j["node_panel"] = panel_config_.to_json();

  j["node_server_list"] = nc_list;
  return j;
}

void config::from_json(boost::json::object j) {
  tpcc_config_.from_json(j["param"].as_object());

  boost::json::object &jblock = j["block"].as_object();
  block_config_.from_json(jblock);

  test_config_.from_json(j["test"].as_object());
  boost::json::object &jc = j["node_client"].as_object();
  client_config_.from_json(jc);

  boost::json::object &jp = j["node_panel"].as_object();
  panel_config_.from_json(jp);

  boost::json::array &ja = j["node_server_list"].as_array();
  for (auto i = ja.begin(); i != ja.end(); i++) {
    node_config_list_.emplace_back(node_config());
    node_config_list_.rbegin()->from_json(i->as_object());
  }
  BOOST_ASSERT(not node_config_list_.empty());

  std::ifstream fsm(block_config_.schema_path());
  std::stringstream ssm;
  ssm << fsm.rdbuf();
  if (not schema_mgr_.from_json_string(ssm.str())) {
    BOOST_ASSERT(false);
    return;
  }

  generate_mapping();
}

result<void> config::from_json_string(const std::string &json_string) {
  try {
    boost::system::error_code ec;
    boost::json::value jv = boost::json::parse(json_string, ec);

    if (ec) {
      BOOST_LOG_TRIVIAL(error) << "Parsing failed: " << ec.message();
      return outcome::failure(EC::EC_CONFIG_ERROR);
    }

    from_json(jv.as_object());
    if (not valid_check()) {
      return outcome::failure(EC::EC_CONFIG_ERROR);
    }
    return outcome::success();
  } catch (std::exception const &e) {
    BOOST_LOG_TRIVIAL(error) << "Parsing failed: " << e.what();
    return outcome::failure(EC::EC_CONFIG_ERROR);
  }
}

std::string config::node_debug_name() {
  return id_2_name(node_conf_.node_id());
}

bool config::valid_check() {
  if (tpcc_config_.num_order_initialize_per_district() *
      tpcc_config_.num_district_per_warehouse() *
      tpcc_config_.num_warehouse() + tpcc_config_.num_new_order() > NUM_ORDER_MAX) {
    BOOST_LOG_TRIVIAL(error) << "order id overflow";
    return false;
  }
  return true;
}