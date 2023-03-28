#include "common/config.h"
#include "common/logger.hpp"
#include "common/panic.h"

config::config()
    : num_rep_group_(0), num_replica_(0),
      mapping_(false),
      register_to_rlb_node_id(0) {

}

void config::const_cast_generate_mapping() const {
  const_cast<config *>(this)->generate_mapping();
}

void config::generate_mapping() {
  if (mapping_) {
    return;
  }
  re_generate_id();
  std::map<shard_id_t, std::map<node_id_t, uint32_t>> priority;
  for (auto nc : node_server_list_) {
    if (nc.is_client()) {
      continue;
    }
    for (auto shard_id : nc.shard_ids()) {
      az2rg2node_[nc.az_id()][shard_id].push_back(nc);
    }
    for (auto shard_id : nc.shard_ids()) {
      rg2az2node_[shard_id][nc.az_id()].push_back(nc);
    }
    node2conf_[nc.node_id()] = nc;
    for (auto shard_id : nc.shard_ids()) {
      priority[shard_id][nc.node_id()] = nc.priority();
    }
  }
  for (auto kv : priority) {
    shard_id_t sid = kv.first;
    for (auto p : kv.second) {
      node_id_t node_id = p.first;
      uint32_t p_no = p.second;
      priority_[sid][p_no].push_back(node_id);
    }
  }

  std::unordered_map<shard_id_t, node_id_t> ccb_shard;
  std::unordered_map<shard_id_t, node_id_t> dsb_shard;
  std::unordered_map<shard_id_t, node_id_t> rlb_shard;
  for (auto p : az2rg2node_[this_node_config().az_id()]) {
    for (auto &node : p.second) {
      if (node.block_type_list().contains(BLOCK_CCB)) {
        ccb_shard.insert(std::make_pair(p.first, node.node_id()));
      }
      if (node.block_type_list().contains(BLOCK_RLB)) {
        rlb_shard.insert(std::make_pair(p.first, node.node_id()));
      }
      if (node.block_type_list().contains(BLOCK_DSB)) {
        dsb_shard.insert(std::make_pair(p.first, node.node_id()));
      }
    }
  }

  ccb_shard_ = ccb_shard;
  dsb_shard_ = dsb_shard;
  rlb_shard_ = rlb_shard;

  num_replica_ = az2rg2node_.size();
  num_rep_group_ = rg2az2node_.size();
  mapping_ = true;
  BOOST_ASSERT(num_replica_ > 0);
  BOOST_ASSERT(num_rep_group_ > 0);
}

uint32_t config::num_rg() const {
  const_cast_generate_mapping();
  return num_rep_group_;
}

std::vector<shard_id_t> config::all_shard_ids() const {
  std::set<shard_id_t> set;
  const_cast_generate_mapping();
  for (auto i = rg2az2node_.begin(); i != rg2az2node_.end(); i++) {
    set.insert(i->first);
  }
  return std::vector<shard_id_t>(set.begin(), set.end());
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

std::vector<node_id_t> config::get_rg_nodes(shard_id_t sd_id) const {
  std::vector<node_id_t> vec;
  const_cast_generate_mapping();
  auto i = rg2az2node_.find(sd_id);
  if (i != rg2az2node_.end()) {
    for (auto kv : i->second) {
      for (auto &node : kv.second) {
        BOOST_ASSERT(std::find(node.shard_ids().begin(), node.shard_ids().end(), sd_id) != node.shard_ids().end());
        vec.push_back(node.node_id());
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
    PANIC("cannot find node");
  }
  if (i->second.size() == 0) {
    PANIC("node set empty");
  }

  for (auto nid : i->second.rbegin()->second) {
    if (is_ccb_block(nid)) {
      return nid;
    }
  }
  PANIC("no CCB node");
}

std::vector<node_id_t>
config::get_rg_block_nodes(shard_id_t sd_id, block_type_id_t block_type) const {
  std::vector<node_id_t> ret;
  std::vector<node_id_t> vec = get_rg_nodes(sd_id);
  for (node_id_t i : vec) {
    if (is_block_type(i, block_type)) {
      ret.push_back(i);
    }
  }
  return ret;
}

const std::vector<node_config> &config::node_server_list() const {
  return node_server_list_;
}
std::vector<node_config> &config::mutable_node_server_list() {
  return node_server_list_;
}

void config::re_generate_id() {

  std::set<std::string> az_name;
  std::set<std::string> shard_name;
  std::map<std::string, az_id_t> az_name_2_id;
  std::map<std::string, shard_id_t> shard_name_2_id;

  for (node_config &n : node_server_list_) {
    for (auto s : n.rg_name()) {
      shard_name.insert(s);
    }
    az_name.insert(n.az_name());
  }
  {
    uint32_t next_az_id = 1;
    for (auto s : az_name) {
      az_name_2_id.insert(std::make_pair(s, next_az_id));
      next_az_id++;
    }
  }
  {
    uint32_t next_rg_id = 1;
    for (auto s : shard_name) {
      shard_name_2_id.insert(std::make_pair(s, next_rg_id));
      next_rg_id++;
    }
  }

  for (node_config &c : node_server_list_) { // set shard ids, az id
    if (!az_name_2_id.contains(c.az_name())) {
      PANIC("could not find az");
    }
    az_id_t az_id = az_name_2_id[c.az_name()];
    c.set_az_id(az_id);
    for (const auto &s : c.rg_name()) {
      if (!shard_name_2_id.contains(s)) {
        PANIC("could not find shard");
      }
      shard_id_t id = shard_name_2_id[s];
      c.append_rg_id(id);
    }
  }
  for (node_config &c : node_server_list_) { // set node id
    az_id_t az_id = c.az_id();
    BOOST_ASSERT(az_id != 0);
    shard_id_t sd_id = c.shard_ids()[0];
    node_id_t node_id = MAKE_NODE_ID(az_id, sd_id, c.block_type_list());
    c.set_node_id(node_id);
    BOOST_ASSERT(c.node_id() != 0);
  }

  node_config *this_node = nullptr;
  for (node_config &c : node_server_list_) { // set this node
    if (c.node_name() == node_name()) {
      this_node = &c;
      break;
    }
  }
  if (this_node) { // this node is not null
    for (node_config &c : node_server_list_) { // set this node
      if (this_node->az_name() == c.az_name() && sub_set_of(this_node->shard_ids(), c.shard_ids()))
        for (block_type_t bt : c.block_type_list()) {
          if (bt == BLOCK_RLB) { // the node is a RLB node
            if (register_to_rlb_node_id != 0) {
              PANIC("register to RLB node error");
            }
            register_to_rlb_node_id = c.node_id();
          }
        }
    }
    BOOST_ASSERT(register_to_rlb_node_id != 0);
  } else { // only when this node is client
    for (uint32_t i = 0; i < node_client_list_.size(); i++) {
      node_config &client = node_client_list_[i];
      if (client.node_name() == node_name()) {
        if (this_node) {
          PANIC("client node name error");
        }
        this_node = &client;
      }
    }
  }
  if (this_node == nullptr) {
    PANIC("node name error");
  }

  for (uint32_t i = 0; i < node_client_list_.size(); i++) {
    uint32_t client_id = i + 1;
    node_config &client = node_client_list_[i];
    {
      az_id_t az_id = az_name_2_id[client.az_name()];
      client.set_az_id(az_id);
      client.append_rg_id(client_id);
      node_id_t node_id =
          MAKE_NODE_ID(az_id, client_id, client.block_type_list());
      client.set_node_id(node_id);
    }
  }
  node_conf_ = *this_node;
  if (node_conf_.node_id() == 0) {
    PANIC("node conf setting error");
  }
}

boost::json::object config::to_json() {
  boost::json::object j;
  j["param"] = tpcc_config_.to_json();
  j["block"] = block_config_.to_json();
  j["test"] = test_config_.to_json();

  boost::json::array server_list;
  for (auto nc : node_server_list_) {
    server_list.emplace_back(nc.to_json());
  }

  boost::json::array client_list;
  for (auto c : node_client_list_) {
    client_list.emplace_back(c.to_json());
  }
  j["node_client_list"] = client_list;
  j["node_server_list"] = server_list;
  return j;
}

void config::from_json(boost::json::object j) {
  tpcc_config_.from_json(j["param"].as_object());

  boost::json::object &jblock = j["block"].as_object();
  block_config_.from_json(jblock);

  test_config_.from_json(j["test"].as_object());
  boost::json::array &jc = j["node_client_list"].as_array();
  for (auto i = jc.begin(); i != jc.end(); i++) {
    node_client_list_.emplace_back(node_config());
    node_client_list_.rbegin()->from_json(i->as_object());
  }

  boost::json::array &ja = j["node_server_list"].as_array();
  for (auto i = ja.begin(); i != ja.end(); i++) {
    node_server_list_.emplace_back(node_config());
    node_server_list_.rbegin()->from_json(i->as_object());
  }
  BOOST_ASSERT(not node_server_list_.empty());

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
      LOG(error) << "Parsing failed: " << ec.message();
      return outcome::failure(EC::EC_CONFIG_ERROR);
    }

    from_json(jv.as_object());
    if (not valid_check()) {
      return outcome::failure(EC::EC_CONFIG_ERROR);
    }
    return outcome::success();
  } catch (std::exception const &e) {
    LOG(error) << "Parsing failed: " << e.what();
    return outcome::failure(EC::EC_CONFIG_ERROR);
  }
}

std::string config::node_debug_name() {
  return id_2_name(node_conf_.node_id());
}

bool config::valid_check() {
  if (tpcc_config_.num_order_initialize_per_district() *
      tpcc_config_.num_district_per_warehouse() *
      tpcc_config_.num_warehouse() +
      tpcc_config_.num_new_order() >
      NUM_ORDER_MAX) {
    LOG(error) << "order id overflow";
    return false;
  }
  return true;
}

std::unordered_map<shard_id_t, node_id_t> config::priority_lead_nodes() const {
  std::unordered_map<shard_id_t, node_id_t> map;
  const_cast_generate_mapping();
  for (auto i = priority_.begin(); i != priority_.end(); ++i) {
    if (i->second.size() == 0) {
      return map;
    }
    for (auto nid : i->second.rbegin()->second) {
      if (is_ccb_block(nid)) {
        map.insert(std::make_pair(i->first, nid));
      }
    }
  }
  if (priority_.size() == map.size()) {
    return map;
  } else {
    map.clear();
    return map;
  }
}

const std::unordered_map<shard_id_t, node_id_t> &config::dsb_shards() const {
  const_cast_generate_mapping();
  return dsb_shard_;
}

const std::unordered_map<shard_id_t, node_id_t> &config::ccb_shards() const {
  const_cast_generate_mapping();
  return ccb_shard_;
}

const std::unordered_map<shard_id_t, node_id_t> &config::rlb_shards() const {
  const_cast_generate_mapping();
  return rlb_shard_;
}