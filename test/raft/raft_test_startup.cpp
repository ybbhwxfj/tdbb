#include "raft_test.h"
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>

ptr<raft_test_context> raft_test_startup(const std::string &test_case_name) {
  // boost::log::add_file_log("/tmp/test_db/block.log");
  boost::log::core::get()->set_filter(boost::log::trivial::severity >=
      boost::log::trivial::debug);
  config_option option;
  option.num_az = 3;
  option.num_shard = 1;
  std::vector<config> confs = generate_config(option);
  //config client = confs.back();
  confs.pop_back(); // erase client configuration
  ptr<raft_test_context> ctx(new raft_test_context());
  for (auto &c : confs) { // do not use panel block in raft test
    c.panel_config().set_invalid();
  }
  std::set<az_id_t> az_ids;
  for (const auto &c : confs) {
    if (c.this_node_config().block_type_list().contains(BLOCK_CCB)) {
      ptr<raft_node> n(new raft_node(c, test_case_name));
      ctx->nodes_.insert(std::make_pair(c.node_id(), n));
      az_ids.insert(c.az_id());
    }
  }
  ctx->az_id_set_ = az_ids;

  for (const auto &n : ctx->nodes_) {
    n.second->start(ctx.get());
  }
  return ctx;
}