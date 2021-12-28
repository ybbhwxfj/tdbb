#include "raft_test_log_gen.h"
#include "proto/proto.h"
#include "common/json_pretty.h"
#include <boost/filesystem.hpp>
#include <fstream>

void raft_test_log_gen() {
  raft_test_log l;
  uint64_t term = 12;
  log_state *s = l.mutable_state();
  s->set_consistency_index(0);
  s->set_term(term);
  s->set_vote(0);
  s->set_commit_index(0);
  uint64_t num_tx = 2;
  uint64_t num_e = 10;
  for (uint64_t i = 0; i < num_e; i++) {
    uint64_t index = i + 1;
    auto e = l.add_entries();
    e->set_index(index);
    e->set_term(term);
    for (uint64_t j = 0; j < num_tx; j++) {
      auto x = e->add_xlog();
      x->set_log_type(tx_cmd_type::TX_CMD_RM_COMMIT);
      x->set_xid(1000 * (i + 1) + (j + 1));
      x->set_index(index);
    }
  }

  std::string json = pb_to_json(l);
  std::stringstream ssm;
  ssm << pb_to_json(l);
  boost::filesystem::path p(__FILE__);
  p = p.parent_path().append("raft_test.json");
  std::ofstream of(p.c_str());
  json_pretty(ssm, of);
}