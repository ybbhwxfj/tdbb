#include "raft_test_log_gen.h"
#include "common/json_pretty.h"
#include "common/tx_log.h"
#include "proto/proto.h"
#include "raft_test_context.h"
#include <boost/filesystem.hpp>
#include <fstream>

void raft_test_log_gen() {
  raft_test_log l;
  uint64_t term = 12;
  raft_log_state *s = l.mutable_state();
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

      if (i + 1 == num_e && j + 1 == num_tx) {
        *e->mutable_repeated_tx_logs() = FINISH_FLAG;
      }
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