#pragma once

#include <thread>
#include <unordered_map>
#include <vector>
#include "raft_node.h"

class raft_test_context {
public:

  raft_test_context() : checked_index_(0) {}

  bool check_at_most_one_leader_per_term(node_id_t node_id, uint64_t term, bool lead);
  bool check_max_committed_logs(node_id_t node_id);
  bool check_commit_entries(node_id_t node_id, bool lead, const std::vector<ptr<log_entry>> &entries);
  void wait_tx_commit(uint64_t index);
  void stop_and_join();

  std::map<node_id_t, uint64_t> commit_xid_;
  std::set<az_id_t> az_id_set_;
  uint64_t checked_index_;
  std::unordered_map<node_id_t, ptr<raft_node>> nodes_;
  std::unordered_map<node_id_t, bool> is_lead_node_;
  std::unordered_map<node_id_t, uint64_t> node_term_;
  std::mutex mutex_;
};
