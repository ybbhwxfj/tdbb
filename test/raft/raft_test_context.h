#pragma once

#include "raft_node.h"
#include <thread>
#include <unordered_map>
#include <vector>
#define FINISH_FLAG "20220926"

class raft_test_context {
public:
  raft_test_context(uint64_t max_log_entries)
      : checked_index_(0), max_log_entries_(max_log_entries), stop_(false) {}

  bool check_at_most_one_leader_per_term(node_id_t node_id, uint64_t term,
                                         bool lead);
  bool check_max_committed_logs(node_id_t node_id);
  bool check_commit_entries(node_id_t node_id, bool lead,
                            const std::vector<ptr<raft_log_entry>> &entries);
  void wait_finish();
  void stop_and_join();
  void commit_log(node_id_t node_id, log_index_t index);
  std::map<node_id_t, uint64_t> commit_xid_;
  std::set<az_id_t> az_id_set_;
  uint64_t checked_index_;
  std::unordered_map<node_id_t, ptr<raft_node>> nodes_;
  std::unordered_map<node_id_t, bool> is_lead_node_;
  std::unordered_map<node_id_t, uint64_t> node_term_;
  uint64_t max_log_entries_;
  std::unordered_map<node_id_t, std::unordered_set<log_index_t>>
      committed_log_index_;
  std::mutex mutex_;
  bool stop_;
};
