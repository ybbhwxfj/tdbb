#include "raft_test_context.h"

bool raft_test_context::check_max_committed_logs(node_id_t node_id) {
  std::scoped_lock l(mutex_);
  std::map<node_id_t, uint64_t> max_committed_log_index;
  for (const auto &n : nodes_) {
    if (not n.second->log_.empty()) {
      max_committed_log_index[n.first] = n.second->log_.rbegin()->first;
    } else {
      max_committed_log_index[n.first] = 0;
    }
  }
  auto i = max_committed_log_index.find(node_id);
  if (i == max_committed_log_index.end()) {
    return false;
  }
  uint64_t committed_index = i->second;
  uint32_t num_le = 0;
  for (const auto &kv : max_committed_log_index) {
    if (kv.second <= committed_index) {
      num_le++;
    }
  }
  return num_le * 2 > nodes_.size();
}

bool raft_test_context::check_at_most_one_leader_per_term(node_id_t node_id, uint64_t term, bool is_lead) {
  std::scoped_lock l(mutex_);
  is_lead_node_[node_id] = is_lead;
  node_term_[node_id] = term;
  if (is_lead_node_.size() == nodes_.size()) {
    uint32_t lead = 0;
    uint32_t follower = 0;
    for (auto kv : is_lead_node_) {
      lead += kv.second ? 1 : 0;
      follower += kv.second ? 1 : 0;
      if (term != node_term_[kv.first]) {
        // if not the same term, do not check
        return true;
      }
    }
    is_lead_node_.clear();
    if (lead == 1) {
      return true;
    } else {
      return false;
    }

  } else {
    // if have unknown state node, do not check
    return true;
  }
}

bool raft_test_context::check_commit_entries(node_id_t node_id,
                                             bool,
                                             const std::vector<ptr<log_entry>> &entries) {
  if (entries.empty()) {
    return true;
  }
  for (auto &e : entries) {
    uint64_t index = e->index();
    for (auto &l : e->xlog()) {
      xid_t x = l.xid();
      if (commit_xid_[node_id] < x) {
        commit_xid_[node_id] = x;
      }
    }
    if (checked_index_ < e->index()) {
      uint32_t num_committed = 0;
      std::map<std::string, std::string> message;
      for (auto p : nodes_) {
        std::scoped_lock lock(p.second->mutex_);
        auto node = p.second;
        bool commit = false;
        auto i = node->log_.find(index);
        if (i == node->log_.end()) {
          commit = false;
          message[id_2_name(node->node_id_)] = "no such log";
        } else {
          auto log_entry = node->log_[index];
          if (log_entry->index() != index) {
            commit = false;
            message[id_2_name(node->node_id_)] = "non consistent index";
          } else if (log_entry->term() != e->term()) {
            message[id_2_name(node->node_id_)] = "non consistent term";
          }
          commit = log_entry->index() == index && log_entry->term() == e->term();
        }

        num_committed += commit ? 1 : 0;
      }
      if (num_committed * 2 <= nodes_.size()) {
        // not majority
        return false;
      }
    }
    checked_index_ = index;
  }
  return true;
}

void raft_test_context::wait_tx_commit(uint64_t index) {
  while (true) {
    uint64_t n = 0;
    for (const auto &kv : commit_xid_) {
      if (kv.second == index) {
        n++;
      }
    }
    if (n == commit_xid_.size()) {
      break;
    }
    sleep(1);
  }
}

void raft_test_context::stop_and_join() {
  for (const auto &n : nodes_) {
    n.second->stop();
  }
  for (const auto &n : nodes_) {
    n.second->join();
  }
}