#pragma once

#include "common/config.h"
#include "common/callback.h"
#include "common/message.h"
#include "common/enum_str.h"
#include "common/ptr.hpp"
#include "network/net_service.h"
#include "network/sender.h"
#include "proto/proto.h"
#include "replog/log_service.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#define MAX_RAFT_LOG_ENTRIES 256

enum raft_state {
  RAFT_STATE_LEADER,
  RAFT_STATE_FOLLOWER,
  RAFT_STATE_CANDIDATE,
};

template<>
enum_strings<raft_state>::e2s_t enum_strings<raft_state>::enum2str;

typedef std::function<void(uint64_t term)> fn_on_become_leader;
typedef std::function<void(uint64_t term)> fn_on_become_follower;

struct sm_status {
  raft_state state;
  uint64_t term;
  uint32_t lead_node;
};

class state_machine {
  friend class raft_node;
  friend class raft_test_context;
private:
  struct progress {
    progress() :
        node_id_(0),
        match_index_(0),
        next_index_(1),
        append_log_num_(MAX_RAFT_LOG_ENTRIES) {}
    node_id_t node_id_;
    log_index_t match_index_;
    log_index_t next_index_;
    uint64_t append_log_num_;
  };

  typedef std::unordered_set<uint32_t> node_set;
  typedef std::unordered_map<uint32_t, std::vector<uint32_t>> voter_logs;
  typedef std::unordered_map<uint32_t, progress> progress_tracer;
  config conf_;
  node_id_t node_id_;
  std::string node_name_;
  uint64_t tick_sequence_;
  std::atomic<uint32_t> tick_count_;
  progress_tracer progress_;

  log_index_t commit_index_;
  uint64_t current_term_;
  raft_state state_;
  node_set votes_responded_;
  node_set votes_granted_;
  bool has_voted_for_;
  node_id_t voted_for_;
  node_id_t leader_id_;
  voter_logs voter_log_;

  std::vector<uint32_t> nodes_ids_;
  std::map<uint64_t, node_id_t> priority_;
  node_id_t priority_replica_node_;
  std::vector<ptr<log_entry>> log_;
  ptr<log_state> log_state_;
  // log index in [1, consistent_log_index_] are write to snapshot [consistent_log_index_ + 1] are in vector
  log_index_t consistent_log_index_;

  ptr<log_entry> null_log_entry_;

  net_service *sender_;
  ptr<boost::asio::steady_timer> timer_tick_;

  std::random_device rnd_dev_;
  std::mt19937 rnd_;
  std::uniform_int_distribution<std::mt19937::result_type> rnd_dist_;
  uint32_t az_rtt_ms_;
  uint32_t flow_control_rtt_num_;
  uint32_t tick_miliseconds_;
  uint32_t follower_tick_max_;
  std::recursive_mutex mutex_;
  shard_id_t rg_id_;
  fn_on_become_leader fn_on_become_leader_;
  fn_on_become_follower fn_on_become_follower_;
  fn_commit_entries fn_on_commit_entries_;

  ptr<log_service> log_service_;

  std::unordered_map<uint32_t, ptr<client>> clients_;
  std::atomic<bool> stopped_;
  std::deque<tx_log> tx_logs_;
  boost::posix_time::ptime start_;
public:
  explicit state_machine(const config &conf,
                net_service *sender,
                fn_on_become_leader fn_on_become_leader,
                fn_on_become_follower fn_on_become_follower,

                fn_commit_entries fn_commit,
                ptr<log_service> ls
  );

  const boost::posix_time::ptime & start_time() const {
    return start_;
  }
  result<void> ccb_append_log(const ccb_append_log_request &msg);

  result<void> append_entry(const ptr<log_entry> &entry);

  void on_start();

  void on_stop();
  void on_recv_message(message_type id, byte_buffer &buffer);

  sm_status status();

  void handle_debug(const std::string &path, std::ostream &os);

  void node_transfer_leader(node_id_t node_id);

  void handle_request_vote_request(const request_vote_request &request);

  void handle_request_vote_response(const request_vote_response &response);

  void handle_append_entries_request(const append_entries_request &request);

  void handle_append_entries_response(const append_entries_response &response);

  void handle_transfer_leader(const transfer_leader &msg);

  void handle_transfer_notify(const transfer_notify &msg);

  static log_index_t offset_to_log_index(uint32_t consistent_log_index, uint64_t offset) {
    return consistent_log_index + offset + 1;
  }

  static uint64_t log_index_to_offset(uint32_t consistent_log_index, log_index_t index) {
    return index - consistent_log_index - 1;
  }

private:
  uint64_t retrieve_log(uint64_t index, uint64_t size);
  void append_entries(progress &tracer);
  result<void> send_append_log();

  void tick();
  void pre_start();
  uint64_t last_log_index();
  uint64_t last_log_term();
  void on_tick_timeout();

  void timeout_request_vote();

  void request_vote();

  void become_leader();

  void become_follower();

  void leader_advance_commit_index();

  void leader_advance_consistency_index();

  void resonse_append_entries_response(uint32_t to_node_id,
                                       uint64_t tick_sequence,
                                       bool success,
                                       uint64_t match_index,
                                       bool heart_beat
  );

  void heart_beat();

  void update_term(uint64_t term);

  log_index_t offset_to_log_index(uint64_t offset);
  uint64_t log_index_to_offset(log_index_t index);

  void update_consistency_index(uint64_t consistency_index);

  void update_commit_index(uint64_t commit_index);

  template<typename MESSAGE>
  result<void> async_send(const ptr<client> &c, message_type mt, MESSAGE &msg) {
    auto r = c->async_send(mt, msg);
    if (!r) {
      if (r.error().code() == EC::EC_NET_UNCONNECTED) {
        sender_->async_client_connect(c);
      }
    }
    return r;
  }

  template<typename MESSAGE>
  result<void> async_send(node_id_t to_node_id, message_type mt, MESSAGE &msg) {
    auto i = clients_.find(to_node_id);
    if (i == clients_.end()) {
      return outcome::failure(EC::EC_NET_CANNOT_FIND_CONNECTION);
    }
    auto r = i->second->async_send(mt, msg);
    if (!r) {
      if (r.error().code() == EC::EC_NET_UNCONNECTED) {
        sender_->async_client_connect(i->second);
      }
    }
    return r;
  }

  void check_log_index();
};