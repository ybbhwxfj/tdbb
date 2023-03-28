#pragma once
#include "common/callback.h"
#include "common/config.h"
#include "common/ctx_strand.h"
#include "common/define.h"
#include "common/enum_str.h"
#include "common/message.h"
#include "common/panic.h"
#include "common/ptr.hpp"
#include "common/random.h"
#include "common/tx_log.h"
#include "network/net_service.h"
#include "network/sender.h"
#include "proto/proto.h"
#include "replog/log_service.h"
#include <atomic>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <cstdint>
#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#if defined(BOOST_ASIO_ENABLE_HANDLER_TRACKING)
#define use_awaitable                                                          \
  boost::asio::use_awaitable_t(__FILE__, __LINE__, __PRETTY_FUNCTION__)
#endif

#define MAX_RAFT_LOG_ENTRIES 256

enum raft_state {
  RAFT_STATE_LEADER,
  RAFT_STATE_FOLLOWER,
  RAFT_STATE_CANDIDATE,
};

template <> enum_strings<raft_state>::e2s_t enum_strings<raft_state>::enum2str;

typedef std::function<void(uint64_t term)> fn_on_become_leader;
typedef std::function<void(uint64_t term)> fn_on_become_follower;

struct sm_status {
  raft_state state;
  uint64_t term;
  uint32_t lead_node;
};

class state_machine : public ctx_strand,
                      public std::enable_shared_from_this<state_machine> {
  friend class raft_node;

  friend class raft_test_context;

private:
  struct progress {
    progress() { PANIC("progress has no constructor"); }

    progress(uint32_t max)
        : node_id_(0), match_index_(0), next_index_(1), append_log_num_(max),
          send_next_index_(1), last_reject_(false) {}

    node_id_t node_id_;
    log_index_t match_index_;
    log_index_t next_index_;
    uint64_t append_log_num_;
    log_index_t send_next_index_;
    uint64_t last_two_log_index_i_;
    bool last_reject_;
  };

  typedef std::unordered_set<uint32_t> node_set;
  typedef std::unordered_map<uint32_t, std::vector<uint32_t>> voter_logs;
  typedef std::unordered_map<uint32_t, progress> progress_tracer;

  config conf_;
  node_id_t node_id_;
  std::string node_name_;
  uint64_t ts_append_send_;
  std::atomic<uint32_t> tick_count_;
  progress_tracer progress_;

  log_index_t commit_index_;
  uint64_t current_term_;
  raft_state state_;
  std::unordered_map<node_id_t, ptr<pre_vote_response>> pre_vote_responded_;
  node_set votes_responded_;
  node_set votes_granted_;
  bool has_voted_for_;
  node_id_t voted_for_;
  node_id_t leader_id_;
  voter_logs voter_log_;

  std::vector<uint32_t> nodes_ids_;
  std::map<uint64_t, node_id_t> priority_;
  node_id_t priority_replica_node_;
  std::vector<ptr<raft_log_entry>> log_;
  std::unordered_map<uint64_t, ptr<scoped_time>> log_debug_;
  ptr<raft_log_state> log_state_;
  // log index in [1, consistent_log_index_] are written to snapshot
  // [consistent_log_index_ + 1] are in vector
  log_index_t consistent_log_index_;

  ptr<raft_log_entry> null_log_entry_;

  ptr<net_service> sender_;
  ptr<boost::asio::steady_timer> timer_tick_;

  std::random_device rnd_dev_;
  std::mt19937 rnd_;
  std::uniform_int_distribution<std::mt19937::result_type> rnd_dist_;
  uint32_t az_rtt_ms_;
  uint32_t flow_control_rtt_num_;
  uint32_t raft_tick_ms_;

  uint32_t follower_tick_max_;
  uint32_t append_log_entries_batch_max_;
  std::recursive_mutex mutex_;

  fn_on_become_leader fn_on_become_leader_;
  fn_on_become_follower fn_on_become_follower_;
  fn_commit_entries fn_on_commit_entries_;

  ptr<log_service> log_service_;

  std::unordered_map<uint32_t, std::vector<ptr<client>>> clients_;
  std::atomic<bool> stopped_;
  std::deque<repeated_tx_logs> tx_logs_;
  std::chrono::steady_clock::time_point start_;
  boost::asio::io_context::strand log_strand_;

public:
  explicit state_machine(const config &conf, ptr<net_service> sender,
                         fn_on_become_leader fn_on_become_leader,
                         fn_on_become_follower fn_on_become_follower,

                         fn_commit_entries fn_commit, ptr<log_service> ls);

  const std::chrono::steady_clock::time_point &start_time() const {
    return start_;
  }

  result<void> ccb_append_log(const ccb_append_log_request &msg,
                              std::chrono::steady_clock::time_point ts);

  void on_start();

  void on_stop();

  // only inovoked by raft test
  void on_recv_message(message_type id, byte_buffer &buffer);

  sm_status status();

  void handle_debug(const std::string &path, std::ostream &os);

  void node_transfer_leader(node_id_t node_id);

  void handle_request_vote_request(const request_vote_request &request);

  void handle_request_vote_response(const request_vote_response &response);

  void handle_append_entries_request(const append_entries_request &request);

  void handle_append_entries_response(const append_entries_response &response);

  void handle_transfer_leader(const ptr<transfer_leader> msg);

  void handle_transfer_notify(const transfer_notify &msg);

  void after_write_log(uint64_t index);

  void send_append_entries_to_all();

  void write_log(std::vector<ptr<raft_log_entry>> &&logs, fn_ec fn);

  void write_state(ptr<raft_log_state> state, fn_ec fn);

  static log_index_t offset_to_log_index(uint32_t consistent_log_index,
                                         uint64_t offset) {
    return consistent_log_index + offset + 1;
  }

  static uint64_t log_index_to_offset(uint32_t consistent_log_index,
                                      log_index_t index) {
    return index - consistent_log_index - 1;
  }
  bool strand_append_entry(ptr<raft_log_entry> entry);

  void handle_pre_vote_request(const pre_vote_request &request);

  void handle_pre_vote_response(ptr<pre_vote_response> response);

private:
  void leader_send_append_entries();

  result<void> leader_append_entry(ptr<raft_log_entry> entries);

  uint64_t retrieve_log(uint64_t index, uint64_t size);

  void append_entries(progress &tracer);

  result<void> send_append_log(bool is_heart_beat);

  void tick();

  void pre_start();

  uint64_t last_log_index();

  uint64_t last_log_term();

  void on_tick_timeout();

  void timeout_request_vote();

  void pre_vote();

  void request_vote();

  void become_leader();

  void become_follower();

  void leader_advance_commit_index();

  void leader_advance_consistency_index();

  void response_append_entries_response(uint32_t to_node_id,
                                        uint64_t ts_append_send, bool success,
                                        uint64_t match_index, bool heart_beat,
                                        bool write_log);

  void heart_beat();

  void update_term(uint64_t term);

  log_index_t offset_to_log_index(uint64_t offset);

  uint64_t log_index_to_offset(log_index_t index);

  void update_consistency_index(uint64_t consistency_index);

  void update_commit_index(uint64_t commit_index);

  template <typename MESSAGE>
  result<void> async_send(node_id_t to_node_id, message_type mt,
                          ptr<MESSAGE> msg) {

    auto i = clients_.find(to_node_id);
    if (i == clients_.end()) {
      return outcome::failure(EC::EC_NET_CANNOT_FIND_CONNECTION);
    }
#ifdef REPLICATION_MULTIPLE_CHANNEL
    uint64_t n = random_n(i->second.size());
#else
    uint64_t n = 0;
#endif
    if (n > i->second.size()) {
      PANIC("array size error");
    }
    ptr<client> client = i->second[n];

    uint32_t ms = conf_.get_test_config().debug_add_wan_latency_ms();
    if (ms > 0) { // only valid when debug ..
      // delayed send
      auto self = shared_from_this();
      ptr<boost::asio::steady_timer> timer(new boost::asio::steady_timer(
          sender_->get_service(SERVICE_ASYNC_CONTEXT),
          boost::asio::chrono::milliseconds(ms)));
      timer->async_wait([to_node_id, mt, msg, client, timer,
                         self](const boost::system::error_code &error) {
        timer.get();
        if (not error.failed()) {
          self->sender_->template conn_async_send(client, mt, msg);
        }
      });
    } else {
      sender_->template conn_async_send(client, mt, msg);
    }

    return outcome::success();
  }

  void check_log_index();
};