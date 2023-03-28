#pragma once

#include "common/block.h"
#include "common/callback.h"
#include "common/config.h"
#include "common/msg_time.h"
#include "common/tx_log.h"
#include "network/net_service.h"
#include "network/sender.h"
#include "proto/proto.h"
#include "raft/state_machine.h"
#include "replog/log_service_impl.h"
#include <boost/enable_shared_from_this.hpp>
#include <boost/format.hpp>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

class rl_block : public block, public std::enable_shared_from_this<rl_block> {
private:
  config conf_;
  ptr<net_service> service_;
  fn_become_leader fn_become_leader_;
  fn_become_follower fn_become_follower_;
  fn_commit_entries commit_entries_;
  uint64_t cno_;
  uint32_t node_id_;
  std::string node_name_;
  std::unordered_map<shard_id_t, node_id_t> ccb_shards_;
  std::unordered_map<shard_id_t, node_id_t> dsb_shards_;
  std::unordered_set<node_id_t, bool> ccb_nodes_;
  std::unordered_set<node_id_t, bool> dsb_nodes_;
  std::optional<uint32_t> ccb_node_id_;
  std::optional<uint32_t> dsb_node_id_;
  uint32_t rg_id_;
  ptr<state_machine> state_machine_;
  ptr<log_service_impl> log_service_;
  std::map<node_id_t, bool> ccb_responsed_;
  ptr<boost::asio::steady_timer> timer_send_report_;
  boost::asio::io_context::strand rlb_strand_;
  std::chrono::steady_clock::time_point start_;
  msg_time time_;

public:
  rl_block(const config &conf, ptr<net_service> service,
           fn_become_leader f_become_leader,
           fn_become_follower f_become_follower,
           fn_commit_entries f_commit_entries);

  ~rl_block() override {}

  virtual void on_start() override;

  virtual void on_stop() override;

  virtual void handle_debug(const std::string &path, std::ostream &os) override;

  template <typename T>
  result<void> handle_message(const ptr<connection> c, message_type t,
                              const ptr<T> m) {
    auto conn = c;
    auto ts = std::chrono::steady_clock::now();
    // run routine in state_machine's strand
    auto shared = this->shared_from_this();
    auto fn = [shared, conn, t, m, ts] {
      scoped_time _t(
          (boost::format("rl_block handle message: %s") % enum2str(t)).str());
      switch (t) {
      case message_type::C2R_APPEND_LOG_REQ: {
        auto msg = static_pointer_cast<ccb_append_log_request>(
            ptr<google::protobuf::Message>(m));
#ifdef TEST_APPEND_TIME
        {
          uint64_t begin = msg->debug_send_ts();
          uint64_t ms = steady_clock_ms_since_epoch() - begin;
          if (ms > APPEND_MS_MAX) {
            LOG(info) << "APPEND_LOG_REQ message " << ms;
          }
        }
#endif
        shared->handle_append_entries(*msg, ts);
        break;
      }
      default: {
        auto r = shared->rlb_handle_message(conn, t, m);
        if (not r) {
        }
        break;
      }
      }
    };
    boost::asio::post(state_machine_->get_strand(), fn);
    return outcome::success();
  }

private:
  template <typename T>
  result<void> rlb_handle_message(const ptr<connection>, message_type type,
                                  const T &) {
    LOG(error) << "handle message " << enum2str(type) << " not implement";
    BOOST_ASSERT(false); // unknown message...
    return outcome::success();
  }

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<ccb_register_ccb_request>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<dsb_register_dsb_request>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<ccb_report_status_response>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<request_vote_request>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<request_vote_response>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<append_entries_request>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<append_entries_response>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<transfer_leader>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<transfer_notify>);

  result<void> rlb_handle_message(const ptr<connection>, message_type,
                                  const ptr<replay_to_dsb_response>);

  void handle_append_entries_response(const append_entries_response &response);

  void handle_transfer_leader(const transfer_leader &msg);

  void handle_transfer_notify(const transfer_notify &msg);

  void handle_register_ccb(const ccb_register_ccb_request &req);

  void handle_register_dsb(const dsb_register_dsb_request &dsb);

  void handle_append_entries(const ccb_append_log_request &req,
                             std::chrono::steady_clock::time_point ts);

  void handle_report_status_response(const ccb_report_status_response &);

  uint64_t cno() { return cno_; }

  void on_become_leader(uint64_t term);

  void on_become_follower(uint64_t term);

  void rlb_strand_send_report(bool lead);

  void on_commit_entries(EC ec, bool is_lead,
                         const std::vector<ptr<raft_log_entry>> &logs);

  void response_commit_log(EC ec, const std::vector<ptr<raft_log_entry>> &logs);

  // when recovery/rester, retrieve all logs
  void response_ccb_register_with_logs(node_id_t) {};

  void send_error_consistency(node_id_t node_id, message_type mt);
};
