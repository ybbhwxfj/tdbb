#pragma once

#include <cstdint>
#include <boost/enable_shared_from_this.hpp>
#include <memory>
#include <utility>
#include "common/block.h"
#include "network/sender.h"
#include "raft/state_machine.h"
#include "proto/proto.h"
#include "common/config.h"
#include "network/net_service.h"
#include "common/callback.h"
#include "replog/log_service_impl.h"
#include "common/msg_time.h"

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
  uint32_t ccb_node_id_;
  uint32_t dsb_node_id_;
  uint32_t rg_id_;
  ptr<state_machine> state_machine_;
  ptr<log_service_impl> log_service_;
  std::map<node_id_t, bool> ccb_responsed_;
  ptr<boost::asio::steady_timer> timer_send_report_;
  std::recursive_mutex mutex_;
  std::chrono::steady_clock::time_point start_;
  msg_time time_;
 public:
  rl_block(const config &conf, ptr<net_service> service,
           fn_become_leader f_become_leader,
           fn_become_follower f_become_follower,
           fn_commit_entries f_commit_entries
  )
      : conf_(conf),
        service_(service),
        fn_become_leader_(std::move(f_become_leader)),
        fn_become_follower_(std::move(f_become_follower)),
        commit_entries_(std::move(f_commit_entries)),
        cno_(0),
        node_id_(conf.node_id()),
        node_name_(id_2_name(conf.node_id())),
        ccb_node_id_(0),
        dsb_node_id_(0),
        rg_id_(conf.rg_id()),
        time_("RLB handle") {
    log_service_ = cs_new<log_service_impl>(conf, service_);

    fn_on_become_leader fn_bl = [this](uint64_t term) {
      on_become_leader(term);
    };
    fn_on_become_follower fn_bf = [this](uint64_t term) {
      on_become_follower(term);
    };

    fn_commit_entries fn_commit = [this](EC ec, bool is_lead, const std::vector<ptr<log_entry>> &logs) {
      on_commit_entries(ec, is_lead, logs);
      log_service_->commit_log(logs, log_write_option());
    };

    state_machine_ = cs_new<state_machine>(conf, service, fn_bl, fn_bf,
                                           fn_commit,
                                           log_service_
    );

    start_ = state_machine_->start_time();
  }

  ~rl_block() override {
  }

  virtual void on_start() override;

  virtual void on_stop() override;

  virtual void handle_debug(const std::string &path, std::ostream &os) override;

  template<typename T>
  result<void> handle_message(const ptr<connection> &c, message_type t, const ptr<T> m) {
    auto conn = c;
    // run routine in state_machine's strand
    auto shared = this->shared_from_this();
    auto fn = [shared, conn, t, m] {
      auto r = shared->rlb_handle_message(conn, t, m);
      if (not r) {

      }
    };
    boost::asio::post(state_machine_->get_strand(), fn);
    return outcome::success();
  }

 private:
  template<typename T>
  result<void> rlb_handle_message(const ptr<connection> &, message_type, const T &) {
    BOOST_ASSERT(false); // unknown message...
    return outcome::success();
  }

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<ccb_register_ccb_request>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<dsb_register_dsb_request>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<ccb_append_log_request>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<ccb_report_status_response>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<request_vote_request>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<request_vote_response>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<append_entries_request>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<append_entries_response>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<transfer_leader>);

  result<void> rlb_handle_message(const ptr<connection> &, message_type, const ptr<transfer_notify>);

  void handle_append_entries_response(const append_entries_response &response);

  void handle_transfer_leader(const transfer_leader &msg);

  void handle_transfer_notify(const transfer_notify &msg);

  void handle_register_ccb(const ccb_register_ccb_request &req);

  void handle_register_dsb(const dsb_register_dsb_request &dsb);

  void handle_append_entries(const ccb_append_log_request &);

  void handle_report_status_response(const ccb_report_status_response &);

  uint64_t cno() { return cno_; }

  void on_become_leader(uint64_t term);

  void on_become_follower(uint64_t term);

  void send_report(bool lead);

  void on_commit_entries(EC ec, bool is_lead, const std::vector<ptr<log_entry>> &logs);

  void response_commit_log(EC ec, const std::vector<ptr<log_entry>> &logs);

  // when recovery/rester, retrieve all logs
  void response_ccb_register_with_logs(node_id_t node);
};
