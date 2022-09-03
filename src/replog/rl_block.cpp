#include "replog/rl_block.h"
#include "common/debug_url.h"

void rl_block::handle_debug(const std::string &path, std::ostream &os) {
  if (not boost::regex_match(path, url_json_prefix)) {
    os << BLOCK_RLB << " name:" << node_name_ << std::endl;
    os << "endpoint:" << conf_.this_node_config().node_peer() << std::endl;
    os << "register_to:" << id_2_name(conf_.register_to_node_id()) << std::endl;
    os << "path:" << path << std::endl;
  }

  state_machine_->handle_debug(path, os);
}

void rl_block::on_start() {
  state_machine_->on_start();
  log_service_->on_start();
  BOOST_LOG_TRIVIAL(info) << "start up RLB " << node_name_ << " ...";
}

void rl_block::on_stop() {
  state_machine_->on_stop();
  log_service_->on_stop();
  // time_.print();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &,
                                          message_type,
                                          const ptr<ccb_register_ccb_request> m) {
  handle_register_ccb(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &,
                                          message_type,
                                          const ptr<dsb_register_dsb_request> m) {
  handle_register_dsb(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &, message_type, const ptr<ccb_append_log_request> m) {
  handle_append_entries(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &,
                                          message_type,
                                          const ptr<ccb_report_status_response> m) {
  handle_report_status_response(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &, message_type, const ptr<request_vote_request> m) {
  state_machine_->handle_request_vote_request(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &, message_type, const ptr<request_vote_response> m) {
  state_machine_->handle_request_vote_response(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &, message_type, const ptr<append_entries_request> m) {
  state_machine_->handle_append_entries_request(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &, message_type, const ptr<append_entries_response> m) {
  state_machine_->handle_append_entries_response(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &, message_type, const ptr<transfer_leader> m) {
  state_machine_->handle_transfer_leader(m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection> &, message_type, const ptr<transfer_notify> m) {
  state_machine_->handle_transfer_notify(*m);
  return outcome::success();
}

void rl_block::handle_register_ccb(const ccb_register_ccb_request &req) {
  sm_status s = state_machine_->status();
  node_id_t node_id = req.source();
  if (s.term == 0 || dsb_node_id_ == 0) {
    return;
  }
  if (s.state != RAFT_STATE_LEADER && s.state != RAFT_STATE_FOLLOWER) {
    return;
  }
  if (ccb_node_id_ != req.source()) {
    ccb_node_id_ = req.source();
  }

  if (req.cno() != cno_ || ccb_node_id_ != node_id) {
    service_->get_service(SERVICE_ASYNC_CONTEXT).dispatch([this, node_id]() {
      BOOST_LOG_TRIVIAL(trace) << node_name_ << " receive register_ccb request first time";
      response_ccb_register_with_logs(node_id);
    });
  } else {
    BOOST_LOG_TRIVIAL(trace) << node_name_ << " receive register_ccb request";
    auto res = std::make_shared<rlb_register_ccb_response>();
    res->set_cno(s.term);
    res->set_source(node_id_);
    res->set_dest(req.source());
    res->set_lead_node(s.lead_node);
    res->set_dsb_node(dsb_node_id_);
    res->set_is_lead(s.state == RAFT_STATE_LEADER);
    if (s.term != req.cno()) {

    }
    auto r = service_->async_send(req.source(),
                                  R2C_REGISTER_RESP, res);
    if (!r) {
      BOOST_LOG_TRIVIAL(error) << "send register_ccb response";
    }
  }
}

void rl_block::handle_register_dsb(const dsb_register_dsb_request &dsb) {
  sm_status s = state_machine_->status();
  if (s.term != 0 && (s.state == RAFT_STATE_LEADER || s.state == RAFT_STATE_FOLLOWER)) {
    if (dsb_node_id_ != dsb.source()) {
      dsb_node_id_ = dsb.source();
      log_service_->set_dsb_id(dsb_node_id_);
    }

    auto res = std::make_shared<rlb_register_dsb_response>();
    res->set_cno(s.term);
    res->set_source(node_id_);
    res->set_dest(dsb.source());
    res->set_lead(s.state == RAFT_STATE_LEADER);
    auto r = service_->async_send(dsb.source(),
                                  R2D_REGISTER_RESP, res);
    if (!r) {
      BOOST_LOG_TRIVIAL(error) << "send register_dsb response";
    }
  }
}

void rl_block::handle_append_entries(const ccb_append_log_request
                                     &request) {
  auto r = state_machine_->ccb_append_log(request);
  if (not r) {
    BOOST_LOG_TRIVIAL(error) << "ccb append log error";
  }
}

void rl_block::on_become_leader(uint64_t term) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  if (fn_become_leader_) {
    fn_become_leader_(term);
  }
  cno_ = term;
  BOOST_LOG_TRIVIAL(info) << node_name_ << " on become leader";
  //ccb_responsed_[ccb_node_id_] = false;
  //send_report(true);

  auto msg = std::make_shared<panel_report>();
  msg->set_source(conf_.node_id());
  msg->set_dest(conf_.panel_config().node_id());
  msg->set_lead(true);
  msg->set_report_type(RLB_NEW_TERM);
  auto rs = service_->async_send(msg->dest(), message_type::PANEL_REPORT, msg);
  if (not rs) {
    BOOST_LOG_TRIVIAL(error) << "send panel info report become leader error ";
  }
}

void rl_block::on_become_follower(uint64_t term) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  cno_ = term;
  if (fn_become_follower_) {
    fn_become_follower_(term);
  }
  BOOST_LOG_TRIVIAL(info) << node_name_ << " on become follower";
  //ccb_responsed_[ccb_node_id_] = false;
  //send_report(false);
  auto msg = std::make_shared<panel_report>();
  msg->set_source(conf_.node_id());
  msg->set_dest(conf_.panel_config().node_id());
  msg->set_lead(false);
  msg->set_report_type(RLB_NEW_TERM);
  auto rs = service_->async_send(msg->dest(), message_type::PANEL_REPORT, msg);
  if (not rs) {
    BOOST_LOG_TRIVIAL(error) << "send panel info report become follower error ";
  }
}

void rl_block::send_report(bool lead) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  auto iter = ccb_responsed_.find(ccb_node_id_);
  if (iter != ccb_responsed_.end() && iter->second) {
    return;
  }
  if (ccb_node_id_ == 0) {
    return;
  }
  ccb_responsed_[ccb_node_id_] = false;
  BOOST_LOG_TRIVIAL(info) << node_name_ << " send report status to CCB " << id_2_name(ccb_node_id_);
  auto msg = std::make_shared<rlb_report_status_to_ccb>();
  msg->set_dest(ccb_node_id_);
  msg->set_source(node_id_);
  msg->set_cno(cno_);
  msg->set_rg_id(rg_id_);
  msg->set_lead(lead);

  auto r = service_->async_send(ccb_node_id_, R2C_REPORT_STATUS_REQ, msg);
  if (!r) {

  }
  timer_send_report_.reset(new boost::asio::steady_timer(
      service_->get_service(SERVICE_ASYNC_CONTEXT),
      boost::asio::chrono::milliseconds(2000)));
  auto fn_timeout = [this, lead](const boost::system::error_code &) {
    send_report(lead);
  };
  timer_send_report_->async_wait(fn_timeout);
}

void rl_block::handle_report_status_response(const ccb_report_status_response &res) {
  auto iter = ccb_responsed_.find(res.source());
  if (iter != ccb_responsed_.end()) {
    BOOST_LOG_TRIVIAL(info) << node_name_ << " handle report status response CCB " << id_2_name(ccb_node_id_);
    iter->second = true;
  }
}

void rl_block::on_commit_entries(EC ec, bool is_lead, const std::vector<ptr<log_entry>> &logs) {
  if (commit_entries_) {
    commit_entries_(EC::EC_OK, is_lead, logs);
  }
  if (ec == EC_OK) {
    if (is_lead) {
      response_commit_log(EC::EC_OK, logs);
    } else {
      //BOOST_LOG_TRIVIAL(error) << node_name_ << " not lead node";
    }
  } else {
    if (is_lead) {
      response_commit_log(ec, logs);
    }
  }
}

void rl_block::response_commit_log(EC ec, const std::vector<ptr<log_entry>> &logs) {
  if (logs.empty()) {
    return;
  }
  auto msg = std::make_shared<rlb_commit_entries>();
  msg->set_error_code(ec);
  msg->set_dest(ccb_node_id_);
  msg->set_source(node_id_);
  auto now = std::chrono::steady_clock::now();
  uint64_t current_us = to_microseconds(now - start_);
  for (const ptr<log_entry> &log : logs) {
    for (const tx_log &xlog : log->xlog()) {
      tx_log *x = msg->add_logs();
      x->set_xid(xlog.xid());
      x->set_log_type(xlog.log_type());
      auto start_us = xlog.repl_latency();
      auto latency = current_us - start_us;
      x->set_repl_latency(latency);
    }
  }

  uint32_t ms = conf_.get_test_config().debug_add_wan_latency_ms();
  if (ms > 0) { // only valid when debug ..
    ptr<boost::asio::steady_timer> timer(
        new boost::asio::steady_timer(
            service_->get_service(SERVICE_ASYNC_CONTEXT),
            boost::asio::chrono::milliseconds(ms)));
    timer->async_wait([msg, timer, this](const boost::system::error_code &error) {
      timer.get();
      if (not error.failed()) {
        auto r = service_->async_send(ccb_node_id_, COMMIT_LOG_ENTRIES, msg);
        if (!r) {
          BOOST_LOG_TRIVIAL(error) << "async send commit log entries error";
        }
      }
    });
  } else {
    auto r = service_->async_send(ccb_node_id_, COMMIT_LOG_ENTRIES, msg);
    if (!r) {
      BOOST_LOG_TRIVIAL(error) << "async send commit log entries error";
    }
  }
}

void rl_block::response_ccb_register_with_logs(node_id_t node) {
  auto res = std::make_shared<rlb_register_ccb_response>();
  fn_state fs = [res](const slice &s) {
    log_state state;
    bool ok = state.ParseFromArray(s.data(), int(s.size()));
    if (not ok) {
      BOOST_ASSERT(false);
    }
    res->set_cno(state.term());
  };
  fn_tx_log fl = [res](const tx_log_index &, const slice &s) {
    tx_log *log = res->add_logs();
    bool ok = log->ParseFromArray(s.data(), int(s.size()));
    if (not ok) {
      BOOST_ASSERT(false);
    }
  };
  log_service_->retrieve_state(fs);
  log_service_->retrieve_log(fl);

  sm_status s = state_machine_->status();
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  res->set_cno(s.term);
  res->set_source(node_id_);
  res->set_dest(node);
  res->set_dsb_node(dsb_node_id_);
  res->set_is_lead(s.state == RAFT_STATE_LEADER);
  auto r = service_->async_send(node, R2C_REGISTER_RESP, res);
  if (not r) {
    BOOST_LOG_TRIVIAL(error) << "send register CCB response error: " << r.error().message();
  }

}