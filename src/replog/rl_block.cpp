#include "replog/rl_block.h"
#include "common/debug_url.h"
#include "proto/tx_op_type.h"

rl_block::rl_block(const config &conf, ptr<net_service> service,
                   fn_become_leader f_become_leader,
                   fn_become_follower f_become_follower,
                   fn_commit_entries f_commit_entries)
    : conf_(conf), service_(service),
      fn_become_leader_(std::move(f_become_leader)),
      fn_become_follower_(std::move(f_become_follower)),
      commit_entries_(std::move(f_commit_entries)), cno_(0),
      node_id_(conf.node_id()), node_name_(id_2_name(conf.node_id())),
      ccb_node_id_(std::nullopt), dsb_node_id_(std::nullopt),
      rlb_strand_(service_->get_service(SERVICE_ASYNC_CONTEXT)),
      time_("RLB handle") {
  log_service_ = cs_new<log_service_impl>(conf, service_);

  fn_on_become_leader fn_bl = [this](uint64_t term) { on_become_leader(term); };
  fn_on_become_follower fn_bf = [this](uint64_t term) {
    on_become_follower(term);
  };

  fn_commit_entries
      fn_commit =
      [this](EC ec, bool is_lead,
             const std::vector<ptr<raft_log_entry>> &logs) {
        on_commit_entries(ec, is_lead, logs);
      };

  state_machine_ = cs_new<state_machine>(conf, service, fn_bl, fn_bf, fn_commit,
                                         log_service_);

  start_ = state_machine_->start_time();
}

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
  LOG(info) << "start up RLB " << node_name_ << " ...";
}

void rl_block::on_stop() {
  state_machine_->on_stop();
  log_service_->on_stop();
  // time_.print();
}

result<void>
rl_block::rlb_handle_message(const ptr<connection>, message_type,
                             const ptr<ccb_register_ccb_request> m) {
  handle_register_ccb(*m);
  return outcome::success();
}

result<void>
rl_block::rlb_handle_message(const ptr<connection>, message_type,
                             const ptr<dsb_register_dsb_request> m) {
  handle_register_dsb(*m);
  return outcome::success();
}

result<void>
rl_block::rlb_handle_message(const ptr<connection>, message_type,
                             const ptr<ccb_report_status_response> m) {
  handle_report_status_response(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection>, message_type,
                                          const ptr<request_vote_request> m) {
  state_machine_->handle_request_vote_request(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection>, message_type,
                                          const ptr<request_vote_response> m) {
  state_machine_->handle_request_vote_response(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection>, message_type,
                                          const ptr<append_entries_request> m) {
  state_machine_->handle_append_entries_request(*m);
  return outcome::success();
}

result<void>
rl_block::rlb_handle_message(const ptr<connection>, message_type,
                             const ptr<append_entries_response> m) {
  state_machine_->handle_append_entries_response(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection>, message_type,
                                          const ptr<transfer_leader> m) {
  state_machine_->handle_transfer_leader(m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection>, message_type,
                                          const ptr<transfer_notify> m) {
  state_machine_->handle_transfer_notify(*m);
  return outcome::success();
}

result<void> rl_block::rlb_handle_message(const ptr<connection>, message_type,
                                          const ptr<replay_to_dsb_response>) {
  return outcome::success();
}

void rl_block::handle_register_ccb(const ccb_register_ccb_request &req) {
  sm_status s = state_machine_->status();
  node_id_t node_id = req.source();
  if (s.term == 0) {
    return;
  }
  if (s.state != RAFT_STATE_LEADER && s.state != RAFT_STATE_FOLLOWER) {
    return;
  }
  auto res = cs_new<rlb_register_ccb_response>();
  res->set_cno(s.term);
  res->set_source(node_id_);
  res->set_dest(req.source());
  res->set_lead_node(s.lead_node);
  res->set_is_lead(s.state == RAFT_STATE_LEADER);
  res->set_ok(true);
  if (s.term != req.cno()) {
    LOG(trace) << node_name_ << " receive register_ccb request";
    if (!ccb_node_id_.has_value() && ccb_shards_.empty()) {
      ccb_node_id_ = std::optional<node_id_t>(node_id);
    }

    for (auto id : req.shard_ids()) {
      ccb_shards_.insert(std::make_pair(id, node_id));
      if (ccb_shards_.size() > 1) {
        ccb_node_id_ = std::nullopt;
      }
      auto iter = dsb_shards_.find(id);
      if (iter != dsb_shards_.end()) {
        res->mutable_shard_ids()->Add(iter->first);
        res->mutable_dsb_node_ids()->Add(iter->second);
      } else {
        return;
      }
    }
  }

  auto r = service_->async_send(req.source(), R2C_REGISTER_RESP, res);
  if (!r) {
    LOG(error) << "send register_ccb response";
  }
}

void rl_block::handle_register_dsb(const dsb_register_dsb_request &dsb) {
  sm_status s = state_machine_->status();
  node_id_t node_id = dsb.source();

  if (s.term == 0) {
    return;
  }
  if (s.state != RAFT_STATE_LEADER && s.state != RAFT_STATE_FOLLOWER) {
    return;
  }
  auto res = cs_new<rlb_register_dsb_response>();
  if (s.term != dsb.cno()) {
    if (dsb_node_id_ == std::nullopt || dsb_node_id_.value() != dsb.source()) {
      dsb_node_id_ = std::optional<node_id_t>(dsb.source());
    }

    if (!dsb_node_id_.has_value() && ccb_shards_.empty()) {
      dsb_node_id_ = std::optional<node_id_t>(node_id);
    }

    for (auto id : dsb.shard_ids()) {
      dsb_shards_.insert(std::make_pair(id, node_id));
      if (dsb_shards_.size() > 1) {
        dsb_node_id_ = std::nullopt;
      }
    }
    if (dsb_shards_.size() == conf_.shard_ids().size()) {
      LOG(info) << node_name_ << " dsb shard size " << dsb_shards_.size();
    }
  } else {
    // same cno
  }

  res->set_ok(true);
  res->set_cno(s.term);
  res->set_source(node_id_);
  res->set_dest(dsb.source());
  res->set_lead(s.state == RAFT_STATE_LEADER);

  auto r = service_->async_send(dsb.source(), R2D_REGISTER_RESP, res);
  if (!r) {
    LOG(error) << "send register_dsb response";
  }
}

void rl_block::send_error_consistency(node_id_t node_id, message_type mt) {
  BOOST_ASSERT(mt == CCB_ERROR_CONSISTENCY || mt == DSB_ERROR_CONSISTENCY);
  auto m = cs_new<error_consistency>();
  m->set_source(node_id_);
  m->set_dest(node_id);
  m->set_cno(cno_);
  result<void> rs =
      service_->async_send(m->dest(), mt, m);
  if (!rs) {
    LOG(error) << node_name_ << "send to CCB error, "
               << id_2_name(m->dest());
  }
}

void rl_block::handle_append_entries(const ccb_append_log_request &request,
                                     std::chrono::steady_clock::time_point ts) {
  if (request.cno() != cno_) {
    send_error_consistency(request.source(), CCB_ERROR_CONSISTENCY);
    return;
  }
  auto r = state_machine_->ccb_append_log(request, ts);
  if (not r) {
    LOG(error) << "ccb append log error";
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
  LOG(trace) << node_name_ << " on become leader";
  // ccb_responsed_[ccb_node_id_] = false;
  // send_report(true);
}

void rl_block::on_become_follower(uint64_t term) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  cno_ = term;
  if (fn_become_follower_) {
    fn_become_follower_(term);
  }
  LOG(info) << node_name_ << " on become follower";
  // ccb_responsed_[ccb_node_id_] = false;
  // send_report(false);
}

void rl_block::handle_report_status_response(
    const ccb_report_status_response &res) {
  auto iter = ccb_responsed_.find(res.source());
  if (iter != ccb_responsed_.end()) {
    LOG(info) << node_name_ << " handle report status response CCB "
              << id_2_name(res.source());
    iter->second = true;
  }
}

void rl_block::on_commit_entries(EC ec, bool is_lead,
                                 const std::vector<ptr<raft_log_entry>>
                                 &logs) {
  if (commit_entries_) {
    commit_entries_(EC::EC_OK, is_lead, logs
    );
  }
  if (ec == EC_OK) {
    if (is_lead) {
      response_commit_log(EC::EC_OK, logs
      );
    } else {
// LOG(error) << node_name_ << " not lead node";
    }
  } else {
    if (is_lead) {
      response_commit_log(ec, logs
      );
    }
  }
}

void rl_block::response_commit_log(
    EC ec, const std::vector<ptr<raft_log_entry>> &logs) {
  if (logs.empty()) {
    return;
  }

  uint64_t cno = cno_;
  uint64_t source = node_id_;
#ifdef TEST_APPEND_TIME
  commit_msg->set_debug_send_ts(steady_clock_ms_since_epoch());
#endif
  auto now = std::chrono::steady_clock::now();
  uint64_t current_us = to_microseconds(now - start_);
  uint64_t previous_latency = 0;
  uint64_t tx_log_index_in_binary = 0;
  auto shared = shared_from_this();
  std::unordered_map<node_id_t, ptr<rlb_commit_entries>>
      commit_msg_map;
  std::unordered_map<node_id_t, ptr<replay_to_dsb_request>>
      replay_msg_map;
  for (
    const ptr<raft_log_entry> &log
      : logs) {
    repeated_tx_logs *tx_logs = log->mutable_repeated_tx_logs();
    handle_repeated_tx_logs_to_buffer(
        *tx_logs,
        [
            ec,
            shared,
            cno,
            source,
            current_us,
            &commit_msg_map,
            &replay_msg_map,
            &previous_latency,
            &tx_log_index_in_binary](
            log_buffer &buffer
        ) {
          uint64_t start_us = buffer.timestamp();
          node_id_t ccb_node_id = buffer.node_id();
          ptr<rlb_commit_entries> commit_msg;
          ptr<replay_to_dsb_request> replay_msg;
          tx_log_index_in_binary += 1;
          if (start_us != 0) {
            auto latency = current_us - start_us;
            buffer.
                set_timestamp(latency);
            previous_latency = latency;
          } else {
            if (previous_latency != 0) {
              buffer.
                  set_timestamp(previous_latency);
            }
          }

          auto iter = commit_msg_map.find(ccb_node_id);
          if (iter == commit_msg_map.end()) {
            commit_msg = cs_new<rlb_commit_entries>();
            commit_msg_map.insert(std::make_pair(ccb_node_id, commit_msg));
            commit_msg->set_error_code(ec);
            commit_msg->set_dest(ccb_node_id);
            commit_msg->set_source(source);
            commit_msg->set_cno(cno);
          } else {
            commit_msg = iter->second;
          }
          commit_msg->mutable_repeated_tx_logs()->
              append(buffer
                         .
                             data(), buffer
                         .
                             size()
          );
          ptr<tx_log_proto> log_proto(cs_new<tx_log_proto>());
          bool ok = log_proto->ParseFromArray(buffer.payload_data(), buffer.payload_size());
          if (!ok) {
            LOG(error)
              << shared->node_name_ << " parse log error";
          }
          if (shared->dsb_shards_.
              size()
              == 1 && shared->dsb_node_id_.
              has_value()
              ) {
            node_id_t dsb_node_id = shared->dsb_node_id_.value();
            auto iter_replay_msg = replay_msg_map.find(dsb_node_id);
            if (iter_replay_msg == replay_msg_map.
                end()
                ) {
              replay_msg = cs_new<replay_to_dsb_request>();
              replay_msg_map.
                  insert(std::make_pair(dsb_node_id, replay_msg)
              );
              replay_msg->
                  set_dest(dsb_node_id);
              replay_msg->
                  set_source(source);
              replay_msg->
                  set_cno(cno);
            } else {
              replay_msg = iter_replay_msg->second;
            }
            replay_msg->mutable_repeated_tx_logs()->
                append(buffer
                           .
                               data(), buffer
                           .
                               size()
            );
          } else {
            ptr<tx_log_proto> proto(cs_new<tx_log_proto>());
            if (ec == EC::EC_OK && proto->ParseFromArray(buffer.payload_data(), buffer.payload_size())) {
              for (auto op : proto->operations()) {
                if (is_write_operation(op.op_type())) {
                  shard_id_t id = op.tuple_row().shard_id();
                  auto iter_dsb_node = shared->dsb_shards_.find(id);
                  if (iter_dsb_node != shared->dsb_shards_.end()) {
                    node_id_t dsb_node_id = iter_dsb_node->second;
                    auto iter_replay_msg = replay_msg_map.find(dsb_node_id);
                    if (iter_replay_msg == replay_msg_map.end()) {
                      replay_msg = cs_new<replay_to_dsb_request>();
                      replay_msg_map.insert(std::make_pair(dsb_node_id, replay_msg));
                      replay_msg->set_dest(dsb_node_id);
                      replay_msg->set_source(source);
                      replay_msg->set_cno(cno);
                    } else {
                      replay_msg = iter_replay_msg->second;
                    }
                    *replay_msg->add_operations() = op;
                  }
                }
              }
            }
          }
        });
  }
  for (auto &pair : commit_msg_map) {
    auto r_send_commit =
        service_->async_send(
            pair.first,
            COMMIT_LOG_ENTRIES,
            pair.second,
            true);
    if (!r_send_commit) {
      LOG(error) << "async send commit log entries error";
    }
  }
  for (auto &pair : replay_msg_map) {
    auto r = service_->async_send(
        pair.first,
        R2D_REPLAY_TO_DSB_REQ,
        pair.second,
        true);
    if (!r) {
      LOG(error) << "async send replay log entries error";
    }
  }
}
