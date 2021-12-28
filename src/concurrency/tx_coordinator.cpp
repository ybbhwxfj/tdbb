#include "concurrency/tx_coordinator.h"

#include <utility>

#ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_SHARE_NOTHING

template<>
enum_strings<tm_state>::e2s_t  enum_strings<tm_state>::enum2str = {
    {TM_IDLE, "TM_IDLE"},
    {TM_COMMITTED, "TM_COMMITTED"},
    {TM_ABORTED, "TM_ABORTED"},
    {TM_DONE, "TM_DONE"},
};
template<>
enum_strings<tm_trace_state>::e2s_t enum_strings<tm_trace_state>::enum2str = {
    {TTS_INVALID, "TTS_INVALID"},
    {TTS_HANDLE_TX_REQUEST, "TTS_HANDLE_TX_REQUEST"},
    {TTM_STATE_ADVANCE, "TTM_STATE_ADVANCE"},
    {TTS_WAIT_APPEND_LOG, "TTS_WAIT_APPEND_LOG"},
    {TTS_APPEND_LOG_DONE, "TTS_APPEND_LOG_DONE"},
};

tx_coordinator::tx_coordinator(
    uint64_t xid, uint32_t node_id,
    std::unordered_map<shard_id_t, node_id_t> lead_node,
    net_service *service, ptr<connection> connection, write_ahead_log *write_ahead_log, fn_tm_state fn)
    : xid_(xid), node_id_(node_id), lead_node_(std::move(lead_node)), service_(service),
      connection_(std::move(connection)), wal_(write_ahead_log), tm_state_(TM_IDLE),
      write_commit_log_(false), write_abort_log_(false), error_code_(EC::EC_OK), victim_(false),
      responsed_(false),
      fn_tm_state_(std::move(fn)) {
  start_ = boost::posix_time::microsec_clock::local_time();
}

void tx_coordinator::on_log_entry_commit(tx_cmd_type type) {
  switch (type) {
  case TX_CMD_TM_COMMIT: {
    on_committed();
    break;
  }
  case TX_CMD_TM_ABORT: {
    on_aborted();
    break;
  }
  case TX_CMD_TM_END: {
    on_ended();
    break;
  }
  default:break;
  }
}

void tx_coordinator::on_committed() {

  trace_message_ += "C;";
  tm_state_ = TM_COMMITTED;
  send_commit();
  send_tx_response();
}

void tx_coordinator::on_aborted() {

  trace_message_ += "A;";
  tm_state_ = TM_ABORTED; // TODO ... ec abort
  error_code_ = EC::EC_TX_ABORT;
  send_abort();
  send_tx_response();
}

void tx_coordinator::on_ended() {
  std::scoped_lock l(mutex_);

  trace_message_ += "E;";
  tm_state_ = TM_DONE;
  if (fn_tm_state_) {
    fn_tm_state_(xid_, tm_state_);
  }
}

result<void> tx_coordinator::handle_tx_request(const tx_request &req) {
  std::scoped_lock l(mutex_);

  trace_message_ += "tx req;";
  BOOST_ASSERT(req.distributed());
  for (auto iter = req.operations().begin(); iter != req.operations().end();
       ++iter) {
    shard_id_t rg = iter->rg_id();
    tx_rm_tracer &tracer = rm_tracer_[rg];
    *tracer.message_.add_operations() = *iter;
    tracer.rm_state_ = tx_rm_state::RM_IDLE;
    tracer.lead_ = 0;

    tracer.message_.set_distributed(true);
    tracer.message_.set_source(node_id_);
    tracer.message_.set_oneshot(req.oneshot());
    auto i = lead_node_.find(rg);
    if (lead_node_.end() != i) {
      tracer.lead_ = i->second;
      tracer.message_.set_dest(i->second);

    } else {
      error_code_ = EC::EC_INVALID_ARGUMENT;
      return outcome::failure(EC::EC_INVALID_ARGUMENT);
    }
  }

  for (auto &iter: rm_tracer_) {
    iter.second.message_.set_xid(xid_);
    iter.second.message_.set_client_request(false);
    auto r = service_->async_send(iter.second.lead_, TX_TM_REQUEST,
                                  iter.second.message_);
    if (not r) {
      return r;
    }
  }
  return outcome::success();
}

void tx_coordinator::send_commit() {
  for (const auto &iter: rm_tracer_) {
    node_id_t lead = iter.second.lead_;
    tx_tm_commit msg;
    msg.set_xid(xid_);
    msg.set_source_node(node_id_);
    msg.set_source_rg(TO_RG_ID(node_id_));
    msg.set_dest_node(lead);
    msg.set_dest_rg(TO_RG_ID(lead));
    result<void> r =
        service_->async_send(lead, message_type::TX_TM_COMMIT, msg);
    if (not r) {
    }
  }
  if (connection_ != nullptr) {
    // TODO response
  }
}
void tx_coordinator::send_abort() {
  for (const auto &iter: rm_tracer_) {
    node_id_t lead = iter.second.lead_;
    tx_tm_abort msg;
    msg.set_xid(xid_);
    msg.set_source_node(node_id_);
    msg.set_source_rg(TO_RG_ID(node_id_));
    msg.set_dest_node(lead);
    msg.set_dest_rg(TO_RG_ID(lead));

    result<void> r = service_->async_send(lead, message_type::TX_TM_ABORT, msg);
    if (not r) {

    }
  }
}

void tx_coordinator::handle_tx_rm_prepare(const tx_rm_prepare &msg) {
  std::scoped_lock l(mutex_);
  trace_message_ += id_2_name(msg.source_node()) + " P" + (msg.commit() ? "C;" : "A;");
  auto iter = rm_tracer_.find(msg.source_rg());
  if (iter == rm_tracer_.end()) {
    BOOST_ASSERT(false);
    return;
  }
  if (iter->second.rm_state_ == RM_IDLE) {
    iter->second.rm_state_ = msg.commit() ? RM_PREPARE_COMMIT : RM_PREPARE_ABORT;
    step_tm_state_advance();
  } else {
    if (not victim_) {
      BOOST_ASSERT(iter->second.rm_state_ != RM_PREPARE_ABORT &&
          iter->second.rm_state_ != RM_ABORTED);
    }
  }
}

void tx_coordinator::handle_tx_rm_ack(const tx_rm_ack &msg) {
  std::scoped_lock l(mutex_);

  auto iter = rm_tracer_.find(msg.source_rg());
  if (iter == rm_tracer_.end()) {
    BOOST_ASSERT(false);
    return;
  }

  if (msg.commit()) {
    if (iter->second.rm_state_ == RM_PREPARE_COMMIT) {
      iter->second.rm_state_ = RM_COMMITTED;
      step_tm_state_advance();
    } else {
      BOOST_ASSERT_MSG(
          iter->second.rm_state_ != RM_PREPARE_ABORT &&
              iter->second.rm_state_ != RM_ABORTED &&
              iter->second.rm_state_ != RM_IDLE,
          "error RM state");
    }
  } else {
    if (iter->second.rm_state_ == RM_PREPARE_COMMIT ||
        iter->second.rm_state_ == RM_PREPARE_ABORT ||
        iter->second.rm_state_ == RM_IDLE) {
      iter->second.rm_state_ = RM_ABORTED;
    } else {
      BOOST_ASSERT_MSG(iter->second.rm_state_ != RM_COMMITTED,
                       "error RM state");
      step_tm_state_advance();
    }
  }

}

void tx_coordinator::step_tm_state_advance() {
  uint32_t prepare_commit = 0;
  uint32_t prepare_abort = 0;
  uint32_t aborted = 0;
  uint32_t committed = 0;
  uint32_t idle = 0;
  uint32_t total = 0;

  trace_message_ += "adv;";
  for (const auto &i: rm_tracer_) {
    total++;
    switch (i.second.rm_state_) {
    case RM_PREPARE_COMMIT:prepare_commit++;
      break;
    case RM_PREPARE_ABORT:prepare_abort++;
      break;
    case RM_COMMITTED:committed++;
      break;
    case RM_ABORTED:aborted++;
      break;
    case RM_IDLE:idle++;
      break;
    }
  }
  if (idle == 0) {
    if (committed == total || aborted == total) {
      write_end_log();
    } else if (prepare_commit + prepare_abort == total) {
      if (prepare_abort == 0) {
        write_commit_log();
      } else {
        write_abort_log();
      }
    }
  }
}

void tx_coordinator::write_commit_log() {
  // append prepare commit log
  if (tm_state_ == TM_IDLE && !write_commit_log_) {
    tx_log log;
    log.set_log_type(TX_CMD_TM_COMMIT);
    log.set_xid(xid_);

    auto s = shared_from_this();

    trace_message_ += "C log;";
    wal_->async_append(log);
    write_commit_log_ = true;
  }
}
void tx_coordinator::write_abort_log() {
  if (tm_state_ == TM_IDLE && !write_abort_log_) {
    tx_log log;
    log.set_log_type(TX_CMD_TM_ABORT);
    log.set_xid(xid_);
    auto s = shared_from_this();

    trace_message_ += "A log;";
    wal_->async_append(log);
    write_abort_log_ = true;
  }
}
void tx_coordinator::write_end_log() {
  if (!write_end_done_ &&
      (tm_state_ == TM_COMMITTED || tm_state_ == TM_ABORTED)) {

    tx_log log;
    log.set_log_type(TX_CMD_TM_END);
    log.set_xid(xid_);
    trace_message_ += "E log;";

    wal_->async_append(log);
    write_end_done_ = true;
  }
}

void tx_coordinator::send_tx_response() {
  if (responsed_) {
    return;
  }

  responsed_ = true;
  response_.set_error_code(uint32_t(error_code_));
  result<void> r = connection_->async_send(CLIENT_TX_RESP, response_);
  if (!r) {
    BOOST_LOG_TRIVIAL(info) << "send client response error";
  }
}

void tx_coordinator::abort(EC ec) {
  std::scoped_lock l(mutex_);
  if (ec == EC_DEADLOCK) {
    victim_ = true;
    trace_message_ += "victim;";
  }
  switch (tm_state_) {
  case TM_IDLE: {
    error_code_ = ec;
    tm_state_ = TM_ABORTED;
    send_tx_response();
    send_abort();
    return;
  }
  case TM_ABORTED: {
    error_code_ = ec;
    send_tx_response();
    send_abort();
    return;
  }
  default: {
    return;
  }
  }
}

void tx_coordinator::timeout_clean_up() {
  boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
  if ((now - start_).total_microseconds() < 1000) {
    return;
  }
  std::scoped_lock l(mutex_);
  switch (tm_state_) {
  case TM_IDLE: {
    tm_state_ = TM_ABORTED;
    send_tx_response();
    send_abort();
    return;
  }
  case TM_COMMITTED: {
    send_tx_response();
    send_commit();
    return;
  }
  case TM_ABORTED: {
    send_tx_response();
    send_abort();
    return;
  }
  case TM_DONE: {
    send_tx_response();
    return;
  }
  }
}
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
void tx_coordinator::handle_tx_enable_violate(const tx_enable_violate &msg) {
  auto iter = rm_tracer_.find(TO_RG_ID(msg.source()));
  if (iter == rm_tracer_.end()) {
    BOOST_ASSERT(false);
    return;
  }
  iter->second.violate_ = msg.violable();
  send_tx_enable_violate();
}

void tx_coordinator::send_tx_enable_violate() {
  for (const auto &iter: rm_tracer_) {
    if (not iter.second.violate_) {
      return;
    }
  }
  for (const auto &iter: rm_tracer_) {
    tx_enable_violate msg;
    msg.set_xid(xid_);
    msg.set_source(node_id_);
    msg.set_dest(iter.second.lead_);
    auto r = service_->async_send(iter.second.lead_, TM_ENABLE_VIOLATE, msg);
    if (not r) {
      BOOST_LOG_TRIVIAL(error) << " send TM tx enable violate error";
    }
  }
}
#endif // DB_TYPE_GEO_REP_OPTIMIZE

#endif // DB_TYPE_SHARE_NOTHING
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC