#include "concurrency/tx_coordinator.h"
#include "common/define.h"
#include <utility>

#ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_SHARE_NOTHING

template<>
enum_strings<tm_state>::e2s_t enum_strings<tm_state>::enum2str = {
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
    boost::asio::io_context::strand s, uint64_t xid, uint32_t node_id,
    std::unordered_map<shard_id_t, node_id_t> lead_node, net_service *service,
    ptr<connection> connection, write_ahead_log *write_ahead_log,
    fn_tm_state fn)
    : tx_base(s, xid), xid_(xid), node_id_(node_id), node_name_(id_2_name(node_id)),
      lead_node_(std::move(lead_node)), service_(service),
      connection_(std::move(connection)), wal_(write_ahead_log),
      tm_state_(TM_IDLE), write_begin_log_(false), write_commit_log_(false), write_abort_log_(false),
      error_code_(EC::EC_OK), victim_(false), responsed_(false),
      fn_tm_state_(std::move(fn)), latency_read_(0), latency_read_dsb_(0),
      latency_replicate_(0), latency_append_(0), latency_lock_wait_(0),
      latency_part_(0), num_lock_(0), num_read_violate_(0),
      num_write_violate_(0) {
  start_ = std::chrono::steady_clock::now();
}

void tx_coordinator::on_log_entry_commit(tx_cmd_type type) {
  std::scoped_lock l(mutex_);
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
  case TX_CMD_TM_BEGIN: {
    on_begin();
  }
  default:break;
  }
}

void tx_coordinator::on_begin() {
#ifdef TX_TRACE
  trace_message_ += "B;";
#endif
  auto _r = send_rm_request();
  if (!_r) {
    error_code_ = EC_TX_ABORT;
    send_tx_response();
  }
}

void tx_coordinator::on_committed() {
#ifdef TX_TRACE
  trace_message_ += "C;";
#endif
  tm_state_ = TM_COMMITTED;
  send_commit();
}

void tx_coordinator::on_aborted() {
#ifdef TX_TRACE
  trace_message_ += "A;";
#endif
  tm_state_ = TM_ABORTED; // TODO ... ec abort
  error_code_ = EC::EC_TX_ABORT;
  send_abort();
  send_tx_response();
}

void tx_coordinator::on_ended() {
  std::scoped_lock l(mutex_);
#ifdef TX_TRACE
  trace_message_ += "E;";
#endif
  tm_state_ = TM_DONE;
  if (fn_tm_state_) {
    fn_tm_state_(xid_, tm_state_);
  }
}

result<void> tx_coordinator::handle_tx_request(const tx_request &req) {
#ifdef TX_TRACE
  trace_message_ += "tx req;";
#endif
  BOOST_ASSERT(req.distributed());
  for (auto iter = req.operations().begin(); iter != req.operations().end();
       ++iter) {
    shard_id_t sd_id = iter->sd_id();

    if (!rm_tracer_.contains(sd_id)) {
      LOG(trace) << node_name_ << " transaction TM " << xid_ << " group " << sd_id;
      rm_tracer_.insert(std::make_pair(sd_id, tx_rm_tracer()));
    }
    tx_rm_tracer &tracer = rm_tracer_[sd_id];
    *tracer.message_.add_operations() = *iter;
    tracer.rm_state_ = tx_rm_state::RM_IDLE;
    tracer.lead_ = 0;

    tracer.message_.set_distributed(true);
    tracer.message_.set_source(node_id_);
    tracer.message_.set_oneshot(req.oneshot());

    auto i = lead_node_.find(sd_id);
    if (lead_node_.end() != i) {
      tracer.lead_ = i->second;
      tracer.message_.set_dest(i->second);

    } else {
      error_code_ = EC::EC_INVALID_ARGUMENT;
      return outcome::failure(EC::EC_INVALID_ARGUMENT);
    }
  }

  write_begin_log();
  return outcome::success();
}

result<void> tx_coordinator::send_rm_request() {
  for (auto &iter : rm_tracer_) {
    iter.second.message_.set_xid(xid_);
    iter.second.message_.set_client_request(false);
    auto m = std::make_shared<tx_request>(iter.second.message_);
    LOG(trace) << node_name_ << " transaction TM " << xid_ << " to " << id_2_name(iter.second.lead_);
    auto r = service_->async_send(iter.second.lead_, TX_TM_REQUEST, m, true);
    if (not r) {
      LOG(error) << node_name_ << " transaction TM " << xid_ << " to " << id_2_name(iter.second.lead_);
      return r;
    }
  }
  return outcome::success();
}

void tx_coordinator::send_commit() {
  LOG(trace) << node_name_ << " send commit " << xid_;
  for (const auto &iter : rm_tracer_) {
    node_id_t lead = iter.second.lead_;
    auto msg = std::make_shared<tx_tm_commit>();

    msg->set_xid(xid_);
    msg->set_source_node(node_id_);
    msg->set_source_rg(TO_RG_ID(node_id_));
    msg->set_dest_node(lead);
    msg->set_dest_rg(TO_RG_ID(lead));
#ifdef TX_TRACE
    trace_message_ += id_2_name(msg->dest_node()) + " SA;";
#endif
    result<void> r =
        service_->async_send(lead, message_type::TX_TM_COMMIT, msg, true);
    if (not r) {
      LOG(error) << "async send tx_rm commit error";
    }
  }
  if (connection_ != nullptr) {
    // TODO response
  }
}
void tx_coordinator::send_abort() {
  LOG(trace) << node_name_ << " send abort " << xid_;
  for (const auto &iter : rm_tracer_) {
    node_id_t lead = iter.second.lead_;
    auto msg = std::make_shared<tx_tm_abort>();
    msg->set_xid(xid_);
    msg->set_source_node(node_id_);
    msg->set_source_rg(TO_RG_ID(node_id_));
    msg->set_dest_node(lead);
    msg->set_dest_rg(TO_RG_ID(lead));

    result<void> r = service_->async_send(lead, message_type::TX_TM_ABORT, msg, true);
    if (not r) {
    }
  }
}

void tx_coordinator::send_end() {
  LOG(trace) << node_name_ << " end " << xid_;
  for (const auto &iter : rm_tracer_) {
    node_id_t lead = iter.second.lead_;
    auto msg = std::make_shared<tx_tm_end>();
    msg->set_xid(xid_);
    msg->set_source_node(node_id_);
    msg->set_dest_node(lead);

    result<void> r = service_->async_send(lead, message_type::TX_TM_END, msg, true);
    if (not r) {
    }
  }
}

void tx_coordinator::handle_tx_rm_prepare(const tx_rm_prepare &msg) {
  LOG(trace) << node_name_ << " tx rm prepare " << xid_
             << (msg.commit() ? " Commit" : " Abort")
             << " from " << id_2_name(msg.source_node());
#ifdef TX_TRACE
  trace_message_ +=
      id_2_name(msg.source_node()) + " P" + (msg.commit() ? "C;" : "A;");
#endif
  auto iter = rm_tracer_.find(msg.source_rg());
  if (iter == rm_tracer_.end()) {
    BOOST_ASSERT(false);
    return;
  }
  if (iter->second.rm_state_ == RM_IDLE) {
    iter->second.rm_state_ =
        msg.commit() ? RM_PREPARE_COMMIT : RM_PREPARE_ABORT;
    if (msg.commit()) {
      latency_read_ += msg.latency_read();
      latency_read_dsb_ += msg.latency_read_dsb();
      latency_replicate_ += msg.latency_replicate();
      latency_append_ += msg.latency_append();
      latency_lock_wait_ += msg.latency_lock_wait();
      latency_part_ += msg.latency_part();
      num_lock_ += msg.num_lock();
      num_read_violate_ += msg.num_read_violate();
      num_write_violate_ += msg.num_write_violate();
    }
    step_tm_state_advance();
  } else {
    if (not victim_) {
      BOOST_ASSERT(iter->second.rm_state_ != RM_PREPARE_ABORT &&
          iter->second.rm_state_ != RM_ABORTED);
    }
  }
}

void tx_coordinator::handle_tx_rm_ack(const tx_rm_ack &msg) {
  LOG(trace) << node_name_ << " tx rm ACK " << xid_
             << (msg.commit() ? " Commit" : " Abort")
             << " from " << id_2_name(msg.source_node());
#ifdef TX_TRACE
  trace_message_ +=
      id_2_name(msg.source_node()) + " A" + (msg.commit() ? "C;" : "A;");
#endif
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
      BOOST_ASSERT_MSG(iter->second.rm_state_ != RM_PREPARE_ABORT &&
          iter->second.rm_state_ != RM_ABORTED &&
          iter->second.rm_state_ != RM_IDLE,
                       "error RM state");
    }
  } else {
    if (iter->second.rm_state_ == RM_PREPARE_COMMIT ||
        iter->second.rm_state_ == RM_PREPARE_ABORT ||
        iter->second.rm_state_ == RM_IDLE) {
      iter->second.rm_state_ = RM_ABORTED;
      step_tm_state_advance();
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
#ifdef TX_TRACE
  trace_message_ += "adv;";
#endif
  for (const auto &i : rm_tracer_) {
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
  LOG(trace) << node_name_ << " state advance " << xid_
             << " prepare commit: " << prepare_commit
             << " prepare abort: " << prepare_abort
             << " committed: " << committed
             << " aborted: " << aborted
             << " total: " << total;
  if (idle == 0) {
    if (committed == total || aborted == total) {
      //send_end();
      send_tx_response();
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

void tx_coordinator::write_begin_log() {
  // append prepare commit log
  if (tm_state_ == TM_IDLE  && !write_begin_log_) {
    tx_log_proto log;
    log.set_log_type(TX_CMD_TM_BEGIN);
    log.set_xid(xid_);
    auto s = shared_from_this();
#ifdef TX_TRACE
    trace_message_ += "B log;";
#endif
    tx_log_binary log_binary = tx_log_proto_to_binary(log);
    wal_->async_append(log_binary);
    write_begin_log_ = true;
  }
}

void tx_coordinator::write_commit_log() {
  // append prepare commit log
  if (tm_state_ == TM_IDLE && not victim_ && !write_commit_log_) {
    tx_log_proto log;
    log.set_log_type(TX_CMD_TM_COMMIT);
    log.set_xid(xid_);

    auto s = shared_from_this();
#ifdef TX_TRACE
    trace_message_ += "C log;";
#endif
    tx_log_binary log_binary = tx_log_proto_to_binary(log);
    wal_->async_append(log_binary);
    write_commit_log_ = true;
  }
}
void tx_coordinator::write_abort_log() {
  if (tm_state_ == TM_IDLE && !write_abort_log_ && !write_commit_log_) {
    tx_log_proto log;
    log.set_log_type(TX_CMD_TM_ABORT);
    log.set_xid(xid_);
    auto s = shared_from_this();
#ifdef TX_TRACE
    trace_message_ += "A log;";
#endif
    tx_log_binary log_binary = tx_log_proto_to_binary(log);
    wal_->async_append(log_binary);
    write_abort_log_ = true;
  }
}
void tx_coordinator::write_end_log() {
  if (!write_end_done_ &&
      (tm_state_ == TM_COMMITTED || tm_state_ == TM_ABORTED)) {

    tx_log_proto log;
    log.set_log_type(TX_CMD_TM_END);
    log.set_xid(xid_);
#ifdef TX_TRACE
    trace_message_ += "E log;";
#endif
    tx_log_binary log_binary = tx_log_proto_to_binary(log);
    wal_->async_append(log_binary);
    write_end_done_ = true;
  }
}

void tx_coordinator::send_tx_response() {
  LOG(trace) << node_name_ << " tx response " << xid_;
  if (responsed_) {
    return;
  }

  responsed_ = true;
  auto response = std::make_shared<tx_response>();
  response->set_error_code(uint32_t(error_code_));
  response->set_latency_part(latency_part_);
  response->set_latency_read(latency_read_);
  response->set_latency_read_dsb(latency_read_dsb_);
  response->set_latency_replicate(latency_replicate_);
  response->set_latency_append(latency_append_);
  response->set_latency_lock_wait(latency_lock_wait_);
  response->set_access_part(rm_tracer_.size());
  response->set_num_lock(num_lock_);
  response->set_num_read_violate(num_read_violate_);
  response->set_num_write_violate(num_write_violate_);

  service_->conn_async_send(connection_, CLIENT_TX_RESP, response);
}

void tx_coordinator::abort(EC ec) {
  std::scoped_lock l(mutex_);
  if (ec == EC_VICTIM) {
    if (tm_state_ == TM_IDLE && not victim_) {
      victim_ = true;
#ifdef TX_TRACE
      trace_message_ += "victim;";
#endif
    }
  }
  if (write_commit_log_) {
    return;
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
  auto now = std::chrono::steady_clock::now();
  std::chrono::duration<double_t> duration = (now - start_);
  if (duration.count() < 1) {
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
  for (const auto &iter : rm_tracer_) {
    if (not iter.second.violate_) {
      return;
    }
  }
  for (const auto &iter : rm_tracer_) {
    auto msg = std::make_shared<tx_enable_violate>();
    msg->set_xid(xid_);
    msg->set_source(node_id_);
    msg->set_dest(iter.second.lead_);
    auto r = service_->async_send(iter.second.lead_, TM_ENABLE_VIOLATE, msg);
    if (not r) {
      BOOST_LOG_TRIVIAL(error) << " send TM tx_rm enable violate error";
    }
  }
}
#endif // DB_TYPE_GEO_REP_OPTIMIZE
void tx_coordinator::debug_tx(std::ostream &os) {
  os << id_2_name(node_id_) << " TM : " << xid_
     << " state: " << enum2str(tm_state_) << " trace: " << trace_message_
     << std::endl;
}
#endif // DB_TYPE_SHARE_NOTHING
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC