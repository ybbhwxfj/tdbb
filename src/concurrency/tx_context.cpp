#include "concurrency/tx_context.h"
#ifdef DB_TYPE_NON_DETERMINISTIC
#include <boost/assert.hpp>
#include <boost/log/trivial.hpp>
#include <utility>

template<>
enum_strings<rm_state>::e2s_t enum_strings<rm_state>::enum2str = {
    {RM_IDLE, "RM_IDLE"},
    {RM_COMMITTED, "RM_COMMITTED"},
    {RM_ABORTED, "RM_ABORTED"},
    {RM_PREPARE_COMMIT, "RM_PREPARE_COMMIT"},
    {RM_PREPARE_ABORT, "RM_PREPARE_ABORT"},
};

template<>
enum_strings<rm_trace_state>::e2s_t enum_strings<rm_trace_state>::enum2str = {
    {RTS_INVALID, "RTS_INVALID"},
    {RTS_HANDLE_OPERATION, "RTS_HANDLE_OPERATION"},
    {RTS_PROCESS_TX_REQUEST, "RTS_PROCESS_TX_REQUEST"},
    {RTS_WAIT_LOCK, "RTS_WAIT_LOCK"},
    {RTS_LOCK_DONE, "RTS_LOCK_DONE"},
    {RTS_WAIT_READ_FROM_DSB, "RTS_WAIT_READ_FROM_DSB"},
    {RTS_READ_FROM_DSB_DONE, "RTS_READ_FROM_DSB_DONE"},
    {RTS_WAIT_FORCE_LOG, "RTS_WAIT_FORCE_LOG"},
    {RTS_FORCE_LOG_DONE, "RTS_FORCE_LOG_DONE"},
};

tx_context::tx_context(uint64_t xid, uint32_t node_id, uint32_t dsb_node_id,
                       uint64_t cno, bool distributed, access_mgr *mgr,
                       net_service *service, ptr<connection> conn, write_ahead_log *write_ahead_log, fn_tx_state fn,
                       deadlock *dl)
    :
    tx(xid),
    cno_(cno), node_id_(node_id),
    node_name_(id_2_name(node_id)), dsb_node_id_(dsb_node_id), xid_(xid),
    distributed_(distributed), coord_node_id_(0), oid_(1), mgr_(mgr),
    service_(service), cli_conn_(std::move(conn)), error_code_(EC::EC_OK),
    state_(rm_state::RM_IDLE), lock_acquire_(nullptr), wal_(write_ahead_log),
    trace_state_(RTS_INVALID),
    has_responsed_(false),
    fn_tx_state_(std::move(fn)),
    prepare_commit_log_synced_(false),
    commit_log_synced_(false),
    dl_(dl),
    victim_(false),
    log_rep_delay_(0) {
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  if (is_geo_rep_optimized()) {
    dependency_committed_ = false;
    dep_in_count_ = 0;
  }
#endif //DB_TYPE_GEO_REP_OPTIMIZE
  BOOST_ASSERT(node_id != 0);
  BOOST_ASSERT(dsb_node_id != 0);
  start_ = boost::posix_time::microsec_clock::local_time();
  part_time_tracer_.begin();
}

void tx_context::notify_lock_acquire(EC ec, const ptr<std::vector<ptr<tx_context>>> &in) {
  trace_message_ += "lk ntf;";
  auto s = shared_from_this();
  auto fn = [s, ec, in]() {
    std::scoped_lock l(s->mutex_);
    if (s->lock_acquire_) {
      auto fn = s->lock_acquire_;
      s->lock_acquire_ = nullptr;
      fn(ec);
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
      if (in) {
        for (const auto &p: *in) {
          s->register_dependency(p);
        }
      }
#endif
    } else {
      BOOST_ASSERT(false);
    }
  };
  boost::asio::dispatch(service_->get_service(SERVICE_HANDLE), fn);
}

void tx_context::lock_acquire(EC ec, oid_t) {
  notify_lock_acquire(ec, nullptr);
}

void tx_context::async_read(
    uint32_t table_id, tuple_id_t key, bool read_for_write,
    const std::function<void(EC, const tuple_pb &tuple)> &fn_read_done) {
  uint32_t oid = oid_++;
  lock_mode lt = lock_mode::LOCK_READ_ROW;
  if (read_for_write) {
    lt = lock_mode::LOCK_WRITE_ROW;
  }
  auto s = shared_from_this();
  BOOST_ASSERT(lock_acquire_ == nullptr);
  ptr<lock_item> l(new lock_item(s->xid_, oid, lt, table_id, predicate(key)));
  s->locks_.insert(std::make_pair(oid, l));
  lock_acquire_ = [table_id, key, oid, s, fn_read_done](EC ec) {
    s->lock_wait_time_tracer_.end();
    s->trace_state_ = RTS_LOCK_DONE;
    if (ec == EC::EC_OK) {
      std::pair<tuple_pb, bool> r = s->mgr_->get(table_id, key);
      if (r.second) {
        BOOST_ASSERT(not(ec == EC::EC_OK && is_tuple_nil(r.first)));
        fn_read_done(ec, r.first);
      } else { // read from DSB
        auto fn_read_from_dsb = [fn_read_done](EC ec,
                                               const tuple_pb &tuple) {
          BOOST_ASSERT(not(ec == EC::EC_OK && is_tuple_nil(tuple)));
          fn_read_done(ec, tuple);
        };

        s->read_data_from_dsb(table_id, key, oid, fn_read_from_dsb);
      }
    } else { // error
      BOOST_LOG_TRIVIAL(info) << "cannot find tuple, table id:" << table_id << " tuple id:" << (key);
      fn_read_done(ec, tuple_pb());
    }
  };

  BOOST_ASSERT(mgr_);
  trace_state_ = RTS_WAIT_LOCK;
  trace_message_ += "lk;";
  lock_wait_time_tracer_.begin();
  mgr_->lock_row(xid_, oid, lt, table_id, predicate(key), shared_from_this());
}

void tx_context::async_update(uint32_t table_id, tuple_id_t key,
                              const tuple_pb &tuple,
                              const std::function<void(EC)> &fn_update_done) {
  uint32_t oid = oid_++;
  auto s = shared_from_this();
  BOOST_ASSERT(lock_acquire_ == nullptr);
  ptr<lock_item> l(new lock_item(s->xid_, oid, LOCK_WRITE_ROW, table_id, predicate(key)));
  s->locks_.insert(std::make_pair(oid, l));
  lock_acquire_ = [table_id, key, oid, s, fn_update_done,
      tuple](EC ec) {
    s->lock_wait_time_tracer_.end();
    s->trace_state_ = RTS_LOCK_DONE;
    if (ec == EC::EC_OK) {
      std::pair<tuple_pb, bool> r = s->mgr_->get(table_id, key);
      if (r.second) {
        fn_update_done(EC::EC_OK);
      } else {
        auto fn_read_done = [s, fn_update_done](EC ec, const tuple_pb &) {
          fn_update_done(ec);
        };

        s->read_data_from_dsb(table_id, key, oid, fn_read_done);
      }
    } else {
      BOOST_LOG_TRIVIAL(info) << "cannot find tuple, table id:" << table_id << " tuple id:" << (key);
      fn_update_done(ec);
    }
  };
  trace_state_ = RTS_WAIT_LOCK;
  trace_message_ += "lk;";
  lock_wait_time_tracer_.begin();
  mgr_->lock_row(xid_, oid, LOCK_WRITE_ROW, table_id, predicate(key), shared_from_this());
}

void tx_context::async_insert(uint32_t table_id, tuple_id_t key,
                              const tuple_pb &tuple,
                              const std::function<void(EC)> &fn_write_done) {
  uint32_t oid = oid_++;
  auto s = shared_from_this();
  BOOST_ASSERT(lock_acquire_ == nullptr);
  ptr<lock_item> l(new lock_item(s->xid_, oid, LOCK_WRITE_ROW, table_id, predicate(key)));
  s->locks_.insert(std::make_pair(oid, l));
  lock_acquire_ = [table_id, key, oid, s, tuple,
      fn_write_done](EC ec) {
    s->lock_wait_time_tracer_.end();
    s->trace_state_ = RTS_LOCK_DONE;
    if (ec == EC::EC_OK) {
      std::pair<tuple_pb, bool> r = s->mgr_->get(table_id, key);
      if (r.second) {
        fn_write_done(EC::EC_DUPLICATION_ERROR);
      } else {
        auto fn_read_done = [s, fn_write_done,
            tuple](EC ec, const tuple_pb &) {
          if (ec == EC::EC_OK) {
            fn_write_done(EC::EC_DUPLICATION_ERROR);
          } else if (ec == EC::EC_NOT_FOUND_ERROR) {
            //BOOST_ASSERT(is_tuple_nil(tuple_found));
            fn_write_done(EC::EC_OK);
          } else {
            fn_write_done(ec);
          }
        };

        s->read_data_from_dsb(table_id, key, oid, fn_read_done);
      }
    } else {
      fn_write_done(ec);
    }
  };
  trace_state_ = RTS_WAIT_LOCK;
  trace_message_ += "lk;";
  lock_wait_time_tracer_.begin();
  mgr_->lock_row(xid_, oid, LOCK_WRITE_ROW, table_id, predicate(key), shared_from_this());
}

void tx_context::async_remove(uint32_t table_id, tuple_id_t key,
                              const fn_ec_tuple &fn_removed) {
  oid_t oid = oid_++;
  auto s = shared_from_this();
  ptr<lock_item> l(new lock_item(s->xid_, oid, LOCK_WRITE_ROW, table_id, predicate(key)));
  s->locks_.insert(std::make_pair(oid, l));
  BOOST_ASSERT(lock_acquire_ == nullptr);
  lock_acquire_ = [s, table_id, key, fn_removed](EC ec) {
    s->lock_wait_time_tracer_.end();
    s->trace_state_ = RTS_LOCK_DONE;
    std::pair<tuple_pb, bool> r = s->mgr_->get(table_id, key);
    if (r.second) {
      fn_removed(ec, r.first);
    } else {
      fn_removed(EC::EC_NOT_FOUND_ERROR, tuple_pb());
    }
  };
  trace_state_ = RTS_WAIT_LOCK;
  trace_message_ += "lk;";
  lock_wait_time_tracer_.begin();
  mgr_->lock_row(xid_, oid, LOCK_WRITE_ROW, table_id, predicate(key), shared_from_this());
}

void tx_context::read_data_from_dsb(uint32_t table_id, tuple_id_t key,
                                    uint32_t oid, const fn_ec_tuple &fn_read_done) {
  trace_message_ += "rd dsb;";
  trace_state_ = RTS_WAIT_READ_FROM_DSB;
  ccb_read_request req;
  req.set_source(node_id_);
  req.set_dest(dsb_node_id_);
  req.set_xid(xid_);
  req.set_oid(oid);
  req.set_table_id(table_id);
  req.set_cno(cno_);
  ds_read_handler_[oid] = fn_read_done;
  BOOST_ASSERT(fn_read_done);
  req.set_tuple_id(key);
  BOOST_ASSERT(dsb_node_id_ != 0);
  read_time_tracer_.begin();
  result<void> r = service_->async_send(dsb_node_id_, C2D_READ_DATA_REQ, req);
  if (!r) {
    BOOST_LOG_TRIVIAL(error) << "node " << dsb_node_id_ << " async_send error " << r.error().message();
  }
}

void tx_context::read_data_from_dsb_response(
    EC ec, uint32_t table_id,
    tuple_id_t key,
    uint32_t oid,
    const tuple_pb &data) {
  std::scoped_lock l(mutex_);
  trace_message_ += "dsb rsp;";
  trace_state_ = RTS_READ_FROM_DSB_DONE;
  BOOST_ASSERT(oid != 0);
  auto i = ds_read_handler_.find(oid);
  if (i != ds_read_handler_.end()) {
    fn_ec_tuple fn = i->second;
    if (EC::EC_OK == ec && is_tuple_nil(data)) {
      BOOST_LOG_TRIVIAL(error) << "data null";
    }
    fn(ec, data);
    ds_read_handler_.erase(i);
  } else {
    BOOST_ASSERT(false);
  }
  if (ec == EC::EC_OK && is_tuple_nil(data)) {
    mgr_->put(table_id, key, data);
  }
  read_time_tracer_.end();
}

void tx_context::process_tx_request(const tx_request &req) {
  trace_message_ += "tx rq;";
  trace_state_ = RTS_PROCESS_TX_REQUEST;
  BOOST_LOG_TRIVIAL(debug) << "tx: " << xid_ << ", request ";
  if (req.distributed()) {
    coord_node_id_ = req.source();
  }
  if (req.oneshot()) {
    for (const tx_operation &op: req.operations()) {
      ops_.emplace_back(op);
    }
    handle_next_operation();
  } else {
    // TODO non oneshot tx
  }
}

void tx_context::handle_next_operation() {
  std::scoped_lock l(mutex_);
  if (error_code_ == EC::EC_OK && state_ != RM_ABORTED) {
    if (!ops_.empty()) {
      auto s = shared_from_this();
      auto op_done = [s](EC ec) {
        tx_operation &op = s->ops_.front();
        s->ops_.pop_front();
        if (ec == EC::EC_DUPLICATION_ERROR ||
            op.op_type() == TX_OP_INSERT) {
        } else {
          s->error_code_ = ec;
        }
        s->handle_next_operation();
      };
      tx_operation &op = ops_.front();
      handle_operation(op, op_done);
    } else {
      if (distributed_) {
#ifdef DB_TYPE_SHARE_NOTHING
        if (is_shared_nothing()) {
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
          if (is_geo_rep_optimized()) {
            send_tx_enable_violate();
          }
#endif
          handle_finish_tx_phase1_prepare_commit();
        }
#endif // DB_TYPE_SHARE_NOTHING
      } else {
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
        if (is_geo_rep_optimized()) {
          dlv_make_violable();
        }
#endif // DB_TYPE_GEO_REP_OPTIMIZE
        handle_finish_tx_phase1_commit();
      }
    }
  } else {
    BOOST_LOG_TRIVIAL(trace) << xid_ << " abort , " << enum2str(error_code_);
    if (distributed_) {
#ifdef DB_TYPE_SHARE_NOTHING
      if (is_shared_nothing()) {
        handle_finish_tx_phase1_prepare_abort();
      }
#endif // DB_TYPE_SHARE_NOTHING
    } else {
      handle_finish_tx_phase1_abort();
    }
  }
}

void tx_context::handle_operation(tx_operation &op, const fn_ec &op_done) {
  trace_message_ += "h op;";
  trace_state_ = RTS_HANDLE_OPERATION;
  switch (op.op_type()) {
  case TX_OP_READ:
  case TX_OP_READ_FOR_WRITE: {
    table_id_t table_id = op.tuple_row().table_id();
    tuple_id_t key = op.tuple_row().tuple_id();
    auto s = shared_from_this();
    auto read_done = [s, table_id, key, op_done](EC ec,
                                                 const tuple_pb &tuple) {
      if (ec == EC::EC_NOT_FOUND_ERROR) {
        tuple_id_t tid = (key);
        BOOST_ASSERT(tid == 0);
        BOOST_LOG_TRIVIAL(trace)
          << s->node_name_ << " cannot find, table_id=" << table_id
          << ", tuple_id=" << tid;
      }
      BOOST_ASSERT(not(ec == EC::EC_OK && is_tuple_nil(tuple)));
      tx_operation *op_response = s->response_.add_operations();
      *op_response->mutable_tuple_row()->mutable_tuple() = (tuple);
      BOOST_LOG_TRIVIAL(trace) << "handle read table " << table_id << "  ";
      op_done(ec);
    };
    bool read_for_write = op.op_type() == TX_OP_READ_FOR_WRITE;
    async_read(table_id, key, read_for_write, read_done);
    return;
  }
  case TX_OP_UPDATE: {
    table_id_t table_id = op.tuple_row().table_id();
    tuple_id_t key = op.tuple_row().tuple_id();
    const tuple_pb tuple = op.tuple_row().tuple();
    auto s = shared_from_this();
    auto update_done = [s, op, table_id, key, tuple, op_done](EC ec) {
      if (ec == EC::EC_NOT_FOUND_ERROR) {
        BOOST_LOG_TRIVIAL(debug)
          << s->node_name_ << " cannot find, table_id=" << table_id
          << ", tuple_id=" << (key);
      }
      BOOST_LOG_TRIVIAL(debug)
        << "handle update table " << table_id << " tuple: ";
      s->append_operation(op);
      op_done(ec);
    };
    async_update(table_id, key, op.tuple_row().tuple(), update_done);
    return;
  }

  case TX_OP_INSERT: {
    table_id_t table_id = op.tuple_row().table_id();
    tuple_id_t key = op.tuple_row().tuple_id();
    const tuple_pb tuple = op.tuple_row().tuple();
    auto s = shared_from_this();
    auto insert_done = [s, op, table_id, key, tuple, op_done](EC ec) {
      if (ec == EC::EC_DUPLICATION_ERROR) {
        BOOST_LOG_TRIVIAL(debug) << s->node_name_ << " find, table_id=" << table_id
                                 << ", tuple=" << (key);
      }
      BOOST_LOG_TRIVIAL(debug)
        << "handle insert table " << table_id << " tuple ";
      s->append_operation(op);
      op_done(ec);
    };
    async_insert(table_id, key, op.tuple_row().tuple(), insert_done);
    return;
  }
  default:BOOST_ASSERT(false);
  }
}

void tx_context::send_tx_response() {
  if (has_responsed_) {
    return;
  }
  if (fn_tx_state_) {
    fn_tx_state_(xid_, state_);
  }
  part_time_tracer_.end();

  BOOST_ASSERT(cli_conn_);
  response_.set_error_code(uint32_t(error_code_));
  //BOOST_LOG_TRIVIAL(info) << append_time_tracer_.duration().total_microseconds() << "us";
  response_.set_latency_append(append_time_tracer_.duration().total_microseconds());
  response_.set_latency_read(read_time_tracer_.duration().total_microseconds());
  response_.set_latency_lock_wait(lock_wait_time_tracer_.duration().total_microseconds());
  response_.set_latency_replicate(log_rep_delay_);
  response_.set_latency_part(part_time_tracer_.duration().total_microseconds());
  response_.set_access_part(1);
  result<void> r = cli_conn_->async_send(CLIENT_TX_RESP, response_);
  if (!r) {
    BOOST_LOG_TRIVIAL(trace) << "send client response error";
  }
}

void tx_context::abort_tx_1p() {
  std::scoped_lock l(mutex_);
  if (state_ == rm_state::RM_IDLE) {
    state_ = rm_state::RM_ABORTED;
    set_tx_cmd_type(TX_CMD_RM_ABORT);
    BOOST_LOG_TRIVIAL(trace) << "transaction RM " << xid_ << "phase1 aborted";
    async_force_log();
  } else if (state_ == rm_state::RM_ABORTED) {
    send_tx_response();
  } else {
    BOOST_ASSERT(false);
  }
}

void tx_context::on_committed_log_commit() {
  commit_log_synced_ = true;
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  if (is_geo_rep_optimized()) {
    dlv_try_tx_commit();
  }
#endif
#ifdef DB_TYPE_NON_DETERMINISTIC
  tx_committed();
#endif
}

void tx_context::on_log_entry_commit(tx_cmd_type type) {
  std::scoped_lock l(mutex_);
  trace_message_ += "lg cmt;";
  log_entry_.clear();
  switch (type) {
  case TX_CMD_RM_COMMIT: {
    append_time_tracer_.end();
    on_committed_log_commit();
    break;
  }
  case TX_CMD_RM_ABORT: {
    on_aborted_log_commit();
    break;
  }
#ifdef  DB_TYPE_SHARE_NOTHING
  case TX_CMD_RM_PREPARE_ABORT: {
    on_prepare_aborted_log_commit();
    break;
  }
  case TX_CMD_RM_PREPARE_COMMIT: {
    on_prepare_committed_log_commit();
    break;
  }
#endif // DB_TYPE_SHARE_NOTHING
  default:break;
  }
}

void tx_context::on_aborted_log_commit() {
  tx_aborted();
}

void tx_context::tx_committed() {
  if (!distributed_) {
    trace_message_ += "tx C;";
    trace_state_ = RTS_FORCE_LOG_DONE;
    BOOST_LOG_TRIVIAL(debug) << "tx: " << xid_ << ", commit ";
    send_tx_response();
    release_lock();
  } else {
#ifdef DB_TYPE_SHARE_NOTHING
    if (is_shared_nothing()) {
      trace_state_ = RTS_FORCE_LOG_DONE;
      trace_message_ += "tx C;";
      BOOST_LOG_TRIVIAL(debug)
        << "tx TM : " << xid_ << ", phase 2 commit: ";
      send_ack_message(true);
      release_lock();
    }
#endif // DB_TYPE_SHARE_NOTHING
  }
}

void tx_context::tx_aborted() {
  if (!distributed_) {
    trace_message_ += "tx A;";
    trace_state_ = RTS_FORCE_LOG_DONE;
    BOOST_LOG_TRIVIAL(debug) << "tx RM : " << xid_ << ", phase 1 abort: ";
    if (error_code_ == EC::EC_OK) {
      error_code_ = EC::EC_TX_ABORT;
    }
    send_tx_response();
    release_lock();
  } else {
#ifdef DB_TYPE_SHARE_NOTHING
    if (is_shared_nothing()) {
      trace_state_ = RTS_FORCE_LOG_DONE;
      BOOST_LOG_TRIVIAL(debug) << "tx TM : " << xid_ << ", phase 2 abort: ";
      send_ack_message(false);
      release_lock();
    }
#endif // DB_TYPE_SHARE_NOTHING
  }
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  dlv_abort();
#endif // DB_TYPE_GEO_REP_OPTIMIZE
}

rm_state tx_context::state() const {
  std::scoped_lock l(const_cast<std::recursive_mutex &>(mutex_));
  return state_;
}

void tx_context::release_lock() {
  if (victim_) {
    trace_message_ += "release;";
  } else {
    trace_message_ = "";
  }

  for (const auto &kv: locks_) {
    ptr<lock_item> l = kv.second;
    mgr_->unlock(l->xid(), l->type(), l->table_id(), l->get_predicate());
  }
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  if (is_geo_rep_optimized()) {
    report_dependency();
  }
#endif
  dl_->tx_finish(xid_);
  //locks_.clear();
}

void tx_context::async_force_log() {
  trace_message_ += "fc lg;";
  trace_state_ = RTS_WAIT_FORCE_LOG;
  append_time_tracer_.begin();
  wal_->async_append(log_entry_);
  log_entry_.clear();
}

void tx_context::append_operation(const tx_operation &op) {
  if (log_entry_.empty()) {
    log_entry_.emplace_back();
  }

  tx_operation *o = log_entry_.rbegin()->add_operations();
  *o = op;
  o->set_xid(xid_);
  o->set_rg_id(TO_RG_ID(node_id_));
}

void tx_context::set_tx_cmd_type(tx_cmd_type type) {
  if (log_entry_.empty()) {
    log_entry_.emplace_back();
  }
  BOOST_ASSERT(not log_entry_.empty());
  tx_log &log = *log_entry_.rbegin();
  log.set_xid(xid_);
  log.set_log_type(type);
}

void tx_context::handle_finish_tx_phase1_commit() {
  std::scoped_lock l(mutex_);
  if (state_ == rm_state::RM_IDLE ||
      state_ == rm_state::RM_PREPARE_COMMIT) {
    state_ = rm_state::RM_COMMITTED;

    set_tx_cmd_type(TX_CMD_RM_COMMIT);

    BOOST_LOG_TRIVIAL(trace) << "transaction RM " << xid_ << " commit";
    async_force_log();
  } else if (state_ == rm_state::RM_COMMITTED) {
    send_tx_response();
  } else {
    BOOST_ASSERT(false);
  }
}

void tx_context::handle_finish_tx_phase1_abort() {
  abort_tx_1p();
}

void tx_context::add_dependency(const ptr<tx_wait_set> &ds) {
  dl_->add_wait_set(*ds);
}

void tx_context::async_wait_lock(fn_wait_lock fn) {
  dl_->async_wait_lock(fn);
}

void tx_context::abort(EC ec) {
  std::scoped_lock l(mutex_);
  if (ec == EC_DEADLOCK) {
    victim_ = true;
    trace_message_ += "victim;";
  }
  if (!distributed_) {
    if (state_ == RM_IDLE) {
      error_code_ = ec;
      abort_tx_1p();
    }
  }
}

#ifdef DB_TYPE_SHARE_NOTHING

void tx_context::on_prepare_committed_log_commit() {
  prepare_commit_log_synced_ = true;
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  if (is_geo_rep_optimized()) {
    dlv_try_tx_prepare_commit();
  }
#endif //DB_TYPE_GEO_REP_OPTIMIZE
#ifdef DB_TYPE_NON_DETERMINISTIC
  tx_prepare_committed();
#endif
}

void tx_context::on_prepare_aborted_log_commit() {
  tx_prepare_aborted();
}

void tx_context::tx_prepare_committed() {
  trace_state_ = RTS_FORCE_LOG_DONE;
  trace_message_ += "tx PC;";
  BOOST_LOG_TRIVIAL(debug) << "tx: " << xid_ << ", prepare commit: ";

  send_prepare_message(true);
}

void tx_context::tx_prepare_aborted() {
  trace_state_ = RTS_FORCE_LOG_DONE;
  trace_message_ = "tx PA;";
  BOOST_LOG_TRIVIAL(debug) << "tx: " << xid_ << ", prepare abort: ";
  send_prepare_message(false);
}

void tx_context::abort_tx_2p() {
  std::scoped_lock l(mutex_);
  trace_message_ += "tx P2;";
  if (state_ == rm_state::RM_IDLE ||
      state_ == rm_state::RM_PREPARE_ABORT ||
      state_ == rm_state::RM_PREPARE_COMMIT) {
    state_ = rm_state::RM_ABORTED;

    set_tx_cmd_type(TX_CMD_RM_ABORT);
    BOOST_LOG_TRIVIAL(trace) << "transaction RM " << xid_ << "phase2 aborted";
    async_force_log();
  } else if (state_ == RM_ABORTED) {
    send_ack_message(false);
  } else {
    BOOST_ASSERT_MSG(false, "error state");
  }
}

void tx_context::handle_finish_tx_phase1_prepare_commit() {
  prepare_commit_tx();
  auto s = shared_from_this();
  async_force_log();
}

void tx_context::handle_finish_tx_phase1_prepare_abort() {
  prepare_abort_tx();
  async_force_log();
}

void tx_context::prepare_commit_tx() {
  std::scoped_lock l(mutex_);
  if (state_ == rm_state::RM_IDLE) {
    state_ = rm_state::RM_PREPARE_COMMIT;
    tx_operation prepare_commit_op;

    set_tx_cmd_type(TX_CMD_RM_PREPARE_COMMIT);
    BOOST_LOG_TRIVIAL(trace) << "transaction RM " << xid_ << " preapre commit";
  }
}

void tx_context::prepare_abort_tx() {
  std::scoped_lock l(mutex_);
  state_ = rm_state::RM_PREPARE_ABORT;
  tx_operation prepare_commit_op;

  set_tx_cmd_type(TX_CMD_RM_PREPARE_ABORT);
  BOOST_LOG_TRIVIAL(trace) << "transaction RM " << xid_ << " preapre commit";
}

void tx_context::send_prepare_message(bool commit) {
  part_time_tracer_.end();
  tx_rm_prepare msg;
  msg.set_xid(xid_);
  msg.set_source_node(node_id_);
  msg.set_source_rg(TO_RG_ID(node_id_));
  msg.set_dest_node(coord_node_id_);
  msg.set_dest_rg(TO_RG_ID(coord_node_id_));
  msg.set_commit(commit);
  msg.set_latency_append(append_time_tracer_.duration().total_microseconds());
  msg.set_latency_read(read_time_tracer_.duration().total_microseconds());
  msg.set_latency_lock_wait(lock_wait_time_tracer_.duration().total_microseconds());
  msg.set_latency_replicate(log_rep_delay_);
  msg.set_latency_part(part_time_tracer_.duration().total_microseconds());
  result<void> r = service_->async_send(coord_node_id_, TX_RM_PREPARE, msg);
  if (!r) {
    BOOST_LOG_TRIVIAL(error) << "async send Prepare error";
  }
}

void tx_context::send_ack_message(bool commit) {
  tx_rm_ack msg;
  msg.set_xid(xid_);
  msg.set_source_node(node_id_);
  msg.set_source_rg(TO_RG_ID(node_id_));
  msg.set_dest_node(coord_node_id_);
  msg.set_dest_rg(TO_RG_ID(coord_node_id_));
  msg.set_commit(commit);
  result<void> r = service_->async_send(coord_node_id_, TX_RM_ACK, msg);
  if (!r) {
    BOOST_LOG_TRIVIAL(error) << "async send ACK error";
  }
}

void tx_context::handle_tx_tm_commit(const tx_tm_commit &msg) {
  BOOST_ASSERT(msg.xid() == xid_);
  if (msg.xid() != xid_) {
    return;
  }
  handle_finish_tx_phase2_commit();
}
void tx_context::handle_tx_tm_abort(const tx_tm_abort &msg) {
  BOOST_ASSERT(msg.xid() == xid_);
  if (msg.xid() != xid_) {
    return;
  }
  handle_finish_tx_phase2_abort();
}

void tx_context::handle_finish_tx_phase2_commit() {
  std::scoped_lock l(mutex_);
  trace_message_ += "h C2;";
  if (state_ == rm_state::RM_PREPARE_COMMIT) {
    state_ = rm_state::RM_COMMITTED;
    set_tx_cmd_type(TX_CMD_RM_COMMIT);
    BOOST_LOG_TRIVIAL(trace) << "transaction RM " << xid_ << " commit";
    async_force_log();
  } else if (state_ == rm_state::RM_COMMITTED) {
    send_ack_message(true);
  } else {
    BOOST_ASSERT(false);
  }
}

void tx_context::handle_finish_tx_phase2_abort() {
  abort_tx_2p();
}

#ifdef DB_TYPE_GEO_REP_OPTIMIZE

void tx_context::register_dependency(const ptr<tx_context> &out) {
  if (xid_ == out->xid_) {
    BOOST_LOG_TRIVIAL(error) << "cannot register the same transaction";
    return;
  }
  if (xid_ < out->xid_) {
    mutex_.lock();
    out->mutex_.lock();
  } else {
    out->mutex_.lock();
    mutex_.lock();
  }
  do {
    if (out->state_ == RM_ABORTED || out->state_ == RM_COMMITTED) {
      break;
    }
    if (state_ == RM_COMMITTED || state_ == RM_ABORTED) {
      break;
    } else {
      auto i = dep_out_set_.find(out->xid_);
      if (i == dep_out_set_.end()) {
        out->dep_in_count_++;
        dep_out_set_[out->xid_] = out;
        out->dep_in_set_[xid_] = shared_from_this();
      }
    }
  } while (false);

  if (xid_ < out->xid_) {
    out->mutex_.lock();
    mutex_.lock();
  } else {
    mutex_.lock();
    out->mutex_.lock();
  }
}

void tx_context::report_dependency() {
  mutex_.lock();
  for (const auto &kv: dep_out_set_) {
    ptr<tx_context> ctx = kv.second;
    xid_t xid = xid_;
    auto fn = [ctx, xid]() {
      ctx->mutex_.lock();
      auto i = ctx->dep_in_set_.find(xid);
      if (i != ctx->dep_in_set_.end()) {
        if (ctx->dep_in_count_ > 0) {
          ctx->dep_in_count_--;
          if (ctx->dep_in_count_ == 0) {
            auto fn = [ctx]() {
              ctx->dependency_commit();
            };
            boost::asio::dispatch(ctx->service_->get_service(SERVICE_HANDLE), fn);
          }
        }
      }
      ctx->mutex_.unlock();
    };
    boost::asio::dispatch(service_->get_service(SERVICE_HANDLE), fn);
  }
  mutex_.unlock();
}

void tx_context::dependency_commit() {
  std::scoped_lock l(mutex_);
  if (distributed_) {
    dlv_try_tx_prepare_commit();
  } else {
    dlv_try_tx_commit();
  }
}

void tx_context::dlv_try_tx_commit() {
  trace_message_ += "dlv try C;";
  if (dependency_committed_ && commit_log_synced_) {
    tx_committed();
  }
}

void tx_context::dlv_try_tx_prepare_commit() {
  trace_message_ += "dlv try PC;";
  if (dependency_committed_ && prepare_commit_log_synced_) {
    tx_prepare_committed();
  }
}

void tx_context::dlv_abort() {
  if (is_geo_rep_optimized()) {
    trace_message_ += "dlv A;";
    for (const auto &kv: dep_out_set_) {
      kv.second->dlv_abort();
    }
    if (dep_in_count_ > 0) {
      error_code_ = EC::EC_CASCADE;
    }
  }
}

void tx_context::dlv_make_violable() {
  trace_message_ += "dlv V;";
  for (const auto &l: locks_) {
    mgr_->make_violable(l.second->xid(),
                        l.second->type(),
                        l.second->table_id(),
                        l.second->key());
  }
}

void tx_context::handle_tx_enable_violate() {
  dlv_make_violable();
}

void tx_context::send_tx_enable_violate() {
  tx_enable_violate msg;
  msg.set_source(node_id_);
  msg.set_dest(coord_node_id_);
  msg.set_violable(true);
  auto r = service_->async_send(msg.dest(), RM_ENABLE_VIOLATE, msg);
  if (!r) {
    BOOST_LOG_TRIVIAL(error) << "report RM enable violate " << msg.violable();
  }
}
#endif // DB_TYPE_GEO_REP_OPTIMIZE

#endif // DB_TYPE_SHARE_NOTHING

void tx_context::timeout_clean_up() {
  boost::posix_time::ptime now = boost::posix_time::microsec_clock::local_time();
  if ((now - start_).total_milliseconds() < 1000) {
    return;
  }

  std::scoped_lock l(mutex_);
  if (state_ == rm_state::RM_PREPARE_COMMIT
      || state_ == rm_state::RM_COMMITTED) {
    return;
  } else {
    if (!distributed_) {
      abort_tx_1p();
      send_tx_response();

    } else {
#ifdef DB_TYPE_SHARE_NOTHING
      if (is_shared_nothing()) {
        abort_tx_2p();
      }
#endif // DB_TYPE_SHARE_NOTHING
    }
  }
}

void tx_context::debug_tx(std::ostream &os) {
  os << "RM: " << xid_ << " state: " << enum2str(state_) << std::endl;
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  if (is_geo_rep_optimized()) {
    os << "    ->" << "in dependency: " << dep_in_count_
       << " prepared: " << prepare_commit_log_synced_
       << " committed: " << commit_log_synced_ << std::endl;
  }
#endif //DB_TYPE_GEO_REP_OPTIMIZE
  os << "    -> trace: " << trace_message_ << std::endl;

}


void tx_context::log_rep_delay(uint64_t us) {
  log_rep_delay_ = us;
}
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC