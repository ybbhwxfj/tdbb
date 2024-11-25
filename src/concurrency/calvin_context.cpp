#include "concurrency/calvin_context.h"
#include "common/berror.h"
#include "common/tx_log.h"
#include "common/utils.h"
#include "common/shard2node.h"
#include <utility>

#ifdef DB_TYPE_CALVIN

calvin_context::calvin_context(boost::asio::io_context::strand s, xid_t xid,
                               node_id_t node_id, std::optional<node_id_t> dsb_node_id,
                               const std::unordered_map<shard_id_t, node_id_t> &shard2node,
                               uint64_t cno, ptr<tx_request> req,
                               net_service *service, lock_mgr_global *access,
                               fn_calvin_context_remove fn_remove)
    : tx_rm(s, xid), xid_(xid), node_id_(node_id),
      ctx_opt_dsb_node_id_(dsb_node_id),
      dsb_node_ids_(shard2node),
      collector_id_(req->source()), cno_(cno), ops_request_(req),
      service_(service), access_(access), log_committed_(false),
      num_lock_ok_(0), num_read_resp_(0), fn_remove_(std::move(fn_remove)),
      read_only_(false),
      commit_(false),
      timeout_invoked_(false) {
  start_ms_ = steady_clock_ms_since_epoch();
  BOOST_ASSERT(ops_request_);
  BOOST_ASSERT(collector_id_ != 0);
  BOOST_ASSERT(ops_request_->operations_size() != 0);
}

void calvin_context::async_lock_acquire(EC ec, oid_t oid) {
  auto ctx = shared_from_this();
  auto fn = [ctx, ec, oid] {
    scoped_time _t("calvin_context::async_lock_acquire");
#ifdef TX_TRACE
    ctx->trace_message_ << boost::format("lk ok %d;") % oid;
    ctx->num_lock_ok_ += 1;
#endif
    auto i = ctx->callback_.find(oid);
    if (i != ctx->callback_.end()) {
      i->second(ec);
    }
  };
  boost::asio::post(ctx->get_strand(), fn);
}

void calvin_context::begin() {
#ifdef TX_TRACE
  auto rm = shared_from_this();
  auto fn_timeout = [rm] {
    if (rm->timeout_invoked_) {
      return;
    }

    auto ms = steady_clock_ms_since_epoch();

    if (ms < rm->start_ms_ + TX_TIMEOUT_MILLIS) {
      std::string trace = rm->trace_message_.str();
      if (rm->commit_) {
        rm->timer_ticker_->cancel();
        return;
      }

      rm->timeout_invoked_ = true;
      LOG(warning) <<
          " tx: " << ms - rm->start_ms_ <<
          " wait ms, " << rm->xid_ <<
          " trace" << trace;
    }
  };
  ptr<timer> t(new timer(
      get_strand(), boost::asio::chrono::milliseconds(TX_TIMEOUT_MILLIS),
      fn_timeout));
  timer_ticker_ = t;
  t->async_tick();
#endif
}
bool calvin_context::on_operation_done(const tx_operation &op,
                                       const tuple_pb &tp) {
  ptr<tx_operation> o(new tx_operation(op));
  *o->mutable_tuple_row()->mutable_tuple() = tp;
  op_response_.insert(std::make_pair(o->operation_id(), o));
  send_read(op);
#ifdef TX_TRACE
  trace_message_ << boost::format("rd_dsb o:%d k:%d;") % op.operation_id() %
                        op.tuple_row().tuple_id();
#endif
  return true;
}

bool calvin_context::on_operation_committed(const tx_log_proto &log) {
#ifdef TX_TRACE
  trace_message_ << "op cmt;";
#endif
  if (log.log_type() == tx_cmd_type::TX_CMD_RM_COMMIT) {
    // BOOST_ASSERT(op.log_type() == TX_CMD_RM_COMMIT);
    log_committed_ = true;
    return tx_commit();
  } else {
    return false;
  }
}

bool calvin_context::tx_commit() {
  BOOST_ASSERT(ops_request_->operations_size() > 0);
  if (size_t(ops_request_->operations_size()) > op_read_.size()) {
    return false;
  }

  if (!log_committed_ && !read_only_) {
    return false;
  }
  // respond
  // tx_rm commit
#ifdef TX_TRACE
  trace_message_ << "tx_rm cmt;";
#endif
  // release locks ...
  for (const tx_operation &op : ops_request_->operations()) {
    lock_mode lm = op_type_to_lock_mode(op.op_type());
    access_->unlock(xid_, lm,
                    op.tuple_row().table_id(),
                    op.tuple_row().shard_id(),
                    predicate(op.tuple_row().tuple_id()));
  }

  auto msg = std::make_shared<calvin_part_commit>();
  msg->set_source(node_id_);
  msg->set_dest(collector_id_);
  msg->set_xid(xid_);
  auto r = service_->async_send(collector_id_, CALVIN_PART_COMMIT, msg, true);
  if (!r) {
    LOG(error) << "send calvin part commit failed , "
               << ec2string(r.error().code());
  }
  LOG(trace) << id_2_name(node_id_) << " calvin part commit " << xid_;

  commit_ = true;
  fn_remove_(xid_);
  return true;
}

void calvin_context::add_lock_acquire_callback(oid_t oid, fn_lock_callback fn) {
#ifdef TX_TRACE
  trace_message_ << boost::format("lk %d;") % oid;
#endif
  callback_[oid] = std::move(fn);
}

void calvin_context::debug_tx(std::ostream &os) const {
  os << "calvin RM " << xid_ << " committed log: " << log_committed_
     << " ops: " << ops_request_->operations_size()
     << " respond: " << op_read_.size() << " lock ok: " << num_lock_ok_
     << " read resp: " << num_read_resp_ << std::endl;
  os << "    trace :" << trace_message_.str() << std::endl;
}

void calvin_context::send_read(const tx_operation &op) {
  auto read = std::make_shared<ccb_read_request>();
  read->set_xid(xid_);
  read->set_source(node_id_);
  node_id_t dest_node_id = shard2node(op.tuple_row().shard_id());

  read->set_dest(dest_node_id);
  read->set_cno(cno_);
  read->set_oid(op.operation_id());
  read->set_table_id(op.tuple_row().table_id());
  read->set_tuple_id(op.tuple_row().tuple_id());

  auto r = service_->async_send(dest_node_id, C2D_READ_DATA_REQ, read, true);
  if (not r) {
    LOG(error) << "read data request error tuple_id:"
               << op.tuple_row().tuple_id() << std::endl;
  }
}

void calvin_context::read_response(const dsb_read_response &res) {
  oid_t oid = res.oid();
#ifdef TX_TRACE
  trace_message_ << boost::format("rd_resp o:%d, k:%d;") % oid %
                        res.tuple_row().tuple_id();
  num_read_resp_ += 1;
#endif
  auto i = op_response_.find(oid);
  if (i != op_response_.end()) {
    op_read_.insert(oid);
    if (res.error_code() == EC::EC_OK && res.has_tuple_row()) {
      *i->second->mutable_tuple_row() = res.tuple_row();
    }
  } else {
    BOOST_ASSERT(false);
  }

  tx_commit();
}

node_id_t calvin_context::shard2node(shard_id_t shard_id) {
  return node_id_of_shard(shard_id, ctx_opt_dsb_node_id_, dsb_node_ids_);
}
#endif // DB_TYPE_CALVIN