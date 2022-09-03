#include "concurrency/calvin_scheduler.h"

#include <utility>
#ifdef DB_TYPE_CALVIN

calvin_scheduler::calvin_scheduler(
    fn_calvin_context_find fn_find,
    const config &conf,
    access_mgr *lm,
    write_ahead_log *wal,
    net_service *service) :
    fn_find_(std::move(fn_find)),
    conf_(conf),
    node_id_(conf.node_id()),
    access_mgr_(lm),
    wal_(wal),
    service_(service) {

}

void calvin_scheduler::schedule(const ptr<calvin_epoch_ops> &e) {
  BOOST_LOG_TRIVIAL(trace) << id_2_name(conf_.node_id()) << " schedule epoch " << e->epoch_ << " ";
#ifdef TX_TRACE
  trace_message_ = "sch " + std::to_string(e->epoch_) + ";";
  trace_message_ += "[";
  for (const auto &p: e->reqs_) {
    trace_message_ += std::to_string(p->xid()) + " ";
  }
  trace_message_ += "]";
#endif
  uint64_t epoch = e->epoch_;
  //BOOST_LOG_TRIVIAL(trace) << id_2_name(conf_.node_id()) << " handle epoch " << epoch << " ops: " << e->ops_.size();
  if (e->reqs_.empty()) {
    send_ack(epoch, e->node_ids_);
    return;
  }
  std::vector<tx_log> entry;
  std::unordered_map<xid_t, ptr<calvin_context>> ctx_set;
  entry.resize(e->reqs_.size());
  uint64_t ops = 0;
  for (size_t i = 0; i < e->reqs_.size(); i++) {
    ptr<tx_request> req = e->reqs_[i];
    tx_log *xlog = &entry[i];
    uint64_t num_write_ops = 0;
    uint64_t num_read_op = 0;
    for (const tx_operation &op : req->operations()) {
      if (is_write_operation(op.op_type())) {
        *xlog->add_operations() = op;
        num_write_ops++;
      } else {
        num_read_op++;
      }
    }
    bool read_only = num_write_ops == 0;
    ops += num_write_ops;
    ops += num_read_op;
    xlog->set_xid(req->xid());
    xlog->set_log_type(TX_CMD_RM_COMMIT);

    create_or_find(ctx_set, req, read_only);
  }
  //BOOST_LOG_TRIVIAL(trace) << id_2_name(conf_.node_id()) << " write operations " << entry.operation().size();
  wal_->async_append(entry);
  /*
  for (auto & c : ctx_set) {
    BOOST_LOG_TRIVIAL(info) << "calvin async append log " << c.second->xid();
    c.second->trace_message_ += "app logs;";
  }*/
  ptr<std::atomic<uint64_t>> num_op(new std::atomic(ops));
  for (const ptr<tx_request> &req : e->reqs_) {
    auto tx = ctx_set[req->xid()];
    for (const tx_operation &op : req->operations()) {
      lock_mode lt;
      if (op.op_type() == TX_OP_DELETE ||
          op.op_type() == TX_OP_INSERT ||
          op.op_type() == TX_OP_UPDATE ||
          op.op_type() == TX_OP_READ_FOR_WRITE) {
        lt = LOCK_WRITE_ROW;
      } else if (op.op_type() == TX_OP_READ) {
        lt = LOCK_READ_ROW;
      } else {
        (*num_op)--;
        assert(false);
        BOOST_LOG_TRIVIAL(error) << " unknown tx_rm op types";
        continue;
      }

      // TODO ... calvin lock acquire done
      auto fn = [this, tx, num_op, op, epoch, e](EC ec) {
        if (ec != EC::EC_OK) {
          BOOST_ASSERT(false);
        }

        //BOOST_ASSERT(ec == EC::EC_OK);
        (*num_op)--;
        if (num_op->load() == 0) {
          send_ack(epoch, e->node_ids_);
        }
        tx_op_done(tx, op);
      };
      tx->add_lock_acquire_callback(op.operation_id(), fn);
      access_mgr_->lock_row(
          op.xid(),
          op.operation_id(),
          lt,
          op.tuple_row().table_id(),
          predicate(op.tuple_row().tuple_id()),
          tx
      );
    }
  }
}

void calvin_scheduler::tx_op_done(const ptr<calvin_context> &tx, const tx_operation &op) {
  tuple_pb tuple;
  bool committed = tx->on_operation_done(op, tuple);
  if (committed) {

  }
}

void calvin_scheduler::send_ack(uint64_t epoch, const std::set<node_id_t> &ids) {
#ifdef TX_TRACE
  trace_message_ += "s ack " + std::to_string(epoch) + ";";
#endif
  for (auto id : ids) {
    auto msg = std::make_shared<calvin_epoch_ack>();
    msg->set_source(node_id_);
    msg->set_dest(id);
    msg->set_epoch(epoch);
    auto r = service_->async_send(msg->dest(), CALVIN_EPOCH_ACK, msg);
    if (!r) {
      BOOST_LOG_TRIVIAL(error) << "async send calvin epoch ack error";
    }
  }
}

ptr<calvin_context> calvin_scheduler::create_or_find(
    std::unordered_map<xid_t, ptr<calvin_context>> &ctx_set,
    const ptr<tx_request> &o,
    bool read_only
) {
  ptr<calvin_context> calvin_ctx;
  if (ctx_set.contains(o->xid())) {
    calvin_ctx = ctx_set[o->xid()];
  } else {
    calvin_ctx = fn_find_(*o);
    if (calvin_ctx) {
      if (read_only) {
        calvin_ctx->set_read_only();
      }
      ctx_set.insert(std::make_pair(calvin_ctx->xid(), calvin_ctx));
    }
  }
  BOOST_ASSERT(calvin_ctx);
  return calvin_ctx;
}
void calvin_scheduler::debug_tx(std::ostream &os) {
  os << "calvin_scheduler:" << std::endl;
  os << "calvin_scheduler trace message :";
  os << "    " << trace_message_ << std::endl;
}
#endif // DB_TYPE_CALVIN