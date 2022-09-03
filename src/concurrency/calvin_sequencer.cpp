
#include "concurrency/calvin_sequencer.h"
#ifdef DB_TYPE_CALVIN
#include <algorithm>
#include <memory>
#include <utility>
#include "common/variable.h"
#include "common/define.h"

template<> enum_strings<epoch_state>::e2s_t enum_strings<epoch_state>::enum2str = {
    {EPOCH_IDLE, "EPOCH_IDLE"},
    {EPOCH_PENDING, "EPOCH_PENDING"},
};

calvin_sequencer::calvin_sequencer(
    config conf,
    net_service *service,
    fn_scheduler fn) :
    conf_(std::move(conf)),
    epoch_(1),
    service_(service),
    scheduler_(std::move(fn)),
    state_(EPOCH_IDLE),
    is_lead_(false),
    handle_tx_(false),
    timer_strand_(service->get_service(SERVICE_ASYNC_CONTEXT)) {

}

void calvin_sequencer::handle_tx_request(const tx_request &tx) {
  std::scoped_lock l(mutex_);
#ifdef TX_TRACE
  trace_message_ += "h tx_rm;";
#endif
  if (not handle_tx_) {
    handle_tx_ = true;
  }
  for (const tx_operation &o : tx.operations()) {
    auto i = tx_request_next_epoch_.find(o.rg_id());
    if (i == tx_request_next_epoch_.end()) {
      ptr<tx_request> t(new tx_request());
      t->set_terminal_id(tx.terminal_id());
      t->set_xid(tx.xid());
      t->set_source(conf_.node_id());
      *t->add_operations() = o;
      tx_request_next_epoch_[o.rg_id()][tx.xid()] = t;
    } else {
      auto ii = i->second.find(tx.xid());
      if (ii == i->second.end()) {
        ptr<tx_request> t(new tx_request());
        t->set_terminal_id(tx.terminal_id());
        t->set_xid(tx.xid());
        t->set_source(conf_.node_id());
        *t->add_operations() = o;
        i->second[tx.xid()] = t;
      } else {
        *ii->second->add_operations() = o;
      }
    }
  }
}

void calvin_sequencer::tick() {
  ptr<boost::asio::steady_timer> timer_tick_;
  timer_tick_.reset(new boost::asio::steady_timer(
      service_->get_service(SERVICE_ASYNC_CONTEXT),
      boost::asio::chrono::milliseconds(conf_.get_tpcc_config().calvin_epoch_ms())));
  auto fn_timeout = boost::asio::bind_executor(
      timer_strand_,
      [timer_tick_, this](const boost::system::error_code &error) {
        if (not error.failed()) {
          tick();
        } else {
          BOOST_LOG_TRIVIAL(error) << " async wait error " << error.message();
        }
      });
  timer_tick_->async_wait(fn_timeout);

  epoch_broadcast_request();
}

void calvin_sequencer::update_shard_lead(shard_id_t shard_id, node_id_t node_id) {
  std::scoped_lock l(mutex_);
  node_id_[shard_id] = node_id;
}

void calvin_sequencer::update_local_lead_state(bool lead) {
  std::scoped_lock l(mutex_);
  is_lead_ = lead;
}

void calvin_sequencer::epoch_broadcast_request() {
  std::scoped_lock l(mutex_);
  if (node_id_.size() != conf_.num_rg()) {
    return;
  }
  if (not is_lead_ || not handle_tx_) {
    return;
  }

  if (state_ == EPOCH_IDLE) {
    trace_message_ = "next epoch;";

    local_epoch_.reset(new local_epoch_ctx());
    local_epoch_->epoch_ = epoch_;

    BOOST_ASSERT(local_epoch_->epoch_ == epoch_);

    state_ = EPOCH_PENDING;

    for (auto kv : node_id_) {
      if (local_epoch_->ack_.contains(kv.first)) {
        BOOST_ASSERT(false);
        continue;
      }
      node_id_t nid = kv.second;
      auto msg = std::make_shared<calvin_epoch>();

      msg->set_dest(nid);
      msg->set_source(conf_.node_id());
      msg->set_epoch(local_epoch_->epoch_);
      shard_id_t sid = TO_RG_ID(nid);
      auto i1 = tx_request_next_epoch_.find(sid);
      if (i1 != tx_request_next_epoch_.end()) {
        for (std::pair<xid_t, ptr<tx_request>> r : i1->second) {
          *msg->add_request() = *r.second;
        }
      }
      BOOST_LOG_TRIVIAL(trace) << id_2_name(conf_.node_id()) << " send calvin epoch " << local_epoch_->epoch_ << " to "
                               << id_2_name(nid);
      auto r = service_->async_send(nid, CALVIN_EPOCH, msg);
      if (!r) {
        BOOST_LOG_TRIVIAL(error) << "async send calvin epoch error...";
      }
    }
    tx_request_next_epoch_.clear();
    epoch_++;
  }
}

void calvin_sequencer::handle_epoch(const calvin_epoch &msg) {
  std::scoped_lock l(mutex_);
  BOOST_ASSERT(conf_.node_id() == msg.dest());

  uint64_t epoch = msg.epoch();
#ifdef TX_TRACE
  trace_message_ += "h ep " + std::to_string(epoch) + " fm " + id_2_name(msg.source()) + ";";
#endif
  auto it = epoch_ctx_receive_.find(epoch);
  ptr<calvin_epoch_ops> ctx;
  if (it == epoch_ctx_receive_.end()) {
    ctx = std::make_shared<calvin_epoch_ops>();
    ctx->epoch_ = epoch;
    epoch_ctx_receive_.insert(std::make_pair(epoch, ctx));
  } else {
    ctx = it->second;
  }
  shard_id_t shard = TO_RG_ID(msg.source());
  auto i = ctx->shard_ids_.find(shard);
  BOOST_ASSERT(shard != 0);
  if (i == ctx->shard_ids_.end()) {
    ctx->shard_ids_.insert(shard);
    ctx->node_ids_.insert(msg.source());
    for (const tx_request &r : msg.request()) {
      ptr<tx_request> t(new tx_request(r));
      ctx->reqs_.push_back(t);
    }
    if (ctx->shard_ids_.size() == conf_.num_rg()) {
      reorder_request(ctx);
    }
  }
}

void calvin_sequencer::handle_epoch_ack(const calvin_epoch_ack &msg) {
  BOOST_ASSERT(msg.dest() == conf_.node_id());
  //BOOST_LOG_TRIVIAL(trace) << "epoch ack , from: " << id_2_name(msg.source()) << " to: " << id_2_name(conf_.node_id());
  std::scoped_lock l(mutex_);
  shard_id_t shard_id = msg.source();
  if (not local_epoch_) {
    return;
  }
  BOOST_ASSERT(local_epoch_->epoch_ == msg.epoch());
  local_epoch_->ack_.insert(shard_id);
#ifdef TX_TRACE
  trace_message_ += "ack fm " + id_2_name(msg.source()) + ";";
#endif
  if (local_epoch_->ack_.size() == node_id_.size()) {
    local_epoch_.reset();  // ready to send next epoch
    epoch_ctx_receive_.erase(msg.epoch());
    state_ = EPOCH_IDLE;
  }
}

void calvin_sequencer::reorder_request(const ptr<calvin_epoch_ops> &ctx) {
  BOOST_LOG_TRIVIAL(trace) << id_2_name(conf_.node_id()) << " reorder epoch " << ctx->epoch_ << " batch request";
#ifdef TX_TRACE
  trace_message_ += "rrd ep " + std::to_string(ctx->epoch_) + ";";
#endif
  for (auto kv : node_id_) {
    ctx->node_ids_.insert(kv.second);
  }
  auto sorter = [](const ptr<tx_request> &o1, const ptr<tx_request> &o2) {
    return o1->xid() < o2->xid();
  };
  std::sort(ctx->reqs_.begin(), ctx->reqs_.end(), sorter);
  //BOOST_LOG_TRIVIAL(trace) << id_2_name(conf_.node_id()) << " schedule " << e->epoch_  << " num ops " << e->ops_.size();

  schedule(ctx);
}

void calvin_sequencer::schedule(const ptr<calvin_epoch_ops> &ops) {
#ifdef TX_TRACE
  trace_message_ += "sch ;";
#endif
  scheduler_(ops);
}

void calvin_sequencer::debug_tx(std::ostream &os) {
  os << "calvin sequencer epoch " << epoch_ << std::endl;
  os << "calvin sequencer epoch state " << enum2str(state_) << std::endl;
  os << "calvin sequencer epoch , trace: " << std::endl;
  os << "    " << trace_message_ << std::endl;
  for (const auto &x : epoch_ctx_receive_) {
    os << " epoch :" << x.first;
    for (auto id : x.second->shard_ids_) {
      os << " shard id:" << id;
    }
    os << std::endl;
    for (auto id : x.second->node_ids_) {
      os << " node id :" << id_2_name(id);
    }
    os << std::endl;
  }
  os << "calvin lead: ";
  for (auto kv : node_id_) {
    os << id_2_name(kv.second) << " ";
  }
}
#endif // DB_TYPE_CALVIN