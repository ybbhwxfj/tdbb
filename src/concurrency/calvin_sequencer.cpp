
#include "concurrency/calvin_sequencer.h"
#ifdef DB_TYPE_CALVIN
#include "common/panic.h"
#include "common/define.h"
#include "common/variable.h"
#include <algorithm>
#include <memory>
#include <utility>

template<>
enum_strings<epoch_state>::e2s_t enum_strings<epoch_state>::enum2str = {
    {EPOCH_IDLE, "EPOCH_IDLE"},
    {EPOCH_PENDING, "EPOCH_PENDING"},
};

calvin_sequencer::calvin_sequencer(const config &conf,
                                   boost::asio::io_context::strand strand,
                                   net_service *service, fn_scheduler fn)
    : conf_(conf), epoch_(1), service_(service), scheduler_(fn),
      state_(EPOCH_IDLE), is_lead_(false), is_priority_leader_(false),
      start_(false), strand_(strand) {
  node_id_ = conf_.priority_lead_nodes();
  auto iter = node_id_.find(TO_RG_ID(conf_.node_id()));
  if (iter != node_id_.end()) {
    if (iter->second == conf_.node_id()) {
      is_priority_leader_ = true;
    }
  }

  BOOST_ASSERT(node_id_.size() == conf_.num_rg());
}

void calvin_sequencer::handle_tx_request(const tx_request &tx) {

#ifdef TX_TRACE
  trace_message_ << "h tx_rm;";
#endif

  for (const tx_operation &o : tx.operations()) {
    auto i = tx_request_next_epoch_.find(o.sd_id());
    if (i == tx_request_next_epoch_.end()) {
      ptr<tx_request> t(new tx_request());
      t->set_terminal_id(tx.terminal_id());
      t->set_xid(tx.xid());
      t->set_source(conf_.node_id());
      *t->add_operations() = o;
      tx_request_next_epoch_[o.sd_id()][tx.xid()] = t;
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

void calvin_sequencer::start() {
  auto self = shared_from_this();
  boost::asio::post(strand_, [self] { self->start_ = true; });
}

void calvin_sequencer::tick() {
  uint32_t ms = conf_.get_tpcc_config().calvin_epoch_ms();
  if (not is_lead_ || not is_priority_leader_) {
    ms = ms * 10;
  }
  auto self = shared_from_this();
  ptr<boost::asio::steady_timer> timer_tick_;
  timer_tick_.reset(new boost::asio::steady_timer(
      service_->get_service(SERVICE_ASYNC_CONTEXT),
      boost::asio::chrono::milliseconds(ms)));
  auto fn_timeout = boost::asio::bind_executor(
      strand_, [timer_tick_, self](const boost::system::error_code &error) {
        if (not error.failed()) {
          self->tick();
        } else {
          LOG(error) << " async wait error " << error.message();
        }
      });
  timer_tick_->async_wait(fn_timeout);

  epoch_broadcast_request();
}

void calvin_sequencer::update_local_lead_state(bool lead) { is_lead_ = lead; }

void calvin_sequencer::epoch_broadcast_request() {
  if (not start_) {
    return;
  }
  if (node_id_.size() != conf_.num_rg()) {
    LOG(trace) << "epoch_broadcast_request leader node != group number";
    return;
  }
  if (not is_lead_ || not is_priority_leader_) {
    LOG(trace) << "epoch_broadcast_request lead " << is_lead_ << " "
               << is_priority_leader_;
    return;
  }

  if (state_ == EPOCH_IDLE) {
#ifdef TX_TRACE
    trace_message_.clear();
    trace_message_ << "ne " << epoch_ << ";";
#endif
    ptr<local_epoch_ctx> epoch(new local_epoch_ctx());
    epoch->epoch_ = epoch_;
    local_epoch_.insert(std::make_pair(epoch->epoch_, epoch));
    state_ = EPOCH_PENDING;

    for (auto kv : node_id_) {
      shard_id_t shard = TO_RG_ID(kv.first);
      if (epoch->ack_.contains(shard)) {
        LOG(error) << id_2_name(conf_.node_id()) << " contains ACK of"
                   << id_2_name(kv.second);
        BOOST_ASSERT(false);
        trace_message_ << "contains" << id_2_name(kv.second) << ";";
        continue;
      }
      node_id_t nid = kv.second;
      auto msg = cs_new<calvin_epoch>();
      // LOG(trace) << "calvin send request to " << id_2_name(nid);
      msg->set_dest(nid);
      msg->set_source(conf_.node_id());
      msg->set_epoch(epoch->epoch_);
      shard_id_t sid = TO_RG_ID(nid);
      auto i1 = tx_request_next_epoch_.find(sid);
      if (i1 != tx_request_next_epoch_.end()) {
        for (std::pair<xid_t, ptr<tx_request>> r : i1->second) {
          *msg->add_request() = *r.second;
        }
      } else {
        // no transactions access this shard
      }
      // LOG(trace) << id_2_name(conf_.node_id())
      //                         << " send calvin epoch " << msg->epoch() << "
      //                         to "
      //                         << id_2_name(nid);
      // TODO ...
      // this message must not be lost
      epoch->ssm_ << "snd " << id_2_name(msg->dest()) << ";";
      auto r = service_->async_send(nid, CALVIN_EPOCH, msg, true);
      if (!r) {
        LOG(error) << "async send calvin epoch error...";
      }
    }
    tx_request_next_epoch_.clear();
    epoch_++;
  }
}

void calvin_sequencer::handle_epoch(const calvin_epoch &msg) {
  // LOG(trace) << id_2_name(conf_.node_id()) << " handle calvin epoch " <<
  // msg.epoch() << " from "
  //                        << id_2_name(msg.source());
  BOOST_ASSERT(conf_.node_id() == msg.dest());

  uint64_t epoch = msg.epoch();
#ifdef TX_TRACE
  trace_message_ << "h ep " << std::to_string(epoch) << " fm "
                 << id_2_name(msg.source()) << ";";
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
  // LOG(trace) << "epoch ack , from: " << id_2_name(msg.source()) << " to: " <<
  // id_2_name(conf_.node_id());

  shard_id_t shard_id = TO_RG_ID(msg.source());
  uint64_t epoch = msg.epoch();
  auto iter = local_epoch_.find(epoch);
  if (local_epoch_.end() == iter) {
    trace_message_ << "ack ep null;";
    LOG(error) << "local epoch = null";
    return;
  }
  ptr<local_epoch_ctx> epoch_ctx = iter->second;
  BOOST_ASSERT(epoch_ctx->epoch_ == msg.epoch());
  epoch_ctx->ack_.insert(shard_id);
#ifdef TX_TRACE
  trace_message_ << "ack fm " << id_2_name(msg.source()) << ";";
#endif
  if (epoch_ctx->ack_.size() == node_id_.size()) {
    local_epoch_.erase(epoch); // ready to send next epoch
    epoch_ctx_receive_.erase(epoch);
    if (epoch + 1 != epoch_) {
      LOG(error) << "calvin error epoch " << epoch << ":" << epoch_;
    }
    state_ = EPOCH_IDLE;
  }
}

void calvin_sequencer::reorder_request(const ptr<calvin_epoch_ops> &ctx) {
  LOG(trace) << id_2_name(conf_.node_id()) << " reorder epoch " << ctx->epoch_
             << " batch request";
#ifdef TX_TRACE
  trace_message_ << "rrd ep " << ctx->epoch_ << ";";
#endif
  for (auto kv : node_id_) {
    ctx->node_ids_.insert(kv.second);
  }
  auto sorter = [](const ptr<tx_request> &o1, const ptr<tx_request> &o2) {
    return o1->xid() < o2->xid();
  };
  std::sort(ctx->reqs_.begin(), ctx->reqs_.end(), sorter);
  // LOG(trace) << id_2_name(conf_.node_id()) << " schedule " << e->epoch_  << "
  // num ops " << e->ops_.size();

  schedule(ctx);
}

void calvin_sequencer::schedule(const ptr<calvin_epoch_ops> &ops) {
#ifdef TX_TRACE
  trace_message_ << "sch ;";
#endif
  scheduler_(ops);
}

void calvin_sequencer::debug_tx(std::ostream &os) {
  os << "calvin sequencer epoch " << epoch_ << std::endl;
  os << "calvin sequencer epoch state " << enum2str(state_) << std::endl;
  os << "calvin sequencer epoch , trace: " << std::endl;
  os << "    " << trace_message_.str() << std::endl;
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
  for (auto n : local_epoch_) {
    os << "epoch: " << n.first;
    os << " trace : " << n.second->ssm_.str();
    for (auto id : n.second->ack_) {
      os << " acks shard:" << id << std::endl;
    }
  }
  os << "calvin lead: ";
  for (auto kv : node_id_) {
    os << id_2_name(kv.second) << " ";
  }
}
#endif // DB_TYPE_CALVIN