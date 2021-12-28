#include "common/db_type.h"
#include "concurrency/cc_block.h"
#include <charconv>
#include <memory>
#include <utility>
#include "common/make_int.h"
#include "common/result.hpp"
#include "common/debug_url.h"
#include "common/json_pretty.h"

cc_block::cc_block(
    const config &conf, net_service *service,
    fn_schedule_before fn_before,
    fn_schedule_after fn_after
) : conf_(conf),
    cno_(0),
    leader_(false),
    node_id_(conf.node_id()),
    node_name_(id_2_name(conf.node_id())),
    rlb_node_id_(conf.register_to_node_id()),
    dsb_node_id_(0),
    neighbour_shard_(0),
    registered_(false),
    mgr_(nullptr),
    service_(service),
    sequence_(0),
    wal_(new write_ahead_log(conf.node_id(), conf.register_to_node_id(), service)),
    fn_schedule_after_(fn_after) {
  auto fn = [this](xid_t xid) {
    abort_tx(xid, EC::EC_DEADLOCK);
  };
  deadlock_.reset(new deadlock(fn, service_, conf_.num_rg()));
  mgr_ = new access_mgr(deadlock_, std::move(fn_before), std::move(fn_after));
  BOOST_ASSERT(node_id_ != 0);
  BOOST_ASSERT(rlb_node_id_ != 0);
  std::set<shard_id_t> sids = conf_.shard_id_set();
  auto i = sids.find(TO_RG_ID(node_id_));
  if (i == sids.end()) {
    BOOST_ASSERT(false);
  }
  i++;
  if (i != sids.end()) {
    neighbour_shard_ = *i;
  } else {
    neighbour_shard_ = *sids.begin();
  }

#ifdef DB_TYPE_CALVIN
  if (is_deterministic()) {
    auto fn_find = [this](const tx_request &req) {
      return create_calvin_context(req);
    };

    calvin_scheduler_.reset(new calvin_scheduler(
        fn_find,
        conf_, mgr_, wal_.get(), service_));
    auto fn = [this](const ptr<calvin_epoch_ops> &ops) {
      calvin_scheduler_->schedule(ops);
    };
    calvin_sequencer_.reset(new calvin_sequencer(conf_, service_, fn));
  }
#endif // DB_TYPE_CALVIN
}

cc_block::~cc_block() {
  delete mgr_;
}

void cc_block::on_start() {
#ifdef DB_TYPE_NON_DETERMINISTIC
  deadlock_->tick();
#endif
  tick();
  //timeout_clean_up();
#ifdef DB_TYPE_CALVIN
  if (is_deterministic()) {
    calvin_sequencer_->tick();
  }
#endif // DB_TYPE_CALVIN
  BOOST_LOG_TRIVIAL(info) << "start up CCB " << node_name_ << " ...";
}

void cc_block::on_stop() {
  if (timer_send_register_) {
    timer_send_register_->cancel();
  }
  if (timer_send_status_) {
    timer_send_status_->cancel();
  }
  if (timer_clean_up_) {
    timer_clean_up_->cancel();
  }
  if (deadlock_) {
    deadlock_->stop();
  }
}

void cc_block::handle_debug(const std::string &path, std::ostream &os) {
  if (not boost::regex_match(path, url_json_prefix)) {
    os << BLOCK_CCB << " name:" << node_name_ << std::endl;
    os << "endpoint:" << conf_.this_node_config().node_peer() << std::endl;
    os << "register_to:" << id_2_name(conf_.register_to_node_id()) << std::endl;
    os << "path:" << path << std::endl;
    os << "registered " << registered_ << std::endl;
    os << "registered to lead " << (rg_lead_[TO_RG_ID(node_id_)] == node_id_) << std::endl;
  }
  if (path == "/lead") {
    for (auto rl: rg_lead_) {
      os << "S:" << rl.first << "L:" << id_2_name(rl.second) << std::endl;
    }
  } else if (boost::regex_match(path, url_tx)) {
    debug_tx(os, 0);
  } else if (boost::regex_match(path, url_tx_xid)) {
    boost::smatch what;
    if (boost::regex_search(path, what, url_tx_xid)) {
      std::string xs = what[1];
      uint64_t xid;
      auto r = std::from_chars(xs.c_str(), xs.c_str() + xs.size(), xid);
      if (r.ec == std::errc()) {
        debug_tx(os, xid);
      }
    }
  } else if (boost::regex_match(path, url_lock)) {
    debug_lock(os);
  } else if (boost::regex_match(path, url_dep)) {
    debug_dependency(os);
  } else if (boost::regex_match(path, url_deadlock)) {
    debug_deadlock(os);
  } else if (boost::regex_match(path, url_json_deadlock)) {
    debug_deadlock(os);
  }
}

void cc_block::tick() {
  send_register();

  timer_send_register_.reset(new boost::asio::steady_timer(
      service_->get_service(SERVICE_HANDLE),
      boost::asio::chrono::milliseconds(500)));
  auto s = shared_from_this();
  auto fn_timeout = [s](const boost::system::error_code &error) {
    if (not error.failed()) {
      s->tick();
    } else {
      BOOST_LOG_TRIVIAL(error) << " async wait error " << error.message();
    }
  };
  timer_send_register_->async_wait(fn_timeout);
}

void cc_block::send_register() {
  std::scoped_lock l(register_mutex_);
  //BOOST_LOG_TRIVIAL(info) << node_name_ << " send register_ccb";
  ccb_register_ccb_request request;
  request.set_source(node_id_);
  request.set_dest(rlb_node_id_);
  request.set_cno(cno_);
  auto r1 = service_->async_send(rlb_node_id_, message_type::C2R_REGISTER_REQ,
                                 request);
  if (!r1) {
    BOOST_LOG_TRIVIAL(error) << "send error register_ccb error";
  }

  panel_info_request req;
  req.set_source(node_id_);
  req.set_dest(conf_.panel_config().node_id());
  req.set_block_type(pb_block_type::PB_BLOCK_CCB);
  auto r2 = service_->async_send(req.dest(), message_type::PANEL_INFO_REQ, req);
  if (!r2) {
    BOOST_LOG_TRIVIAL(error) << "send PANEL_INFO_REQ error";
  }
}

void cc_block::handle_register_ccb_response(
    const rlb_register_ccb_response &response) {
  std::scoped_lock l(register_mutex_);

  panel_report msg;
  msg.set_source(conf_.node_id());
  msg.set_dest(conf_.panel_config().node_id());
  msg.set_lead(response.is_lead());
  msg.set_registered(response.source());
  msg.set_report_type(CCB_REGISTERED_RLB);
  auto r = service_->async_send(msg.dest(), message_type::PANEL_REPORT, msg);
  if (not r) {
    BOOST_LOG_TRIVIAL(error) << "send panel report error: " << r.error().message();
  }

  if (cno_ != response.cno()) {
    std::string lead = response.is_lead() ? "leader" : "follower";
    BOOST_LOG_TRIVIAL(info) << "CCB "
                            << node_name_
                            << " registered to RLB "
                            << id_2_name(response.source())
                            << " cno = " << cno_ << " " << lead;

    send_status_acked_.clear();
    if (leader_ != response.is_lead()) {
      send_broadcast_status(response.is_lead());
      leader_ = response.is_lead();
    } else {

    }
#ifdef DB_TYPE_CALVIN
    if (is_deterministic()) {
      calvin_sequencer_->update_local_lead_state(response.is_lead());
    }
#endif // DB_TYPE_CALVIN

    cno_ = response.cno();
    dsb_node_id_ = response.dsb_node();
    registered_ = true;
  } else {

  }
}

void cc_block::handle_client_tx_request(const ptr<connection> &conn,
                                        const tx_request &request) {
  EC ec = EC::EC_OK;

  BOOST_ASSERT(request.client_request());
  BOOST_ASSERT(conn != nullptr);
  BOOST_ASSERT(mgr_);
  BOOST_ASSERT(service_);
  if (request.operations_size() > 0) {
    if (request.oneshot()) {
#ifdef DB_TYPE_CALVIN
      if (is_deterministic()) {
        handle_calvin_tx_request(conn, request);
      }
#endif //DB_TYPE_CALVIN
#ifdef DB_TYPE_NON_DETERMINISTIC
      if (is_non_deterministic()) {
        handle_non_deterministic_tx_request(conn, request);
      }
#endif //DB_TYPE_NON_DETERMINISTIC
    } else {
      BOOST_ASSERT_MSG(false, "not implement");
    }
  } else {
    tx_response response;
    response.set_error_code(uint32_t(ec));
    auto r2 = conn->send_message(CLIENT_TX_RESP, response);
    if (!r2) {
    }
  }
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const rlb_register_ccb_response &m) {
  handle_register_ccb_response(m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &c, message_type t, const tx_request &m) {
  switch (t) {
  case CLIENT_TX_REQ: {
    handle_client_tx_request(c, m);
    break;
  }
#ifdef DB_TYPE_SHARE_NOTHING
  case TX_TM_REQUEST: {
    handle_tx_tm_request(m);
    break;
  }
  default: {
    break;
  }
#endif
  }
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const rlb_commit_entries &m) {
  handle_log_entries_commit(m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ccb_broadcast_status &m) {
  handle_broadcast_status_req(m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &,
                                          message_type,
                                          const ccb_broadcast_status_response &m) {
  handle_broadcast_status_resp(m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &c, message_type, const lead_status_request &m) {
  handle_lead_status_request(c, m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const panel_info_response &m) {
  handle_panel_info_resp(m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const rlb_report_status_to_ccb &m) {
  handle_report_status(m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const dsb_read_response &m) {
#ifdef DB_TYPE_NON_DETERMINISTIC
  if (is_non_deterministic()) {
    handle_read_data_response(m);
  }
#endif
#ifdef DB_TYPE_CALVIN
  if (is_deterministic()) {
    handle_calvin_read_response(m);
  }
#endif
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const tx_rm_prepare &m) {
  handle_tx_rm_prepare(m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const tx_rm_ack &m) {
  handle_tx_rm_ack(m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const tx_tm_commit &m) {
  handle_tx_tm_commit(m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const tx_tm_abort &m) {
  handle_tx_tm_abort(m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const dependency_set &m) {
  handle_dependency_set(m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type t, const tx_enable_violate &m) {
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  if (t == TM_ENABLE_VIOLATE) {
    handle_tx_tm_enable_violate(m);
  } else if (t == RM_ENABLE_VIOLATE) {
    handle_tx_rm_enable_violate(m);
  }
  return outcome::success();
#endif //DB_TYPE_GEO_REP_OPTIMIZE
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const calvin_part_commit &m) {
  handle_calvin_part_commit(m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const calvin_epoch &m) {
#ifdef DB_TYPE_CALVIN
  calvin_sequencer_->handle_epoch(m);
#endif
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const calvin_epoch_ack &m) {
#ifdef DB_TYPE_CALVIN
  calvin_sequencer_->handle_epoch_ack(m);
#endif
  return outcome::success();
}

uint64_t cc_block::gen_xid() {
  uint32_t seq = ++sequence_;
  uint64_t xid = make_uint64(seq, conf_.node_id());
  return xid;
}

void cc_block::handle_log_entries_commit(const rlb_commit_entries &msg) {
#ifdef DB_TYPE_CALVIN
  if (is_deterministic()) {
    handle_calvin_log_commit(msg);
  }
#endif
#ifdef DB_TYPE_NON_DETERMINISTIC
  if (is_non_deterministic()) {
    handle_append_log_response(msg);
  }
#endif
}

void cc_block::handle_report_status(const rlb_report_status_to_ccb &message) {
  if (message.cno() != cno_) {
    // register ...
    registered_ = false;
  }

  ccb_report_status_response res;
  res.set_source(node_id_);
  res.set_dest(message.source());
  res.set_cno(message.cno());
  auto r = service_->async_send(res.dest(), C2R_REPORT_STATUS_RESP, res);
  if (not r) {
    BOOST_ASSERT(false);
  }

  // clear ACK state for resend when timeout...
  send_status_acked_.clear();
  BOOST_LOG_TRIVIAL(info) << "CCB " << node_name_ << " handle report status, send broadcast status ";

  send_broadcast_status(message.lead());
}

void cc_block::send_broadcast_status(bool lead) {
  bool all_ack = false;
  if (!send_status_acked_.empty()) {
    all_ack = true;
    for (auto kv: send_status_acked_) {
      if (!kv.second) {
        all_ack = false;
      }
    }
  }
  if (all_ack) {
    return;
  }
  for (const node_config &c: conf_.node_config_list()) {
    if (is_ccb_block(c.node_id())) {
      if (!send_status_acked_.contains(c.node_id()) ||
          !send_status_acked_[c.node_id()]) {
        ccb_broadcast_status msg;
        msg.set_dest(c.node_id());
        msg.set_source(node_id_);
        msg.set_lead(lead);

        send_status_acked_[c.node_id()] = false;
        auto rs =
            service_->async_send(msg.dest(), CCB_BORADCAST_STATUS_REQ, msg);
        if (!rs) {
          BOOST_LOG_TRIVIAL(info) << "send broadcast status error";
        }
      }
    }
  }

  timer_send_status_.reset(new boost::asio::steady_timer(
      service_->get_service(SERVICE_HANDLE),
      boost::asio::chrono::milliseconds(2000)));
  auto s = shared_from_this();
  auto fn_timeout = [s, lead](const boost::system::error_code &error) {
    if (error.failed()) {
      BOOST_LOG_TRIVIAL(error) << s->node_name_ << " wait timeout error..." << error.message();
      return;
    }
    std::scoped_lock l(s->mutex_);
    BOOST_LOG_TRIVIAL(info) << s->node_name_ << " timeout , send broadcast status ";
    s->send_broadcast_status(lead);
  };
  timer_send_status_->async_wait(fn_timeout);
}

void cc_block::handle_broadcast_status_req(const ccb_broadcast_status &msg) {
  shard_id_t rg = TO_RG_ID(msg.source());
  node_id_t source_node = msg.source();
  auto iter = rg_lead_.find(rg);
  if (iter == rg_lead_.end()) {
    if (msg.lead()) {
      rg_lead_.insert(std::make_pair(rg, source_node));
      if (rg == neighbour_shard_) {
        if (deadlock_) {
          deadlock_->set_next_node(source_node);
        }
      }
#ifdef DB_TYPE_CALVIN
      if (is_deterministic()) {
        calvin_sequencer_->update_shard_lead(rg, source_node);
      }
#endif // DB_TYPE_CALVIN
    }
  } else {
    if (msg.lead()) {
      iter->second = source_node;
    } else if (iter->second == source_node) {
      rg_lead_.erase(iter);
    }
  }

  ccb_broadcast_status_response res;
  res.set_source(node_id_);
  res.set_dest(source_node);
  auto rs = service_->async_send(res.dest(), CCB_BORADCAST_STATUS_RESP, res);
  if (!rs) {
  }
}

void cc_block::handle_broadcast_status_resp(
    const ccb_broadcast_status_response &req) {
  send_status_acked_[req.source()] = true;
}

void cc_block::handle_lead_status_request(const ptr<connection> &conn, const lead_status_request &) {
  //BOOST_ASSERT(node_id_ == msg.dest());

  lead_status_response response;
  for (auto p: rg_lead_) {
    response.mutable_lead()->Add(p.second);
    if (TO_RG_ID(p.second) == TO_RG_ID(node_id_)) {
      response.set_rg_lead(p.second);
    }
  }
  auto r = conn->async_send(LEAD_STATUS_RESPONSE, response);
  if (!r) {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " send lead status response error";
  }
}

void cc_block::handle_panel_info_resp(const panel_info_response &msg) {
  for (auto id: msg.ccb_leader()) {
    shard_id_t sid = TO_RG_ID(id);
    rg_lead_[sid] = id;
    if (sid == neighbour_shard_) {
      if (deadlock_) {
        deadlock_->set_next_node(id);
      }
    }
#ifdef DB_TYPE_CALVIN
    if (is_deterministic()) {
      calvin_sequencer_->update_shard_lead(sid, id);
    }
#endif // DB_TYPE_CALVIN
  }
}

#ifdef DB_TYPE_NON_DETERMINISTIC

void cc_block::create_tx_context(const ptr<connection> &conn, const tx_request &req) {
  uint64_t xid = req.xid();
  BOOST_LOG_TRIVIAL(debug) << node_name_ << " transaction " << xid
                           << " request";
  ptr<tx_context> ctx;
  auto if_absent = [& ctx, req, conn, xid, this]() {
    BOOST_ASSERT(dsb_node_id_ != 0);
    auto fn_remove = [this](uint64_t xid, rm_state state) {
      if (state == rm_state::RM_ABORTED ||
          state == rm_state::RM_COMMITTED
          ) {
        if (xid && state) {

        }

        this->tx_context_.remove(xid, nullptr);
      }
    };
    ctx = std::make_shared<tx_context>(
        xid, node_id_, dsb_node_id_, cno_,
        req.distributed(), mgr_, service_, conn,
        wal_.get(),
        fn_remove,
        deadlock_.get());

    return std::make_pair(xid, ctx);
  };
  bool ok = tx_context_.insert(xid, if_absent);
  if (ok) {
    ctx->process_tx_request(req);
  } else {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " existing transaction " << xid;
  }
}

void cc_block::handle_append_log_response(
    const rlb_commit_entries &response) {
  EC ec = EC(response.error_code());
  if (ec != EC::EC_OK) {
    std::set<xid_t> set;

    for (const tx_log &op: response.logs()) {
      auto xid = xid_t(op.xid());
      set.insert(xid);
    }

    for (xid_t xid: set) {
      abort_tx(xid, EC::EC_APPEND_LOG_ERROR);
    }
    return;
  }

  for (const tx_log &xl: response.logs()) {
    xid_t xid = xl.xid();
    tx_cmd_type t = xl.log_type();
    if (fn_schedule_after_ != nullptr) {
      fn_schedule_after_(tx_op(t, xid, xl.operations().size() + 1));
    }
    switch (t) {
#ifdef DB_TYPE_SHARE_NOTHING
    case TX_CMD_TM_ABORT:
    case TX_CMD_TM_COMMIT:
    case TX_CMD_TM_BEGIN:
    case TX_CMD_TM_END: {
      std::pair<ptr<tx_coordinator>, bool> p = tx_coordinator_.find(xid);
      if (p.second) {
        p.first->on_log_entry_commit(t);
      } else {
        BOOST_LOG_TRIVIAL(error) << "cannot find long1 :" << xid;
      }
      break;
    }
#endif
    case TX_CMD_RM_PREPARE_ABORT:
    case TX_CMD_RM_PREPARE_COMMIT:
    case TX_CMD_RM_ABORT:
    case TX_CMD_RM_COMMIT:
    case TX_CMD_RM_BEGIN: {
      std::pair<ptr<tx_context>, bool> p = tx_context_.find(xid);
      if (p.second) {
        p.first->on_log_entry_commit(t);
      } else {
        BOOST_LOG_TRIVIAL(error) << "cannot find long1 :" << xid;
      }
      break;
    }
    default: {
      break;
    }
    }
  }

}

void cc_block::handle_read_data_response(const dsb_read_response &response) {
  EC ec = EC(response.error_code());
  uint64_t xid = response.xid();
  std::pair<ptr<tx_context>, bool> p = tx_context_.find(xid);
  if (p.second) {
    tuple_id_t key(response.tuple_row().tuple_id());
    p.first->read_data_from_dsb_response(ec, response.table_id(), key,
                                         response.oid(),
                                         response.tuple_row().tuple());
    tuple_pb tuple;
    std::pair<bool, size_t> tuple_ok;
    // TODO marshall tuple
    if (!tuple_ok.first) {
      return;
    }
  } else {
    BOOST_LOG_TRIVIAL(error) << "cannot find transaction " << xid;
  }
}

void cc_block::handle_non_deterministic_tx_request(const ptr<connection> &conn,
                                                   const tx_request &request) {
  uint64_t xid = gen_xid();
  BOOST_LOG_TRIVIAL(trace) << node_name_ << " handle dist=" << request.distributed() << " tx " << xid;
  const_cast<tx_request &>(request).set_xid(xid);
  if (request.distributed()) {
#ifdef DB_TYPE_SHARE_NOTHING
    if (is_shared_nothing()) {
      create_tx_coordinator(conn, request);
    }
#endif
  } else {
    create_tx_context(conn, request);
  }
}

void cc_block::timeout_clean_up() {
  timer_clean_up_.reset(new boost::asio::steady_timer(
      service_->get_service(SERVICE_HANDLE),
      boost::asio::chrono::milliseconds(2000)));
  auto s = shared_from_this();
  auto fn_timeout = [s](const boost::system::error_code &error) {
    if (not error.failed()) {
      s->timeout_clean_up_tx();
      s->timeout_clean_up();
    } else {
      BOOST_LOG_TRIVIAL(error) << " async wait clean up timeout error " << error.message();
    }
  };
  timer_clean_up_->async_wait(fn_timeout);
}

void cc_block::timeout_clean_up_tx() {
  auto fn_kv_ctx = [](uint64_t, const ptr<tx_context> &rm) {
    rm->timeout_clean_up();
  };
  tx_context_.traverse(fn_kv_ctx);
#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {
    auto fn_kv_coord = [](uint64_t, const ptr<tx_coordinator> &tm) {
      tm->timeout_clean_up();
    };
    tx_coordinator_.traverse(fn_kv_coord);
  }
#endif
}

#ifdef DB_TYPE_SHARE_NOTHING

void cc_block::handle_tx_tm_request(const tx_request &request) {
  BOOST_ASSERT(request.operations_size() > 0);
  BOOST_ASSERT(request.distributed());
  BOOST_ASSERT(mgr_);
  BOOST_ASSERT(service_);
  BOOST_ASSERT(!request.client_request());
  if (request.oneshot()) {
    BOOST_ASSERT(request.xid() != 0);
    create_tx_context(nullptr, request);
  } else {
    BOOST_ASSERT_MSG(false, "not implement");
    // non one_shot tx
  }
}

void cc_block::create_tx_coordinator(const ptr<connection> &conn, const tx_request &req) {
  uint64_t xid = req.xid();
  BOOST_LOG_TRIVIAL(trace) << "transaction " << xid << " request";
  ptr<tx_coordinator> c;
  auto if_absent = [&c, req, conn, xid, this]() {
    auto fn_remove = [this](uint64_t xid, tm_state state) {
      if (state == tm_state::TM_DONE) {
        tx_coordinator_.remove(xid, nullptr);
      }
    };

    c = std::make_shared<tx_coordinator>(
        xid, node_id_, rg_lead_, service_, conn, wal_.get(), fn_remove);
    result<void> r = c->handle_tx_request(req);
    if (not r) {
      BOOST_LOG_TRIVIAL(info) << "";
    }
    if (conn != nullptr) {
    }
    return std::make_pair(xid, c);
  };
  bool ok = tx_coordinator_.insert(xid, if_absent);
  if (ok) {

  } else {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " existing transaction " << xid;
  }
}

void cc_block::handle_tx_rm_prepare(const tx_rm_prepare &msg) {
  std::pair<ptr<tx_coordinator>, bool> p = tx_coordinator_.find(msg.xid());
  if (p.second) {
    p.first->handle_tx_rm_prepare(msg);
  } else {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " cannot find long1 " << msg.xid();
  }
}

void cc_block::handle_tx_rm_ack(const tx_rm_ack &msg) {
  std::pair<ptr<tx_coordinator>, bool> p = tx_coordinator_.find(msg.xid());
  if (p.second) {
    p.first->handle_tx_rm_ack(msg);
  } else {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " cannot find long1 " << msg.xid();
  }
}

void cc_block::handle_tx_tm_commit(const tx_tm_commit &msg) {
  std::pair<ptr<tx_context>, bool> p = tx_context_.find(msg.xid());
  if (p.second) {
    p.first->handle_tx_tm_commit(msg);
  } else {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " cannot find long1 " << msg.xid();
  }
}

void cc_block::handle_tx_tm_abort(const tx_tm_abort &msg) {
  std::pair<ptr<tx_context>, bool> p = tx_context_.find(msg.xid());
  if (p.second) {
    p.first->handle_tx_tm_abort(msg);
  } else {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " cannot find long1 " << msg.xid();
  }
}

void cc_block::handle_dependency_set(const dependency_set &msg) {
  if (deadlock_) {
    deadlock_->recv_dependency(msg);
  }
}
#endif // DB_TYPE_SHARE_NOTHING

#ifdef DB_TYPE_GEO_REP_OPTIMIZE
void cc_block::handle_tx_tm_enable_violate(const tx_enable_violate &msg) {
  BOOST_ASSERT(msg.dest() == node_id_);
  std::pair<ptr<tx_context>, bool> r = tx_context_.find(msg.xid());
  if (r.second) {
    ptr<tx_context> ctx = r.first;
    ctx->handle_tx_enable_violate();
  }
}
void cc_block::handle_tx_rm_enable_violate(const tx_enable_violate &msg) {
  BOOST_ASSERT(msg.dest() == node_id_);
  std::pair<ptr<tx_coordinator>, bool> r = tx_coordinator_.find(msg.xid());
  if (r.second) {
    r.first->handle_tx_enable_violate(msg);
  }
}

#endif // DB_TYPE_GEO_REP_OPTIMIZE

#endif // #ifdef DB_TYPE_NON_DETERMINISTIC

#ifdef DB_TYPE_NON_DETERMINISTIC
void cc_block::abort_tx(xid_t xid, EC ec) {
  std::pair<ptr<tx_context>, bool> r = tx_context_.find(xid);
  if (r.second) {
    BOOST_LOG_TRIVIAL(info) << "victim tx" << xid;
    r.first->abort(ec);
  }
#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {
    std::pair<ptr<tx_coordinator>, bool> rc = tx_coordinator_.find(xid);
    if (rc.second) {
      BOOST_LOG_TRIVIAL(info) << "victim tx " << xid;
      rc.first->abort(ec);
    }
  }
#endif
}
#endif

void cc_block::debug_tx(std::ostream &os, xid_t xid) {
  os << "debug tx context: " << std::endl;
#ifdef DB_TYPE_NON_DETERMINISTIC
  auto fn_kv_ctx = [&os, xid](uint64_t k, const ptr<tx_context> &rm) {
    if (xid == 0 || xid == k) {
      rm->debug_tx(os);
    }
  };
  tx_context_.traverse(fn_kv_ctx);

#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {
    auto fn_kv_coord = [&os, xid](uint64_t k, const ptr<tx_coordinator> &tm) {
      if (xid == 0 || xid == k) {
        os << "TM : " << xid << " state: " << enum2str(tm->state()) << " trace: " << tm->trace_message()
           << std::endl;
      }
    };
    tx_coordinator_.traverse(fn_kv_coord);
  }
#endif
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_CALVIN
  if (is_deterministic()) {
    calvin_scheduler_->debug_tx(os);
    calvin_sequencer_->debug_tx(os);
    os << "calvin context:" << std::endl;
    auto f1 = [&os, xid](xid_t k, const ptr<calvin_context> &t) {
      if (xid == 0 || xid == k) {
        t->debug_tx(os);
      }
    };
    calvin_context_.traverse(f1);
    os << "calvin collector:" << std::endl;
    auto f2 = [&os, xid](xid_t k, const ptr<calvin_collector> &t) {
      if (xid == 0 || xid == k) {
        t->debug_tx(os);
      }
    };
    calvin_collector_.traverse(f2);
  }
#endif // DB_TYPE_CALVIN
}

void cc_block::debug_lock(std::ostream &os) {
  os << "debug tx lock: " << std::endl;
  mgr_->debug_lock(os);
}

void cc_block::debug_dependency(std::ostream &os) {
  tx_wait_set s;
  mgr_->debug_dependency(s);

  dependency_set ds;
  s.to_dependency_set(ds);
  std::stringstream ssm;
  ssm << pb_to_json(ds);
  json_pretty(ssm, os);
}

void cc_block::debug_deadlock(std::ostream &os) {
  if (deadlock_) {
    deadlock_->debug_deadlock(os);
  }
}
#ifdef DB_TYPE_CALVIN

ptr<calvin_context> cc_block::create_calvin_context(const tx_request &req) {
  ptr<calvin_context> calvin_ctx;
  auto fn_find = [&calvin_ctx](ptr<calvin_context> c) {
    calvin_ctx = std::move(c);
  };
  auto fn_insert = [this, &calvin_ctx, req]() {
    auto fn_remove = [this](xid_t xid) {
      return remove_calvin_context(xid);
    };
    calvin_ctx = std::make_shared<calvin_context>(
        req.xid(),
        conf_.node_id(),
        dsb_node_id_,
        cno_,
        std::make_shared<tx_request>(req),
        service_,
        mgr_,
        fn_remove);
    return std::make_pair(req.xid(), calvin_ctx);
  };
  calvin_context_.find_or_insert(req.xid(), fn_find, fn_insert);
  BOOST_ASSERT(calvin_ctx);
  return calvin_ctx;
}

void cc_block::remove_calvin_context(xid_t xid) {
  calvin_context_.remove(xid, nullptr);
}
void cc_block::handle_calvin_tx_request(ptr<connection> conn,
                                        const tx_request &request) {
  xid_t xid = gen_xid();
  ptr<calvin_collector> collector(new calvin_collector(xid, std::move(conn), request));
  collector->mutable_request().set_source(node_id_);
  bool ok = calvin_collector_.insert(xid, collector);
  if (!ok) {
    BOOST_LOG_TRIVIAL(error) << "existing long1";
  }
  // collector->request() assigned xid
  calvin_sequencer_->handle_tx_request(collector->request());
}

void cc_block::handle_calvin_log_commit(const rlb_commit_entries &msg) {
  for (const auto &op: msg.logs()) {
    std::pair<ptr<calvin_context>, bool> r = calvin_context_.find(op.xid());
    if (r.second) {
      bool committed = r.first->on_operation_committed(op);
      if (committed) {
        //calvin_context_.remove(op.xid(), nullptr);
      }
    } else {
      BOOST_LOG_TRIVIAL(error) << "cannot find tx " << op.xid();
    }
  }

}

void cc_block::handle_calvin_part_commit(const calvin_part_commit &msg) {
  xid_t xid = msg.xid();
  std::pair<ptr<calvin_collector>, bool> r = calvin_collector_.find(xid);
  if (r.second) {
    bool all_committed = r.first->part_commit(msg);
    if (all_committed) {
      calvin_collector_.remove(xid, nullptr);
    }
  } else {
    BOOST_LOG_TRIVIAL(error) << "can not find long1 " << xid;
  }
}

void cc_block::handle_calvin_read_response(const dsb_read_response &msg) {
  uint64_t xid = msg.xid();
  std::pair<ptr<calvin_context>, bool> p = calvin_context_.find(xid);
  if (p.second) {
    p.first->read_response(msg);
  } else {
    BOOST_LOG_TRIVIAL(error) << "cannot find transaction " << xid;
  }
}
#endif // DB_TYPE_CALVIN