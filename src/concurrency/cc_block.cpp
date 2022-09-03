#include "common/db_type.h"
#include "concurrency/cc_block.h"
#include <charconv>
#include <memory>
#include <utility>
#include "common/timer.h"
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
#ifdef DB_TYPE_CALVIN
    strand_calvin_(service->get_service(SERVICE_ASYNC_CONTEXT)),
#endif
    fn_schedule_after_(fn_after),
    time_("CCB h msg"),
    strand_send_register_(service->get_service(SERVICE_ASYNC_CONTEXT)),
    strand_send_status_(service->get_service(SERVICE_ASYNC_CONTEXT)),
    timer_strand_(service->get_service(SERVICE_ASYNC_CONTEXT)) {
  auto fn = [this](xid_t xid) {
    abort_tx(xid, EC::EC_DEADLOCK);
  };

  deadlock_.reset(new deadlock(fn, service_, conf_.num_rg()));
  mgr_ = new access_mgr(service_, deadlock_.get(), std::move(fn_before), std::move(fn_after));
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
  uint64_t num_max_terminal = conf_.get_tpcc_config().num_terminal() + 10;
  tx_context_.resize(num_max_terminal);
#ifdef DB_TYPE_SHARE_NOTHING
  tx_coordinator_.resize(num_max_terminal);
#endif
#ifdef DB_TYPE_CALVIN
  calvin_context_.resize(num_max_terminal);
  calvin_collector_.resize(num_max_terminal);
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

#ifdef DB_TYPE_CALVIN
  if (is_deterministic()) {
    auto ccb = shared_from_this();

    auto fn_find = [ccb](const tx_request &req) {
      xid_t xid = req.xid();
      uint32_t terminal_id = ccb->xid_to_terminal_id(xid);
      auto pair = ccb->calvin_context_[terminal_id].find(xid);
      if (pair.second) {
        return pair.first;
      } else {
        auto ctx = ccb->create_calvin_context(req);
        ccb->calvin_context_[terminal_id].insert(xid, ctx);
        return ctx;
      }
    };

    calvin_scheduler_.reset(new calvin_scheduler(
        fn_find,
        conf_, mgr_, wal_.get(), service_));
    auto fn_calvin = [this](const ptr<calvin_epoch_ops> &ops) {
      calvin_scheduler_->schedule(ops);
    };
    calvin_sequencer_.reset(new calvin_sequencer(conf_, service_, fn_calvin));


    async_run_tx_routine(strand_calvin_, [ccb] {
      ccb->calvin_sequencer_->tick();
    });

  }
#endif // DB_TYPE_CALVIN
  BOOST_LOG_TRIVIAL(info) << "start up CCB " << node_name_ << " ...";
}

void cc_block::on_stop() {
  {

    std::scoped_lock l(timer_send_register_mutex_);
    if (timer_send_register_) {
      timer_send_register_->cancel_and_join();
    }
  }
  {
    std::scoped_lock l(timer_send_status_mutex_);
    if (timer_send_status_) {
      timer_send_status_->cancel_and_join();
    }
  }
  if (deadlock_) {
    deadlock_->stop_and_join();
  }
  // todo elegant exit
  sleep(5);
  // time_.print();
  BOOST_LOG_TRIVIAL(info) << "stop CCB " << node_name_ << " ...";
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
    for (auto rl : rg_lead_) {
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
  auto s = shared_from_this();
  auto fn_timeout = boost::asio::bind_executor(
      timer_strand_,
      [s]() {
        s->send_register();
      });

  {
    std::scoped_lock l(timer_send_register_mutex_);
    if (!timer_send_register_) {
      ptr<timer> pt(new timer(
          strand_send_register_,
          boost::asio::chrono::milliseconds(500),
          fn_timeout
      ));
      timer_send_register_ = pt;
    }
  }
  timer_send_register_->async_tick();
}

void cc_block::send_register() {
  std::scoped_lock l(timer_send_register_mutex_);
  //BOOST_LOG_TRIVIAL(info) << node_name_ << " send register_ccb";
  auto request = std::make_shared<ccb_register_ccb_request>();
  request->set_source(node_id_);
  request->set_dest(rlb_node_id_);
  request->set_cno(cno_);
  auto r1 = service_->async_send(rlb_node_id_, message_type::C2R_REGISTER_REQ,
                                 request);
  if (!r1) {
    BOOST_LOG_TRIVIAL(error) << "send error register_ccb error";
  }

  auto req = std::make_shared<panel_info_request>();
  req->set_source(node_id_);
  req->set_dest(conf_.panel_config().node_id());
  req->set_block_type(pb_block_type::PB_BLOCK_CCB);
  auto r2 = service_->async_send(req->dest(), message_type::PANEL_INFO_REQ, req);
  if (!r2) {
    BOOST_LOG_TRIVIAL(error) << "send PANEL_INFO_REQ error";
  }
}

void cc_block::handle_register_ccb_response(
    const rlb_register_ccb_response &response) {
  std::scoped_lock l(timer_send_register_mutex_);

  auto msg = std::make_shared<panel_report>();
  msg->set_source(conf_.node_id());
  msg->set_dest(conf_.panel_config().node_id());
  msg->set_lead(response.is_lead());
  msg->set_registered(response.source());
  msg->set_report_type(CCB_REGISTERED_RLB);
  auto r = service_->async_send(msg->dest(), message_type::PANEL_REPORT, msg);
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
      auto ccb = shared_from_this();
      auto is_lead = response.is_lead();
      async_run_tx_routine(strand_calvin_, [ccb, is_lead] {
        ccb->calvin_sequencer_->update_local_lead_state(is_lead);
      });
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

result<void> cc_block::ccb_handle_message(const ptr<connection> &,
                                          message_type,
                                          const ptr<rlb_register_ccb_response> m) {
  handle_register_ccb_response(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &c, message_type t, const ptr<tx_request> m) {
  switch (t) {
    case CLIENT_TX_REQ: {
      handle_client_tx_request(c, *m);
      break;
    }
#ifdef DB_TYPE_SHARE_NOTHING
    case TX_TM_REQUEST: {
      handle_tx_tm_request(*m);
      break;
    }
    default: {
      break;
    }
#endif
  }
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<rlb_commit_entries> m) {
  handle_log_entries_commit(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<ccb_broadcast_status> m) {
  handle_broadcast_status_req(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &,
                                          message_type,
                                          const ptr<ccb_broadcast_status_response> m) {
  handle_broadcast_status_resp(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &c, message_type, const ptr<lead_status_request> m) {
  handle_lead_status_request(c, *m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<panel_info_response> m) {
  handle_panel_info_resp(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &,
                                          message_type,
                                          const ptr<rlb_report_status_to_ccb> m) {
  handle_report_status(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<dsb_read_response> m) {
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

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_rm_prepare> m) {
  handle_tx_rm_prepare(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_rm_ack> m) {
  handle_tx_rm_ack(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_tm_commit> m) {
  handle_tx_tm_commit(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_tm_abort> m) {
  handle_tx_tm_abort(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_tm_end> m) {
  handle_tx_tm_end(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<dependency_set> m) {
  handle_dependency_set(m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type t, const ptr<tx_enable_violate> m) {
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  if (t == TM_ENABLE_VIOLATE) {
    handle_tx_tm_enable_violate(*m);
  } else if (t == RM_ENABLE_VIOLATE) {
    handle_tx_rm_enable_violate(*m);
  }
  return outcome::success();
#endif //DB_TYPE_GEO_REP_OPTIMIZE
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<calvin_part_commit> m) {
  handle_calvin_part_commit(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<calvin_epoch> m) {
#ifdef DB_TYPE_CALVIN
  auto ccb = shared_from_this();
  async_run_tx_routine(strand_calvin_, [ccb, m] {
    ccb->calvin_sequencer_->handle_epoch(*m);
  });
#endif
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> &, message_type, const ptr<calvin_epoch_ack> m) {
#ifdef DB_TYPE_CALVIN
  auto ccb = shared_from_this();
  async_run_tx_routine(strand_calvin_, [ccb, m] {
    ccb->calvin_sequencer_->handle_epoch_ack(*m);
  });

#endif
  return outcome::success();
}

uint64_t cc_block::gen_xid(uint32_t terminal_id) {
  uint32_t seq = ++sequence_;
  BOOST_ASSERT(terminal_id != 0);
  uint64_t xid = make_uint64(seq, terminal_id);
  return xid;
}

uint32_t cc_block::xid_to_terminal_id(xid_t xid) {
  uint32_t terminal_id = uint32_t(xid & 0xffffffff);
  return terminal_id;
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

  auto res = std::make_shared<ccb_report_status_response>();
  res->set_source(node_id_);
  res->set_dest(message.source());
  res->set_cno(message.cno());
  auto r = service_->async_send(res->dest(), C2R_REPORT_STATUS_RESP, res);
  if (not r) {
    BOOST_ASSERT(false);
  }

  // clear ACK state for resend when timeout...
  send_status_acked_.clear();
  BOOST_LOG_TRIVIAL(info) << "CCB " << node_name_ << " handle report status, send broadcast status ";

  send_broadcast_status(message.lead());
}

void cc_block::send_broadcast_status(bool lead) {
  for (const node_config &c : conf_.node_config_list()) {
    if (is_ccb_block(c.node_id())) {
      if (!send_status_acked_.contains(c.node_id()) ||
          !send_status_acked_[c.node_id()]) {
        auto msg = std::make_shared<ccb_broadcast_status>();
        msg->set_dest(c.node_id());
        msg->set_source(node_id_);
        msg->set_lead(lead);

        send_status_acked_[c.node_id()] = false;
        auto rs =
            service_->async_send(msg->dest(), CCB_BORADCAST_STATUS_REQ, msg);
        if (!rs) {
          BOOST_LOG_TRIVIAL(info) << "send broadcast status error";
        }
      }
    }
  }

  {
    std::scoped_lock l(timer_send_status_mutex_);
    auto s = shared_from_this();
    auto fn_timeout =
        [s, lead]() {
          std::scoped_lock l(s->mutex_);
          BOOST_LOG_TRIVIAL(info) << s->node_name_ << " timeout , send broadcast status ";
          s->send_broadcast_status(lead);
        };
    if (!timer_send_status_) {
      ptr<timer> t = ptr<timer>(new timer(
          strand_send_status_,
          boost::asio::chrono::milliseconds(STATUS_REPORT_TIMEOUT_MILLIS),
          fn_timeout));
      timer_send_status_ = t;
      t->async_tick();
    } else {
      timer_send_status_->reset_callback(fn_timeout);
    }
  }
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
        auto ccb = shared_from_this();
        async_run_tx_routine(strand_calvin_, [ccb, rg, source_node] {
          ccb->calvin_sequencer_->update_shard_lead(rg, source_node);
        });
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

  auto res = std::make_shared<ccb_broadcast_status_response>();
  res->set_source(node_id_);
  res->set_dest(source_node);
  auto rs = service_->async_send(res->dest(), CCB_BORADCAST_STATUS_RESP, res);
  if (!rs) {
  }
}

void cc_block::handle_broadcast_status_resp(
    const ccb_broadcast_status_response &req) {
  send_status_acked_[req.source()] = true;
  bool all_ack = false;
  if (!send_status_acked_.empty()) {
    all_ack = true;
    for (auto kv : send_status_acked_) {
      if (!kv.second) {
        all_ack = false;
      }
    }
  }
  if (all_ack) {
    std::scoped_lock l(timer_send_status_mutex_);
    if (timer_send_status_) {
      timer_send_status_->cancel();
    }
    return;
  }
}

void cc_block::handle_lead_status_request(const ptr<connection> &conn, const lead_status_request &) {
  //BOOST_ASSERT(node_id_ == msg.dest());

  auto response = std::make_shared<lead_status_response>();
  for (auto p : rg_lead_) {
    response->mutable_lead()->Add(p.second);
    if (TO_RG_ID(p.second) == TO_RG_ID(node_id_)) {
      response->set_rg_lead(p.second);
    }
  }
  service_->conn_async_send(conn, LEAD_STATUS_RESPONSE, response);

}

void cc_block::handle_panel_info_resp(const panel_info_response &msg) {
  for (auto id : msg.ccb_leader()) {
    shard_id_t sid = TO_RG_ID(id);
    rg_lead_[sid] = id;
    if (sid == neighbour_shard_) {
      if (deadlock_) {
        deadlock_->set_next_node(id);
      }
    }
#ifdef DB_TYPE_CALVIN
    if (is_deterministic()) {
      auto ccb = shared_from_this();
      async_run_tx_routine(strand_calvin_, [ccb, sid, id] {
        ccb->calvin_sequencer_->update_shard_lead(sid, id);
      });
    }
#endif // DB_TYPE_CALVIN
  }
}

#ifdef DB_TYPE_NON_DETERMINISTIC

ptr<tx_context> cc_block::create_tx_context_gut(xid_t xid, bool distributed, ptr<connection> conn) {
  boost::asio::io_context::strand strand_tx_context(service_->get_service(SERVICE_ASYNC_CONTEXT));
  BOOST_ASSERT(dsb_node_id_ != 0);
  auto ccb = shared_from_this();
  auto fn_remove = [ccb, xid](uint64_t, rm_state state) {
    if (state == rm_state::RM_ABORTED ||
        state == rm_state::RM_COMMITTED ||
        state == rm_state::RM_ENDED
        ) {
      uint32_t terminal_id = ccb->xid_to_terminal_id(xid);
      ccb->tx_context_[terminal_id].remove(xid, nullptr);
    }
  };
  auto ctx = std::make_shared<tx_context>(
      strand_tx_context,
      xid, node_id_, dsb_node_id_, cno_,
      distributed, mgr_, service_, conn,
      wal_.get(),
      fn_remove,
      deadlock_.get());

  return ctx;
}
void cc_block::create_tx_context(const ptr<connection> &conn, const tx_request &req) {
  uint64_t xid = req.xid();
  //BOOST_LOG_TRIVIAL(debug) << node_name_ << " transaction " << xid
  //                         << " request";
  ptr<tx_context> ctx = create_tx_context_gut(xid, req.distributed(), conn);
  uint32_t terminal_id = xid_to_terminal_id(xid);
  bool ok = tx_context_[terminal_id].insert(xid, ctx);
  if (ok) {
    async_run_tx_routine(ctx->get_strand(),
                         [req, ctx] {
                           ctx->process_tx_request(req);
                         });
  } else {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " existing transaction " << xid;
  }
}

void cc_block::handle_append_log_response(
    const rlb_commit_entries &response) {
  EC ec = EC(response.error_code());
  if (ec != EC::EC_OK) {
    std::set<xid_t> set;

    for (const tx_log &op : response.logs()) {
      auto xid = xid_t(op.xid());
      set.insert(xid);
    }

    for (xid_t xid : set) {
      abort_tx(xid, EC::EC_APPEND_LOG_ERROR);
    }
    return;
  }

  for (const tx_log &xl : response.logs()) {
    xid_t xid = xl.xid();
    tx_cmd_type t = xl.log_type();
    if (fn_schedule_after_ != nullptr) {
      fn_schedule_after_(tx_op(t, xid, xl.operations().size() + 1));
    }
    uint64_t repl_latency = xl.repl_latency();
    switch (t) {
#ifdef DB_TYPE_SHARE_NOTHING
      case TX_CMD_TM_ABORT:
      case TX_CMD_TM_COMMIT:
      case TX_CMD_TM_BEGIN:
      case TX_CMD_TM_END: {
        uint32_t terminal_id = xid_to_terminal_id(xid);
        std::pair<ptr<tx_coordinator>, bool> rc = tx_coordinator_[terminal_id].find(xid);
        if (rc.second) {
          auto tm = rc.first;
          async_run_tx_routine(
              tm->get_strand(),
              [tm, t] {
                tm->on_log_entry_commit(t);
              }
              //BOOST_LOG_TRIVIAL(trace) << "victim tx_rm " << xid;
          );
        } else {
          //BOOST_LOG_TRIVIAL(error) << "cannot find tx_rm :" << xid;
        }
        break;
      }
#endif
      case TX_CMD_RM_PREPARE_ABORT:
      case TX_CMD_RM_PREPARE_COMMIT:
      case TX_CMD_RM_ABORT:
      case TX_CMD_RM_COMMIT:
      case TX_CMD_RM_BEGIN: {
        uint32_t terminal_id = xid_to_terminal_id(xid);
        std::pair<ptr<tx_context>, bool> p = tx_context_[terminal_id].find(xid);
        if (p.second) {
          auto ctx = p.first;
          async_run_tx_routine(
              ctx->get_strand(),
              [repl_latency, t, ctx] {
                ctx->log_rep_delay(repl_latency);
                ctx->on_log_entry_commit(t);
              }
          );

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

void cc_block::handle_read_data_response(ptr<dsb_read_response> response) {
  uint64_t xid = response->xid();
#ifdef DEBUG_SEND_TIME
  auto ms_start = response->debug_send_ts();
  auto ts_recv_read_resp = ms_since_epoch();
  if (ts_recv_read_resp > ms_start + MS_MAX) {
    BOOST_LOG_TRIVIAL(info) << "DSB -> CCB read response " << ts_recv_read_resp -ms_start << "ms";
  }
#endif
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> p = tx_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(
        ctx->get_strand(),
#ifdef DEBUG_SEND_TIME
        [ctx, response, ts_recv_read_resp] {
          auto ts_cc_hand_read_resp = ms_since_epoch();
      if (ts_cc_hand_read_resp > ts_recv_read_resp + MS_MAX) {
        BOOST_LOG_TRIVIAL(info) << "CCB recv read resp -> CCB handle read read resp " << ts_cc_hand_read_resp - ts_recv_read_resp << "ms";
      }
#else
        [ctx, response] {
#endif
          ctx->read_data_from_dsb_response(response);
        });
  } else {
    BOOST_LOG_TRIVIAL(error) << "cannot find transaction read data response , xid=" << xid;
  }
}

void cc_block::handle_non_deterministic_tx_request(const ptr<connection> &conn,
                                                   const tx_request &request) {
  uint64_t xid = gen_xid(request.terminal_id());
  BOOST_LOG_TRIVIAL(trace) << node_name_ << " handle dist=" << request.distributed() << " tx_rm " << xid;
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
    // non one_shot tx_rm
  }
}

ptr<tx_coordinator> cc_block::create_tx_coordinator_gut(const ptr<connection> &conn, const tx_request &req) {
  uint64_t xid = req.xid();

    auto fn_remove = [this](uint64_t xid, tm_state state) {
    if (state == tm_state::TM_DONE) {
      uint32_t terminal_id = xid_to_terminal_id(xid);
      tx_coordinator_[terminal_id].remove(xid, nullptr);
    }
  };
  boost::asio::io_context::strand strand(service_->get_service(SERVICE_ASYNC_CONTEXT));
  ptr<tx_coordinator> c = std::make_shared<tx_coordinator>(
      strand,
      xid, node_id_,
      rg_lead_,
      service_,
      conn,
      wal_.get(),
      fn_remove);
  return c;
}

void cc_block::create_tx_coordinator(const ptr<connection> &conn, const tx_request &req) {
  uint64_t xid = req.xid();
  BOOST_LOG_TRIVIAL(trace) << "transaction " << xid << " request";

  uint32_t terminal_id = xid_to_terminal_id(xid);
  ptr<tx_coordinator> coordinator = create_tx_coordinator_gut(conn, req);
  bool ok = tx_coordinator_[terminal_id].insert(xid, coordinator);
  if (ok) {
    result<void> r = coordinator->handle_tx_request(req);
    if (not r) {
      BOOST_LOG_TRIVIAL(info) << "";
    }
    if (conn != nullptr) {
    }
  } else {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " existing transaction " << xid;
  }

}

void cc_block::handle_tx_rm_prepare(const tx_rm_prepare &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_coordinator>, bool> p = tx_coordinator_[terminal_id].find(xid);
  if (p.second) {
    auto tm = p.first;
    async_run_tx_routine(
        tm->get_strand(),
        [tm, msg] {
          tm->handle_tx_rm_prepare(msg);
        }
        //BOOST_LOG_TRIVIAL(trace) << "victim tx_rm " << xid;
    );

  } else {
    //BOOST_LOG_TRIVIAL(error) << node_name_ << " cannot find tx_rm " << msg.xid();
  }
}

void cc_block::handle_tx_rm_ack(const tx_rm_ack &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_coordinator>, bool> p = tx_coordinator_[terminal_id].find(xid);
  if (p.second) {
    auto tm = p.first;
    async_run_tx_routine(
        tm->get_strand(),
        [tm, msg] {
          tm->handle_tx_rm_ack(msg);
        }
        //BOOST_LOG_TRIVIAL(trace) << "victim tx_rm " << xid;
    );

  } else {
    //BOOST_LOG_TRIVIAL(error) << node_name_ << " cannot find tx_rm " << msg.xid();
  }
}

void cc_block::handle_tx_tm_commit(const tx_tm_commit &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> p = tx_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(
        ctx->get_strand(),
        [ctx, msg] { ctx->handle_tx_tm_commit(msg); }
    );
  } else {
    //BOOST_LOG_TRIVIAL(error) << node_name_ << " cannot find tx_rm " << msg.xid();
  }
}

void cc_block::handle_tx_tm_abort(const tx_tm_abort &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> p = tx_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(
        ctx->get_strand(),
        [ctx, msg] {
          ctx->handle_tx_tm_abort(msg);
        }
    );
  } else {
    //BOOST_LOG_TRIVIAL(error) << node_name_ << " cannot find tx_rm " << msg.xid();
  }
}

void cc_block::handle_dependency_set(const ptr<dependency_set> msg) {
  if (deadlock_) {
    deadlock_->recv_dependency(msg);
  }
}

void cc_block::handle_tx_tm_end(const tx_tm_end &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> p = tx_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(
        ctx->get_strand(),
        [ctx] { ctx->tx_ended(); });
  } else {

  }
}
#endif // DB_TYPE_SHARE_NOTHING

#ifdef DB_TYPE_GEO_REP_OPTIMIZE
void cc_block::handle_tx_tm_enable_violate(const tx_enable_violate &msg) {
  BOOST_ASSERT(msg.dest() == node_id_);
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> r = tx_context_[terminal_id].find(xid);
  if (r.second) {
    ptr<tx_context> ctx = r.first;
    ctx->handle_tx_enable_violate();
  }
}
void cc_block::handle_tx_rm_enable_violate(const tx_enable_violate &msg) {
  BOOST_ASSERT(msg.dest() == node_id_);
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_coordinator>, bool> r = tx_coordinator_[terminal_id].find(xid);
  if (r.second) {
    auto tm = r.first;
    async_run_tx_routine(
        tm->get_strand(),
        [tm, msg] {
          tm->handle_tx_enable_violate(msg);
        }
        //BOOST_LOG_TRIVIAL(trace) << "victim tx_rm " << xid;
    );

  }
}

#endif // DB_TYPE_GEO_REP_OPTIMIZE

#endif // #ifdef DB_TYPE_NON_DETERMINISTIC

#ifdef DB_TYPE_NON_DETERMINISTIC
void cc_block::abort_tx(xid_t xid, EC ec) {
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> r = tx_context_[terminal_id].find(xid);
  if (r.second) {
    auto ctx = r.first;
    async_run_tx_routine(ctx->get_strand(),
                         [ctx, ec] {
                           ctx->abort(ec);
                         }
    );
    //BOOST_LOG_TRIVIAL(trace) << "victim tx_rm " << xid;

  }
#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {
    uint32_t terminal_id = xid_to_terminal_id(xid);
    std::pair<ptr<tx_coordinator>, bool> rc = tx_coordinator_[terminal_id].find(xid);
    if (rc.second) {
      auto tm = rc.first;
      async_run_tx_routine(
          tm->get_strand(),
          [tm, ec] {
            tm->abort(ec);
          }
          //BOOST_LOG_TRIVIAL(trace) << "victim tx_rm " << xid;
      );
    } else {
      //BOOST_LOG_TRIVIAL(info) << id_2_name(node_id_) << " cannot find victim tx_rm " << xid;
    }
  }
#endif
}
#endif

void cc_block::debug_tx(std::ostream &os, xid_t xid) {
  os << "debug tx_rm context: " << std::endl;
#ifdef DB_TYPE_NON_DETERMINISTIC
  auto fn_kv_ctx = [&os, xid](uint64_t k, const ptr<tx_context> &rm) {
    if (xid == 0 || xid == k) {
      rm->debug_tx(os);
    }
  };
  for (size_t i = 0; i < tx_context_.size(); i++) {
    tx_context_[i].traverse(fn_kv_ctx);
  }
#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {
    auto fn_kv_coord = [&os, xid](uint64_t k, const ptr<tx_coordinator> &tm) {
      if (xid == 0 || xid == k) {
        tm->debug_tx(os);
      }
    };
    for (size_t i = 0; i < tx_coordinator_.size(); i++) {
      tx_coordinator_[i].traverse(fn_kv_coord);
    }
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
    for (size_t i = 0; i < calvin_context_.size(); i++) {
      calvin_context_[i].traverse(f1);
    }

    os << "calvin collector:" << std::endl;
    auto f2 = [&os, xid](xid_t k, const ptr<calvin_collector> &t) {
      if (xid == 0 || xid == k) {
        t->debug_tx(os);
      }
    };

    for (size_t i = 0; i < calvin_context_.size(); i++) {
      calvin_collector_[i].traverse(f2);
    }
  }
#endif // DB_TYPE_CALVIN
}

void cc_block::debug_lock(std::ostream &os) {
  os << "debug tx_rm lock: " << std::endl;
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
  boost::asio::io_context::strand s(strand_calvin_);
  auto fn_remove = [this](xid_t xid) {
    return remove_calvin_context(xid);
  };
  ptr<calvin_context> calvin_ctx = std::make_shared<calvin_context>(
      s,
      req.xid(),
      conf_.node_id(),
      dsb_node_id_,
      cno_,
      std::make_shared<tx_request>(req),
      service_,
      mgr_,
      fn_remove);
  uint32_t terminal_id = xid_to_terminal_id(req.xid());
  calvin_context_[terminal_id].insert(req.xid(), calvin_ctx);
  BOOST_ASSERT(calvin_ctx);
  return calvin_ctx;
}

void cc_block::remove_calvin_context(xid_t xid) {
  uint32_t terminal_id = xid_to_terminal_id(xid);
  calvin_context_[terminal_id].remove(xid, nullptr);
}

void cc_block::handle_calvin_tx_request(ptr<connection> conn,
                                        const tx_request &request) {
  xid_t xid = gen_xid(request.terminal_id());
  uint32_t terminal_id = xid_to_terminal_id(xid);
  boost::asio::io_context::strand strand_calvin(strand_calvin_);
  ptr<calvin_collector> collector(new calvin_collector(strand_calvin, xid, std::move(conn), service_, request));
  collector->mutable_request().set_source(node_id_);
  bool ok = calvin_collector_[terminal_id].insert(xid, collector);
  if (!ok) {
    BOOST_LOG_TRIVIAL(error) << "existing long1";
  }
  // collector->request() assigned xid
  auto ccb = shared_from_this();
  async_run_tx_routine(strand_calvin_, [ccb, collector] {
    ccb->calvin_sequencer_->handle_tx_request(collector->request());
  });
}

void cc_block::handle_calvin_log_commit(const rlb_commit_entries &msg) {
  for (const auto &op : msg.logs()) {
    xid_t xid = op.xid();
    uint32_t terminal_id = xid_to_terminal_id(xid);
    std::pair<ptr<calvin_context>, bool> r = calvin_context_[terminal_id].find(op.xid());
    if (r.second) {
      auto ctx = r.first;
      auto ccb = shared_from_this();
      async_run_tx_routine(ctx->get_strand(), [xid, terminal_id, ccb, ctx, op] {
        bool committed = ctx->on_operation_committed(op);
        if (committed) {
          ccb->calvin_context_[terminal_id].remove(xid, nullptr);
        }
      });
    } else {
      //BOOST_LOG_TRIVIAL(error) << "cannot find tx_rm " << op.xid();
    }
  }
}

void cc_block::handle_calvin_part_commit(const calvin_part_commit &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<calvin_collector>, bool> r = calvin_collector_[terminal_id].find(xid);
  if (r.second) {
    bool all_committed = r.first->part_commit(msg);
    if (all_committed) {
      calvin_collector_[terminal_id].remove(xid, nullptr);
    }
  } else {
    BOOST_LOG_TRIVIAL(error) << "can not find long1 " << xid;
  }
}

void cc_block::handle_calvin_read_response(const ptr<dsb_read_response> msg) {
  uint64_t xid = msg->xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<calvin_context>, bool> p = calvin_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(ctx->get_strand(), [ctx, msg] {
      ctx->read_response(*msg);
    });

  } else {
    BOOST_LOG_TRIVIAL(error) << "cannot find transaction " << xid;
  }
}

void cc_block::async_run_tx_routine(
    boost::asio::io_context::strand strand,
    std::function<void()> routine) {
  boost::asio::post(
      strand,
      routine);
}
#endif // DB_TYPE_CALVIN