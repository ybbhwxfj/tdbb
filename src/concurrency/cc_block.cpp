#include "concurrency/cc_block.h"
#include "common/db_type.h"
#include "common/debug_url.h"
#include "common/json_pretty.h"
#include "common/make_int.h"
#include "common/result.hpp"
#include "common/timer.h"
#include "common/shard2node.h"
#include <charconv>
#include <memory>
#include <utility>

cc_block::cc_block(const config &conf, net_service *service,
                   fn_schedule_before fn_before, fn_schedule_after fn_after)
    : conf_(conf), cno_(0), leader_(false), node_id_(conf.node_id()),
      node_name_(id_2_name(conf.node_id())),
      rlb_node_id_(conf.register_to_node_id()), cc_opt_dsb_node_id_(std::nullopt),
      neighbour_shard_(0), registered_(false), mgr_(nullptr), service_(service),
      sequence_(0), wal_(new write_ahead_log(
        conf.node_id(), conf.register_to_node_id(), service)),
#ifdef DB_TYPE_CALVIN
      strand_calvin_(service->get_service(SERVICE_ASYNC_CONTEXT)),
#endif
      fn_schedule_after_(fn_after), time_("CCB h msg"),
      strand_ccb_tick_(service->get_service(SERVICE_ASYNC_CONTEXT)) {
  auto fn = [this](xid_t xid) { abort_tx(xid, EC::EC_VICTIM); };
  rg_lead_ = conf_.priority_lead_nodes();
  deadlock_ = cs_new<deadlock>(
      fn, service_,
      conf_.num_rg(), conf_.get_test_config().deadlock_detection(),
      conf_.get_test_config().deadlock_detection_ms(),
      conf_.get_test_config().lock_timeout_ms()
  );
  mgr_ = new access_mgr(service_, deadlock_.get(), std::move(fn_before),
                        std::move(fn_after),
                        conf_.all_shard_ids(),
                        MAX_TABLES);
  BOOST_ASSERT(node_id_ != 0);
  BOOST_ASSERT(rlb_node_id_ != 0);
  auto ids = conf_.shard_ids();
  std::set<shard_id_t> sids = std::set<shard_id_t>(ids.begin(), ids.end());
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
  uint64_t num_max_terminal = conf_.final_num_terminal() + 10;
  tx_context_.resize(num_max_terminal);
#ifdef DB_TYPE_SHARE_NOTHING
  tx_coordinator_.resize(num_max_terminal);
#endif
#ifdef DB_TYPE_CALVIN
  calvin_context_.resize(num_max_terminal);
  calvin_collector_.resize(num_max_terminal);
#endif // DB_TYPE_CALVIN
}

cc_block::~cc_block() { delete mgr_; }

void cc_block::on_start() {

#ifdef DB_TYPE_NON_DETERMINISTIC
  for (auto p : rg_lead_) {
    if (p.first == neighbour_shard_) {
      if (deadlock_) {
        LOG(trace) << "neighbour " << id_2_name(node_id_) << " "
                   << id_2_name(p.second);
        deadlock_->set_next_node(p.second);
      }
    }
  }
  deadlock_->start();
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

    calvin_scheduler_.reset(
        new calvin_scheduler(fn_find, conf_, mgr_, wal_.get(), service_));
    auto scheduler = calvin_scheduler_;
    auto fn_calvin = [scheduler](const ptr<calvin_epoch_ops> &ops) {
      scheduler->schedule(ops);
    };
    calvin_sequencer_.reset(
        new calvin_sequencer(conf_, strand_calvin_, service_, fn_calvin));
    auto sequencer = calvin_sequencer_;
    async_run_tx_routine(strand_calvin_, [sequencer] {
      scoped_time _t("calvin_sequencer::start");
      sequencer->tick();
    });
  }
#endif // DB_TYPE_CALVIN
  LOG(info) << "start up CCB " << node_name_ << " ...";
}

void cc_block::on_stop() {
  {

    if (timer_send_register_) {
      timer_send_register_->cancel_and_join();
    }
  }

  if (deadlock_) {
    deadlock_->stop_and_join();
  }
  // todo elegant exit
  sleep(5);
  // time_.print();
  LOG(info) << "stop CCB " << node_name_ << " ...";
}

void cc_block::handle_debug(const std::string &path, std::ostream &os) {
  if (not boost::regex_match(path, url_json_prefix)) {
    os << BLOCK_CCB << " name:" << node_name_ << std::endl;
    os << "endpoint:" << conf_.this_node_config().node_peer() << std::endl;
    os << "register_to:" << id_2_name(conf_.register_to_node_id()) << std::endl;
    os << "path:" << path << std::endl;
    os << "registered " << registered_ << std::endl;
    os << "registered to lead " << (rg_lead_[TO_RG_ID(node_id_)] == node_id_)
       << std::endl;
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
  auto fn_timeout = boost::asio::bind_executor(strand_ccb_tick_,
                                               [s]() { s->send_register(); });

  {
    if (!timer_send_register_) {
      ptr<timer> pt(new timer(
          strand_ccb_tick_,
          boost::asio::chrono::milliseconds(CCB_REGISTER_TIMEOUT_MILLIS),
          fn_timeout));
      timer_send_register_ = pt;
    }
  }
  timer_send_register_->async_tick();
}

void cc_block::send_register() {
  // LOG(info) << node_name_ << " send register_ccb";
  auto request = cs_new<ccb_register_ccb_request>();
  if (!registered_) {
    cno_ = 0;
    if (wal_) {
      wal_->set_cno(cno_);
    }
    for (auto id : conf_.shard_ids()) {
      request->add_shard_ids(id);
    }
  } else {

  }
  request->set_source(node_id_);
  request->set_dest(rlb_node_id_);
  request->set_cno(cno_);
  auto r1 = service_->async_send(rlb_node_id_, message_type::C2R_REGISTER_REQ,
                                 request);
  if (!r1) {
    LOG(error) << "send error register_ccb error";
  }
}

void cc_block::strand_ccb_handle_register_ccb_response(
    const rlb_register_ccb_response &response) {

  if (cno_ != response.cno()) {
    std::string lead = response.is_lead() ? "leader" : "follower";
    LOG(info) << "CCB " << node_name_ << " registered to RLB "
              << id_2_name(response.source()) << " cno = " << response.cno() << " "
              << lead;

    send_status_acked_.clear();
    leader_ = false;
    if (leader_ != response.is_lead()) {

      leader_ = response.is_lead();
    }
    cno_ = response.cno();
    if (wal_) {
      wal_->set_cno(cno_);
    }
  } else {
    return;
  }
#ifdef DB_TYPE_CALVIN
  if (is_deterministic()) {
    auto ccb = shared_from_this();
    auto is_lead = response.is_lead();
    async_run_tx_routine(strand_calvin_, [ccb, is_lead] {
      scoped_time _t("calvin_sequencer::update_local_lead_state");
      ccb->calvin_sequencer_->update_local_lead_state(is_lead);
    });
  }
#endif // DB_TYPE_CALVIN

  for (int i = 0; i < response.shard_ids_size(); i++) {
    shard_id_t shard_id = response.shard_ids(i);
    node_id_t node_id = response.dsb_node_ids(i);
    dsb_shard2node_.insert(std::make_pair(shard_id, node_id));
  }

  if (dsb_shard2node_.size()==1) {
    cc_opt_dsb_node_id_ = std::optional<node_id_t>(dsb_shard2node_.begin()->second);
  }
  registered_ = true;
}

void cc_block::handle_client_tx_request(const ptr<connection> conn,
                                        const ptr<tx_request> request) {
  EC ec = EC::EC_OK;
  LOG(trace) << node_name_ << " handle tx request "
             << " leader"
             << id_2_name(conf_.get_largest_priority_node_of_shard(
                 TO_RG_ID(node_id_)));

  BOOST_ASSERT(request->client_request());
  BOOST_ASSERT(conn != nullptr);
  BOOST_ASSERT(mgr_);
  BOOST_ASSERT(service_);
  if (request->operations_size() > 0) {
    if (request->oneshot()) {
#ifdef DB_TYPE_CALVIN
      if (is_deterministic()) {
        handle_calvin_tx_request(conn, request);
      }
#endif // DB_TYPE_CALVIN
#ifdef DB_TYPE_NON_DETERMINISTIC
      if (is_non_deterministic()) {
        handle_non_deterministic_tx_request(conn, request);
      }
#endif // DB_TYPE_NON_DETERMINISTIC
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

result<void> cc_block::ccb_handle_message(const ptr<connection> conn,
                                          message_type,
                                          const ptr<ccb_state_req> req) {

  auto s = shared_from_this();
  LOG(info) << "receive request CCB state " << id_2_name(node_id_) << "s" << req->shard_id() << "t" << req->term_id();
  auto fn_handle_response = [s, conn, req]() {
    s->strand_ccb_handle_state(conn, req);
  };
  boost::asio::post(strand_ccb_tick_, fn_handle_response);
  return outcome::success();
}

result<void>
cc_block::ccb_handle_message(const ptr<connection>, message_type,
                             const ptr<rlb_register_ccb_response> m) {
  auto s = shared_from_this();
  auto fn_handle_ccb_response = [s, m]() {
    s->strand_ccb_handle_register_ccb_response(*m);
  };
  boost::asio::post(strand_ccb_tick_, fn_handle_ccb_response);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> c, message_type,
                                          const ptr<warm_up_req> req) {
  BOOST_ASSERT(req->term_id() != 0);
  LOG(trace) << id_2_name(conf_.node_id()) << " CCB handle warm up, term_id: " << req->term_id();
  std::pair<ptr<connection>, bool> pair =
      term_connection_table_.find(req->term_id());
  ptr<connection> conn;
  if (pair.second) {
    conn = pair.first;
  } else {
    conn = c;
    term_connection_table_.insert(req->term_id(), conn);
  }

  if (!registered_) {
    ptr<warm_up_resp> warm_up_resp_to_client(cs_new<warm_up_resp>());
    warm_up_resp_to_client->set_term_id(req->term_id());
    warm_up_resp_to_client->set_ok(false);
    service_->conn_async_send(conn, CLIENT_HANDLE_WARM_UP_RESP,
                              warm_up_resp_to_client);
    return outcome::success();
  } else {
    if (req->tuple_key().empty()) {
      PANIC("cannot find this shard id");
      return outcome::failure(EC::EC_NOT_FOUND_ERROR);
    }
    shard_id_t shard_id = req->tuple_key().begin()->shard_id();
    auto iter = dsb_shard2node_.find(shard_id);
    if (iter == dsb_shard2node_.end()) {
      PANIC("cannot find this shard id");
      return outcome::failure(EC::EC_NOT_FOUND_ERROR);
    }
    node_id_t dest_node_id = iter->second;
    const_cast<warm_up_req &>(*req).set_source(node_id_);
    const_cast<warm_up_req &>(*req).set_dest(dest_node_id);
    auto result = service_->async_send(dest_node_id,
                                       DSB_HANDLE_WARM_UP_REQ, req, true);
    if (!result) {
      LOG(error) << id_2_name(conf_.node_id()) << "send to RLB error, "
                 << id_2_name(req->source());
    } else {
      return result;
    }
    return outcome::success();
  }
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<warm_up_resp> resp) {
  LOG(trace) << id_2_name(conf_.node_id()) << " CCB handle warm up resp, " << "DSB:" << id_2_name(resp->source()) << ", term id: " << resp->term_id();

  for (const tuple_row &row : resp->tuple_row()) {
    tuple_pb t;
    swap(t, const_cast<tuple_pb &>(row.tuple()));
    mgr_->put(row.table_id(), row.shard_id(), row.tuple_id(), std::move(t));
  }

  std::pair<ptr<connection>, bool> pair =
      term_connection_table_.find(resp->term_id());
  ptr<connection> conn;
  if (pair.second) {
    conn = pair.first;
  } else {
    return outcome::failure(EC::EC_NOT_FOUND_ERROR);
  }
  ptr<warm_up_resp> warm_up_resp_to_client(cs_new<warm_up_resp>());
  warm_up_resp_to_client->set_term_id(resp->term_id());
  warm_up_resp_to_client->set_ok(true);
  service_->conn_async_send(conn, CLIENT_HANDLE_WARM_UP_RESP,
                            warm_up_resp_to_client);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> c,
                                          message_type t,
                                          const ptr<tx_request> m) {
  switch (t) {
  case CLIENT_TX_REQ: {
    handle_client_tx_request(c, m);
    break;
  }
#ifdef DB_TYPE_SHARE_NOTHING
  case TX_TM_REQUEST: {
    handle_tx_tm_request(*m);
    break;
  }
  default: {
    assert(false);
    break;
  }
#endif
  }
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<rlb_commit_entries> m) {
  handle_log_entries_commit(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection> c, message_type,
                                          const ptr<lead_status_request> m) {
  handle_lead_status_request(c, *m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<dsb_read_response> m) {
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

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<tx_rm_prepare> m) {
  handle_tx_rm_prepare(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<tx_rm_ack> m) {
  handle_tx_rm_ack(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<tx_tm_commit> m) {
  handle_tx_tm_commit(*m);
  return outcome::success();
}
result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<tx_tm_abort> m) {
  handle_tx_tm_abort(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<tx_tm_end> m) {
  handle_tx_tm_end(*m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<dependency_set> m) {
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

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<calvin_part_commit> m) {
  handle_calvin_part_commit(m);
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<calvin_epoch> m) {
#ifdef DB_TYPE_CALVIN
  auto ccb = shared_from_this();
  async_run_tx_routine(strand_calvin_, [ccb, m] {
    scoped_time _t("calvin_context::handle_epoch");
    ccb->calvin_sequencer_->handle_epoch(*m);
  });
#endif
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<calvin_epoch_ack> m) {
#ifdef DB_TYPE_CALVIN
  auto ccb = shared_from_this();
  async_run_tx_routine(strand_calvin_, [ccb, m] {
    scoped_time _t("calvin_sequencer::handle_epoch_ack");
    ccb->calvin_sequencer_->handle_epoch_ack(*m);
  });

#endif
  return outcome::success();
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<tx_victim> m) {
#ifdef DB_TYPE_SHARE_NOTHING
  abort_tx(m->xid(), EC::EC_VICTIM);
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
#ifdef TEST_APPEND_TIME
  uint64_t begin = msg.debug_send_ts();
  uint64_t ms = steady_clock_ms_since_epoch() - begin;
  if (ms > TEST_NETWORK_MS_MAX +
               conf_.get_test_config().debug_add_wan_latency_ms()) {
    LOG(info) << "COMMIT_LOG_ENTRIES message "
              << ms - conf_.get_test_config().debug_add_wan_latency_ms();
  }
#endif
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

void cc_block::handle_lead_status_request(const ptr<connection> conn,
                                          const lead_status_request &) {
  // BOOST_ASSERT(node_id_ == msg.dest());

  auto response = std::make_shared<lead_status_response>();
  for (auto p : rg_lead_) {
    response->mutable_lead()->Add(p.second);
    if (TO_RG_ID(p.second) == TO_RG_ID(node_id_)) {
      response->set_rg_lead(p.second);
    }
  }
  service_->conn_async_send(conn, LEAD_STATUS_RESPONSE, response);
}

#ifdef DB_TYPE_NON_DETERMINISTIC

ptr<tx_context> cc_block::create_tx_context_gut(xid_t xid, bool distributed,
                                                ptr<connection> conn) {
  boost::asio::io_context::strand strand_tx_context(
      service_->get_service(SERVICE_ASYNC_CONTEXT));
  BOOST_ASSERT(!cc_opt_dsb_node_id_.has_value() || cc_opt_dsb_node_id_.value() != 0);
  auto ccb = shared_from_this();
  auto fn_remove = [ccb, xid](uint64_t, rm_state state) {
    if (state == rm_state::RM_ABORTING || state == rm_state::RM_COMMITTING ||
        state == rm_state::RM_ENDED) {
      uint32_t terminal_id = ccb->xid_to_terminal_id(xid);
      bool remove_ok = ccb->tx_context_[terminal_id].remove(xid, nullptr);
      if (!remove_ok) {
        LOG(warning) << "remove " << xid << " no such transaction";
      }
    }
  };
  auto ctx = cs_new<tx_context>(
      strand_tx_context, xid,
      node_id_,
      cc_opt_dsb_node_id_,
      dsb_shard2node_,
      cno_, distributed,
      mgr_, service_, conn, wal_.get(), fn_remove, deadlock_.get());

  return ctx;
}
void cc_block::create_tx_context(const ptr<connection> conn,
                                 const tx_request &req) {
  uint64_t xid = req.xid();
  // LOG(debug) << node_name_ << " transaction " << xid
  //                          << " request";
  ptr<tx_context> ctx = create_tx_context_gut(xid, req.distributed(), conn);
  uint32_t terminal_id = xid_to_terminal_id(xid);
  bool ok = tx_context_[terminal_id].insert(xid, ctx);
  if (ok) {
    async_run_tx_routine(ctx->get_strand(), [req, ctx] {
      scoped_time _t("tx_context::process_tx_request");
      ctx->process_tx_request(req);
    });
  } else {
    LOG(error) << node_name_ << " existing transaction " << xid;
  }
}

struct log_proto_set {
  std::vector<std::pair<ptr<tx_log_proto>, uint64_t>> proto_;
  std::set<xid_t> xid_;
};

ptr<log_proto_set>
rlb_commit_entries_to_proto_set(node_id_t node_id, const rlb_commit_entries &message) {
  ptr<log_proto_set> set(cs_new<log_proto_set>());

  handle_repeated_tx_logs_to_proto_and_timestamp(
      message.repeated_tx_logs(), [node_id, set](ptr<tx_log_proto> p, uint64_t ts, node_id_t id) {
        if (node_id == id) {
          set->proto_.push_back(std::make_pair(p, ts));
          set->xid_.insert(p->xid());
        }
      });
  return set;
}

void cc_block::handle_append_log_response(const rlb_commit_entries &response) {
  ptr<log_proto_set> proto_set = rlb_commit_entries_to_proto_set(node_id_, response);

  EC ec = EC(response.error_code());
  auto ts = std::chrono::steady_clock::now();
  if (ec != EC::EC_OK) {
    for (xid_t xid : proto_set->xid_) {
      abort_tx(xid, EC::EC_APPEND_LOG_ERROR);
    }
    return;
  }

  for (auto pair : proto_set->proto_) {
    tx_log_proto &xl = *pair.first;
    uint64_t repl_latency = pair.second;
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
      uint32_t terminal_id = xid_to_terminal_id(xid);
      std::pair<ptr<tx_coordinator>, bool> rc =
          tx_coordinator_[terminal_id].find(xid);
      if (rc.second) {
        auto tm = rc.first;
        async_run_tx_routine(
            tm->get_strand(),
            [tm, t] {
              scoped_time _t("tx_coordinator::on_log_entry_commit");
              tm->on_log_entry_commit(t);
            }
            // LOG(trace) << "victim tx_rm " << xid;
        );
      } else {
        LOG(error) << "cannot find tx_rm :" << xid;
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
        async_run_tx_routine(ctx->get_strand(), [repl_latency, t, ctx, ts] {
          scoped_time _t("tx_context::on_log_entry_commit");
          ctx->log_rep_delay(repl_latency);
          ctx->on_log_entry_commit(t, ts);
        });

      } else {
        LOG(error) << "cannot find long1 :" << xid;
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

  auto ts = std::chrono::steady_clock::now();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> p = tx_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(ctx->get_strand(), [ts, ctx, response] {
      scoped_time _t("tx_context::read_data_from_dsb_response");
      ctx->read_data_from_dsb_response(response, ts);
    });
  } else {
    LOG(error) << "cannot find transaction read data response , xid=" << xid;
  }
}

void cc_block::handle_non_deterministic_tx_request(
    const ptr<connection> conn, const ptr<tx_request> request) {
  uint64_t xid = gen_xid(request->terminal_id());
  LOG(trace) << node_name_ << " handle dist=" << request->distributed()
             << " tx_rm " << xid;
  const_cast<tx_request &>(*request).set_xid(xid);
  if (request->distributed()) {
#ifdef DB_TYPE_SHARE_NOTHING
    if (is_shared_nothing()) {
      create_tx_coordinator(conn, *request);
    }
#endif
  } else {
    create_tx_context(conn, *request);
  }
}

#ifdef DB_TYPE_SHARE_NOTHING

void cc_block::handle_tx_tm_request(const tx_request &request) {
  LOG(trace) << node_name_ << "handle RM request " << request.xid();
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

ptr<tx_coordinator>
cc_block::create_tx_coordinator_gut(const ptr<connection> conn,
                                    const tx_request &req) {
  uint64_t xid = req.xid();

  auto fn_remove = [this](uint64_t xid, tm_state state) {
    if (state == tm_state::TM_DONE) {
      uint32_t terminal_id = xid_to_terminal_id(xid);
      tx_coordinator_[terminal_id].remove(xid, nullptr);
    }
  };
  boost::asio::io_context::strand strand(
      service_->get_service(SERVICE_ASYNC_CONTEXT));
  ptr<tx_coordinator> c = std::make_shared<tx_coordinator>(
      strand, xid, node_id_, rg_lead_, service_, conn, wal_.get(), fn_remove);
  return c;
}

void cc_block::create_tx_coordinator(const ptr<connection> conn,
                                     const tx_request &req) {
  uint64_t xid = req.xid();
  LOG(trace) << "transaction " << xid << " request";

  uint32_t terminal_id = xid_to_terminal_id(xid);
  ptr<tx_coordinator> coordinator = create_tx_coordinator_gut(conn, req);
  bool ok = tx_coordinator_[terminal_id].insert(xid, coordinator);
  if (ok) {
    result<void> r = coordinator->handle_tx_request(req);
    if (not r) {
      LOG(info) << "";
    }
    if (conn != nullptr) {
    }
  } else {
    LOG(error) << node_name_ << " existing transaction " << xid;
  }
}

void cc_block::handle_tx_rm_prepare(const tx_rm_prepare &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_coordinator>, bool> p =
      tx_coordinator_[terminal_id].find(xid);
  if (p.second) {
    auto tm = p.first;
    async_run_tx_routine(
        tm->get_strand(),
        [tm, msg] {
          scoped_time _t("tx_coordinator::handle_tx_rm_prepare");
          tm->handle_tx_rm_prepare(msg);
        }
        // LOG(trace) << "victim tx_rm " << xid;
    );

  } else {
    // LOG(error) << node_name_ << " cannot find tx_rm " << msg.xid();
  }
}

void cc_block::handle_tx_rm_ack(const tx_rm_ack &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_coordinator>, bool> p =
      tx_coordinator_[terminal_id].find(xid);
  if (p.second) {
    auto tm = p.first;
    async_run_tx_routine(tm->get_strand(),
                         [tm, msg] {
                           scoped_time _t("tx_coordinator::handle_tx_rm_ack");
                           tm->handle_tx_rm_ack(msg);
                         }
        // LOG(trace) << "victim tx_rm " << xid;
    );

  } else {
    // LOG(error) << node_name_ << " cannot find tx_rm " << msg.xid();
  }
}

void cc_block::handle_tx_tm_commit(const tx_tm_commit &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> p = tx_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(ctx->get_strand(), [ctx, msg] {
      scoped_time _t("tx_coordinator::handle_tx_tm_commit");
      ctx->handle_tx_tm_commit(msg);
    });
  } else {
    // LOG(error) << node_name_ << " cannot find tx_rm " << msg.xid();
  }
}

void cc_block::handle_tx_tm_abort(const tx_tm_abort &msg) {
  xid_t xid = msg.xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<tx_context>, bool> p = tx_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(ctx->get_strand(), [ctx, msg] {
      scoped_time _t("tx_coordinator::handle_tx_tm_abort");
      ctx->handle_tx_tm_abort(msg);
    });
  } else {
    // LOG(error) << node_name_ << " cannot find tx_rm " << msg.xid();
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
    async_run_tx_routine(ctx->get_strand(), [ctx] {
      scoped_time _t("tx_coordinator::tx_ended");
      ctx->tx_ended();
    });
  } else {
  }
}



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

#endif // DB_TYPE_SHARE_NOTHING
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC

#ifdef DB_TYPE_NON_DETERMINISTIC
void cc_block::abort_tx(xid_t xid, EC ec) {
  uint32_t terminal_id = xid_to_terminal_id(xid);

  std::pair<ptr<tx_context>, bool> r = tx_context_[terminal_id].find(xid);
  if (r.second) {
    auto ctx = r.first;
    async_run_tx_routine(ctx->get_strand(), [ctx, ec] {
      scoped_time _t("tx_context::abort");
      ctx->abort(ec);
    });
    // LOG(trace) << "victim tx_rm " << xid;
  }
#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {

    std::pair<ptr<tx_coordinator>, bool> rc =
        tx_coordinator_[terminal_id].find(xid);
    if (rc.second) {
      auto tm = rc.first;
      async_run_tx_routine(tm->get_strand(),
                           [tm, ec] {
                             scoped_time _t("tx_coordinator::abort");
                             tm->abort(ec);
                           }
          // LOG(trace) << "victim tx_rm " << xid;
      );
    } else {
      // LOG(info) << id_2_name(node_id_) << " cannot find victim tx_rm " <<
      // xid;
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
  auto fn_remove = [this](xid_t xid) { return remove_calvin_context(xid); };
  ptr<calvin_context> calvin_ctx = std::make_shared<calvin_context>(
      s,
      req.xid(),
      conf_.node_id(),
      cc_opt_dsb_node_id_,
      dsb_shard2node_,
      cno_,
      cs_new<tx_request>(req),
      service_,
      mgr_, fn_remove);
  calvin_ctx->begin();
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
                                        const ptr<tx_request> request) {
  xid_t xid = gen_xid(request->terminal_id());
  uint32_t terminal_id = xid_to_terminal_id(xid);
  boost::asio::io_context::strand strand_calvin(strand_calvin_);
  ptr<calvin_collector> collector(new calvin_collector(
      strand_calvin, xid, std::move(conn), service_, request));

  LOG(trace) << node_name_ << " handle dist=" << request->distributed()
             << " calvin " << xid;
  bool ok = calvin_collector_[terminal_id].insert(xid, collector);
  if (!ok) {
    LOG(error) << "existing xid " << xid;
  }
  // collector->request() assigned xid
  auto ccb = shared_from_this();
  async_run_tx_routine(strand_calvin_, [ccb, collector] {
    scoped_time _t("calvin_sequencer::handle_tx_request");
    ccb->calvin_sequencer_->handle_tx_request(collector->request());
  });
}

void cc_block::handle_calvin_log_commit(const rlb_commit_entries &msg) {
  ptr<log_proto_set> proto_set = rlb_commit_entries_to_proto_set(node_id_, msg);

  for (auto pair : proto_set->proto_) {
    tx_log_proto &log_proto = *pair.first;
    xid_t xid = log_proto.xid();

    LOG(trace) << node_name_ << " calvin log commit " << xid << ",log type" << log_proto.log_type() << ", ptr:" << &msg;

    uint32_t terminal_id = xid_to_terminal_id(xid);
    std::pair<ptr<calvin_context>, bool> r =
        calvin_context_[terminal_id].find(xid);
    if (r.second) {
      auto ctx = r.first;
      auto ccb = shared_from_this();
      async_run_tx_routine(
          ctx->get_strand(), [xid, terminal_id, ccb, ctx, log_proto] {
            bool committed = ctx->on_operation_committed(log_proto);
            if (committed) {
              scoped_time _t("remove calvin context");
              ccb->calvin_context_[terminal_id].remove(xid, nullptr);
            }
          });
    } else {
      // LOG(error) << "cannot find tx_rm " << op.xid();
    }
  }
}

void cc_block::handle_calvin_part_commit(const ptr<calvin_part_commit> msg) {
  xid_t xid = msg->xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<calvin_collector>, bool> r =
      calvin_collector_[terminal_id].find(xid);
  if (r.second) {
    ptr<calvin_collector> collector = r.first;
    auto ccb = shared_from_this();
    async_run_tx_routine(
        collector->get_strand(), [ccb, collector, terminal_id, xid, msg] {
          bool all_committed = collector->part_commit(msg);
          if (all_committed) {
            ccb->calvin_collector_[terminal_id].remove(xid, nullptr);
          }
        });
  } else {
    LOG(error) << "can not find long1 " << xid;
  }
}

void cc_block::handle_calvin_read_response(const ptr<dsb_read_response> msg) {
  uint64_t xid = msg->xid();
  uint32_t terminal_id = xid_to_terminal_id(xid);
  std::pair<ptr<calvin_context>, bool> p =
      calvin_context_[terminal_id].find(xid);
  if (p.second) {
    auto ctx = p.first;
    async_run_tx_routine(ctx->get_strand(), [ctx, msg] {
      scoped_time _t("tx_context::read_response");
      ctx->read_response(*msg);
    });

  } else {
    LOG(error) << "cannot find transaction " << xid;
  }
}

#endif // DB_TYPE_CALVIN

void cc_block::async_run_tx_routine(boost::asio::io_context::strand strand,
                                    std::function<void()> routine) {
  boost::asio::post(strand, routine);
}

void cc_block::strand_ccb_handle_state(ptr<connection> conn, ptr<ccb_state_req> req) {
  ptr<ccb_state_resp> resp(new ccb_state_resp);
  bool start = req->number() > 0;
  resp->set_cno(cno_);
  resp->set_lead(leader_);
  resp->set_node_id(node_id_);
  resp->set_term_id(req->term_id());
  resp->set_shard_id(req->shard_id());
#ifdef DB_TYPE_CALVIN
  if (is_deterministic()) {
    if (start) {
      calvin_sequencer_->start();
    }
  }
#endif
  auto r = conn->async_send(CLIENT_CCB_STATE_RESP, resp);
  if (!r) {
    LOG(error) << " send ccb state resp error";
    return;
  }
  LOG(trace) << " send response CCB state " << id_2_name(node_id_) << "s" << req->shard_id() << "t" << req->term_id();
}

result<void> cc_block::ccb_handle_message(const ptr<connection>, message_type,
                                          const ptr<error_consistency> m) {
  LOG(error) << "error consistency no " << m->cno() << ", current:" << cno_;
  registered_ = false;
  return outcome::success();
}

node_id_t cc_block::shard2node(shard_id_t shard_id) {
  return node_id_of_shard(shard_id, cc_opt_dsb_node_id_, dsb_shard2node_);
}