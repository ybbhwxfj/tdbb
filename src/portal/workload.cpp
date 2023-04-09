#include "portal/workload.h"
#include "common/bench_result.h"
#include "common/db_type.h"
#include "common/logger.hpp"
#include "common/make_key.h"
#include "common/panic.h"
#include "common/table_id.h"
#include "common/time_tracer.h"
#include "common/tuple.h"
#include "network/client.h"
#include "network/db_client.h"
#include <boost/assert.hpp>
#include <boost/filesystem.hpp>
#include <iostream>

void per_terminal::reset_database_connection() {
  node_id_ = 0;
  client_conn_.reset();
}

void per_terminal::update(uint32_t commit, uint32_t abort, uint32_t total,
                          uint32_t num_part,
                          std::chrono::nanoseconds commit_duration,
                          std::chrono::nanoseconds duration_append_log,
                          std::chrono::nanoseconds duration_replicate_log,
                          std::chrono::nanoseconds duration_read,
                          std::chrono::nanoseconds duration_read_dsb,
                          std::chrono::nanoseconds duration_lock_wait,
                          std::chrono::nanoseconds duration_part,
                          uint32_t num_lock, uint32_t num_read_violate,
                          uint32_t num_write_violate

) {
  std::scoped_lock l(mutex_);
  result_.num_commit += commit;
  result_.num_abort += abort;
  result_.num_tx += total;
  result_.commit_duration += commit_duration;
  result_.duration_append_log += duration_append_log;
  result_.duration_replicate_log += duration_replicate_log;
  result_.duration_read += duration_read;
  result_.duration_read_dsb += duration_read_dsb;
  result_.duration_lock_wait += duration_lock_wait;
  result_.duration_part += duration_part;
  result_.num_part += num_part;
  result_.num_lock += num_lock;
  result_.num_write_violate += num_write_violate;
  result_.num_read_violate += num_read_violate;
}

tpm_statistic per_terminal::get_result() {
  std::scoped_lock l(mutex_);
  auto r = result_;
  result_.reset();
  return r;
}

bench_result tpm_statistic::compute_bench_result(
    uint32_t num_term,
    const std::string &node_name
) {
  bench_result res;
  if (uint64_t(to_milliseconds(duration)) != 0 && num_tx != 0) {
    double tps = double(num_commit) / (to_seconds(duration)) * num_term;
    double ar = double(num_abort) / double(num_tx);

    if (num_commit != 0) {
      double latency = to_milliseconds(commit_duration) / num_commit;

      res.set_latency((float_t) latency);
      if (to_milliseconds(duration_read_dsb) > to_milliseconds(duration_read)) {
        BOOST_ASSERT(false);
      }
      float_t latency_read = to_milliseconds(duration_read) / num_commit;
      float_t latency_read_dsb =
          to_milliseconds(duration_read_dsb) / num_commit;
      float_t latency_append = 0.0;
      float_t latency_replicate = 0.0;
      float_t latency_lock_wait = 0.0;
      float_t latency_part = 0.0;

      if (num_part != 0) {
        latency_append = to_milliseconds(duration_append_log) / num_part;
        latency_replicate = to_milliseconds(duration_replicate_log) / num_part;
        latency_lock_wait = to_milliseconds(duration_lock_wait) / num_part;
        latency_part = to_milliseconds(duration_part) / num_part;
      }

      res.set_latency_read(latency_read);
      res.set_latency_read_dsb(latency_read_dsb);
      res.set_latency_append(latency_append);
      res.set_latency_replicate(latency_replicate);
      res.set_latency_lock_wait(latency_lock_wait);
      res.set_latency_part(latency_part);
      res.set_tpm(tps * 60.0);
      LOG(info) << node_name << " TPS : " << tps << ", ABORT RATE : " << ar
                << ", latency: " << latency << "ms"
                                            // #ifdef TEST_APPEND_TIME
                << ", read: " << latency_read
                << ", read_dsb: " << latency_read_dsb
                << ", lock_wait: " << latency_lock_wait
                << ", part: " << latency_part
                << ", CCB append: " << latency_append
                << ", raft replicate: " << latency_replicate

// #endif
            ;
    } else {
      LOG(info) << node_name << " TPS : " << tps;
    }

    res.set_tpm(tps * 60);
  } else {
    LOG(info) << node_name << " TPS : 0";
  }
  return res;
}

workload::workload(const config &conf)
    : conf_(conf), num_new_order_(conf.get_tpcc_config().num_new_order()),
      oneshot_(true), stopped_(false),
      tuple_gen_(conf.schema_manager().id2table()),
      output_result_(conf.get_tpcc_config().num_output_result()),
      output_windows_size_(conf.get_tpcc_config().num_output_result() / 4) {
  uint32_t num_rg = conf.num_rg();
  uint32_t num_wh = conf.get_tpcc_config().num_warehouse();
  BOOST_ASSERT(num_rg < num_wh);
  BOOST_ASSERT(num_rg > 0);
  BOOST_ASSERT(num_new_order_ > 0);
  uint32_t num_wh_per_rg = num_wh / num_rg;

  for (uint32_t sd_id = 1; sd_id <= num_rg; sd_id++) {
    boundary b;
    b.lower = 1 + num_wh_per_rg * (sd_id - 1);
    b.upper = b.lower + num_wh_per_rg;
    if (b.upper > num_wh) {
      b.upper = num_wh;
    } else {
      b.upper -= 1;
    }
    LOG(info) << "SHARD: " << sd_id << " [" << b.lower << "," << b.upper << ']';
    rg2wid_boundary_.insert(std::make_pair(sd_id, b));
  }
}

result<void> workload::connect_to_lead(per_terminal *t, bool wait_all) {
  if (t->client_conn_) {
    lead_status_request request;
    request.set_source(0);
    request.set_dest(t->node_id_);
    auto r1 = t->client_conn_->send_message(LEAD_STATUS_REQUEST, request);
    if (!r1) {
      if (r1.error().code() == EC::EC_NET_UNCONNECTED) {
        t->client_conn_->connect();
      }
      return r1;
    }
    lead_status_response response;
    auto r2 = t->client_conn_->recv_message(LEAD_STATUS_RESPONSE, response);
    if (!r2) {
      if (r2.error().code() == EC::EC_NET_UNCONNECTED) {
        t->client_conn_->connect();
      }
      return r2;
    }

    if (response.rg_lead() != 0) {
      if (wait_all) {
        if (t->node_id_ == response.rg_lead() &&
            conf_.num_rg() == uint32_t(response.lead().size())) {
          bool leader_ok = true;
          for (auto nid : response.lead()) {
            shard_id_t sid = TO_RG_ID(nid);
            node_id_t lp_node = conf_.get_largest_priority_node_of_shard(sid);
            if (lp_node != 0 && lp_node != nid) {
              leader_ok = false;
            }
          }
          if (leader_ok) {
            return outcome::success();
          } else {
            return outcome::failure(EC::EC_NOT_LEADER);
          }
        }
      } else {
        if (t->node_id_ == response.rg_lead()) {
          return outcome::success();
        } else {
          t->node_id_ = response.rg_lead();
        }
      }
    }
  }
  return outcome::failure(EC::EC_NET_UNCONNECTED);
}

void workload::random_get_replica_client(shard_id_t sd_id, per_terminal *td) {
  per_terminal &t = *td;
  node_id_t node_id = t.node_id_;
  if (node_id == 0) {
    if (t.nodes_id_set_.empty()) {
      t.nodes_id_set_ = conf_.get_rg_block_nodes(sd_id, BLOCK_TYPE_ID_CCB);
    }
    node_id = *t.nodes_id_set_.rbegin();
    t.nodes_id_set_.pop_back();
  }

  BOOST_ASSERT(is_ccb_block(node_id));
  auto i = t.client_set_.find(node_id);
  if (i != t.client_set_.end()) {
    t.client_conn_ = i->second;
    t.node_id_ = node_id;
  } else {
    LOG(error) << " no such node " << id_2_name(node_id);
    BOOST_ASSERT("false");
  }
}

void workload::connect_database(shard_id_t shard_id, uint32_t term_id) {

  per_terminal *td = get_terminal_data(shard_id, term_id);
  td->nodes_id_set_ = conf_.get_rg_block_nodes(shard_id, BLOCK_TYPE_ID_CCB);

  // generate client to block db
  for (node_id_t id : td->nodes_id_set_) {
    node_config cf = conf_.get_node_conf(id);
    ptr<db_client> cli(new db_client(conf_.az_id(), cf));
    td->client_set_[id] = cli;
  }

  std::set<node_id_t> leader_nodes;
  uint64_t second = 2;
  // guarantee all node have the largest priority node becomes leader
  while (true) {
    leader_nodes.clear();
    for (auto id : td->nodes_id_set_) {
      ccb_state_req req;
      req.set_term_id(term_id);
      req.set_shard_id(shard_id);
      ptr<db_client> cli = td->client_set_[id];
      auto result = cli->send_message(CLIENT_CCB_STATE_REQ, req);
      if (!result) {
        if (result.error().code() == EC_NET_UNCONNECTED) {
          cli->connect();
        }
        sleep(second);
        LOG(trace) << "connect to node " << id_2_name(id) <<  "s" << shard_id << "t" << term_id;
        break;
      }
      LOG(trace) << "send ccb state request 1 " << id_2_name(id) <<  "s" << shard_id << "t" << term_id;
      ccb_state_resp resp;
      auto result_recv = cli->recv_message(CLIENT_CCB_STATE_RESP, resp);
      if (!result_recv) {
        sleep(second);
        LOG(trace) << "cannot receive ccb state response " << id_2_name(id) << "s" << shard_id << "t" << term_id;
      }
      if (resp.lead()) {
        LOG(trace) << "shard_ids:" << shard_id << " term_id:" << term_id
                   << " shard leader " << id_2_name(id);
        leader_nodes.insert(id);
      }

      LOG(trace) << "receive ccb state response 1 " << id_2_name(id) <<  "s" << shard_id << "t" << term_id;
    }

    if (not leader_nodes.empty()) {
      LOG(trace) << "connect database ok " << "s" << shard_id << "t" << term_id;
      node_id_t largest_priority_node =
          conf_.get_largest_priority_node_of_shard(shard_id);

      if (largest_priority_node != 0) {
        bool found = false;
        for (auto nid : leader_nodes) {
          if (nid == largest_priority_node) {
            found = true;
          }
        }
        if (found) {
          break;
        } else {
          LOG(trace) << "largest priority node "
                     << id_2_name(largest_priority_node) << "is not leader" << "s" << shard_id << "t" << term_id;
        }
      } else {
        LOG(trace) << "no priority_node " << "s" << shard_id << "t" << term_id;
      }
      break;
    } else {
      for (auto id : leader_nodes) {
        LOG(trace) << "shard_ids:" << shard_id << " term_id:" << term_id
                  << "leader node " << id_2_name(id) ;
      }
      LOG(info) << "shard_ids:" << shard_id << " term_id:" << term_id
                 << " wait leader election";
    }
    sleep(second);
  }

  LOG(trace) << "connect db done " << "s" << shard_id << "t" << term_id;

  // connect to block-db backend
  std::set<uint32_t> connected;
  while (connected.size() != td->client_set_.size()) {
    for (auto kv : td->client_set_) {
      if (connected.count(kv.first) == 0) {
        if (kv.second->connect()) {
          connected.insert(kv.first);
        } else {
          sleep(1);
        }
      }
    }
  }

  // guarantee all backend have the leader status
  for (;;) {
    // break loop when connect to a right leader node...
    node_id_t largest_priority_node =
        conf_.get_largest_priority_node_of_shard(shard_id);
    if (largest_priority_node != 0) {
      td->node_id_ = largest_priority_node;
      random_get_replica_client(shard_id, td);
    } else {
      random_get_replica_client(shard_id, td);
    }
    result<void> r = connect_to_lead(td, true);
    if (r) {
      LOG(trace) << "shard_ids:" << shard_id << " term_id:" << term_id
                 << " would connect leader " << id_2_name(td->node_id_);
      break;
    } else {
      sleep(1);
    }
  }

  for (auto id : td->nodes_id_set_) {
    ccb_state_req req;
    req.set_number(1);
    req.set_term_id(term_id);
    req.set_shard_id(shard_id);
    ptr<db_client> cli = td->client_set_[id];
    auto result = cli->send_message(CLIENT_CCB_STATE_REQ, req);
    if (!result) {
      if (result.error().code() == EC_NET_UNCONNECTED) {
        cli->connect();
      }
      LOG(trace) << "connect to node " << id_2_name(id);
      continue;
    }
    LOG(trace) << "send ccb state request 2 " << id_2_name(id) <<  "s" << shard_id << "t" << term_id;

    ccb_state_resp resp;
    auto result_recv = cli->recv_message(CLIENT_CCB_STATE_RESP, resp);
    if (!result_recv) {
      LOG(trace) << "cannot receive ccb state response " << id_2_name(id);
    }
    LOG(trace) << "receive ccb state response 2 " << id_2_name(id) <<  "s" << shard_id << "t" << term_id;

  }
  BOOST_ASSERT(td->client_conn_);

  LOG(trace) << "connect db return " << "s" << shard_id << "t" << term_id;
}

void workload::load_data(node_id_t node_id) {
  LOG(info) << id_2_name(node_id) << " begin load table ";
  node_config conf = conf_.get_node_conf(node_id);
  db_client client(conf_.az_id(), conf);
  for (;;) {
    bool ok = client.connect();
    if (ok) {
      break;
    }
    boost::this_thread::sleep_for(boost::chrono::seconds(3));
  }

  client_load_data_request request;
  client_load_data_response response;
  request.set_workload("tpcc");
  for (auto shard_id : conf.shard_ids()) {
    auto iter = rg2wid_boundary_.find(shard_id);
    if (iter == rg2wid_boundary_.end()) {
      PANIC("no such replication group");
    }
    boundary b = iter->second;
    request.set_wid_lower(b.lower);
    request.set_wid_upper(b.upper);
    result<void> rs = client.send_message(CLIENT_LOAD_DATA_REQ, request);
    if (!rs) {
      LOG(error) << "send message error";
      return;
    }
    result<void> rr = client.recv_message(CLIENT_LOAD_DATA_RESP, response);
    if (!rr) {
      LOG(error) << "client load data failed ...";
      return;
    }
    if (EC(response.error_code()) != EC::EC_OK) {
      LOG(error) << "client load data failed ...";
      return;
    }
    LOG(info) << id_2_name(node_id) << " finish load table";
  }
}

void workload::warm_up_cached1(shard_id_t sd_id, uint32_t term_id) {
  float_t percent_cached_tuple =
      conf_.get_test_config().percent_cached_tuple();
  std::uniform_real_distribution<> dist(0, 1.0);
  std::random_device rd;
  std::mt19937 e(rd());

  per_terminal *td = get_terminal_data(sd_id, term_id);
  ptr<warm_up_req> req(cs_new<warm_up_req>());

  std::unordered_map<shard_id_t, std::vector<ptr<warm_up_req>>> warm_up_requests;
  for (size_t i = 0; i < td->requests_.size(); i++) {
    const tx_request &t = td->requests_[i];
    for (const tx_operation &op : t.operations()) {
      float_t f = dist(e);
      if (f <= percent_cached_tuple) {
        auto iter = warm_up_requests.find(op.sd_id());
        if (op.op_type() != tx_op_type::TX_OP_INSERT) {
          ptr<warm_up_req> req;
          if (iter == warm_up_requests.end()) {
            req = cs_new<warm_up_req>();
            std::vector<ptr<warm_up_req>> vec;
            vec.push_back(req);
            warm_up_requests.insert(std::make_pair(op.sd_id(), vec));
          } else {
            auto vec = iter->second;
            ptr<warm_up_req> r = *vec.rbegin();
            if (r->tuple_key_size() > 200) {
              req = cs_new<warm_up_req>();
              std::vector<ptr<warm_up_req>> vec;
              vec.push_back(req);
              warm_up_requests.insert(std::make_pair(op.sd_id(), vec));
            } else {
              req = r;
            }
          }

          tuple_key *key = req->add_tuple_key();
          BOOST_ASSERT(op.tuple_row().shard_id() != 0);
          key->set_shard_id(op.tuple_row().shard_id());
          key->set_table_id(op.tuple_row().table_id());
          key->set_tuple_id(op.tuple_row().tuple_id());
        }
      } else {
        LOG(trace) << "un-cache row" << op.tuple_row().table_id()
                   << " key: " << op.tuple_row().tuple_id();
      }
    }
  }
  for (auto p : warm_up_requests) {
    std::scoped_lock<std::mutex> l(warm_up_req_mutex_);
    auto iter = warm_up_req_.find(p.first);
    if (iter == warm_up_req_.end()) {
      warm_up_req_.insert(std::make_pair(p.first, p.second));
    } else {
      iter->second.insert(iter->second.end(), p.second.begin(), p.second.end());
    }
  }
}

void workload::warm_up_cached2(shard_id_t sd_id, uint32_t term_id) {
  BOOST_ASSERT(term_id != 0);
  std::vector<ptr<warm_up_req>> vec;
  {
    std::scoped_lock<std::mutex> l(warm_up_req_mutex_);
    auto iter = warm_up_req_.find(sd_id);
    if (iter == warm_up_req_.end()) {
      return;
    } else {
      vec.swap(iter->second);
      warm_up_req_.erase(sd_id);
    }
  }
  per_terminal *td = get_terminal_data(sd_id, term_id);
  for (auto request : vec) {
    while (true) {
      ptr<db_client> cli = td->client_conn_;
      request->set_term_id(term_id);
      result<void> send_res =
          cli->send_message(CCB_HANDLE_WARM_UP_REQ, *request);
      if (!send_res) {
        td->reset_database_connection();
        sleep(1);
        continue;
      }
      LOG(trace) << "request warm up" << sd_id << " terminal " << term_id << " conn:" << id_2_name(td->node_id_)
        << ", node:" << td->client_conn_;

      warm_up_resp response;
      result<void> recv_res =
          cli->recv_message(CLIENT_HANDLE_WARM_UP_RESP, response);
      if (!recv_res) {
        sleep(1);
        td->reset_database_connection();
        continue;
      }
      LOG(trace) << "response warm up" << sd_id << " terminal " << term_id;
      if (!response.ok()) {
        continue;
      }
      break;
    }
  }
}

void workload::run_new_order(shard_id_t sd_id, uint32_t term_id) {
  per_terminal *td = get_terminal_data(sd_id, term_id);
  BOOST_ASSERT(td->client_conn_);
  uint32_t commit = 0;
  uint32_t abort = 0;
  uint32_t total = 0;
  per_terminal &pt = *td;
  std::vector<tx_request> &requests = pt.requests_;
  time_tracer tracer;
  node_id_t leader_node_id = conf_.get_largest_priority_node_of_shard(sd_id);
  LOG(trace) << "request num:, " << requests.size() << ", term_id:" << term_id;
  std::chrono::nanoseconds duration_append_log(0);
  std::chrono::nanoseconds duration_replicate_log(0);
  std::chrono::nanoseconds duration_read(0);
  std::chrono::nanoseconds duration_read_dsb(0);
  std::chrono::nanoseconds duration_lock_wait(0);
  std::chrono::nanoseconds duration_part(0);
  uint64_t num_part = 0;
  uint32_t num_lock = 0;
  uint32_t num_read_violate = 0;
  uint32_t num_write_violate = 0;
  BOOST_ASSERT(!requests.empty());
  for (tx_request &t : requests) {
    if (stopped_.load()) {
      break;
    }
    BOOST_ASSERT(t.client_request());
    total++;
    ptr<db_client> cli = pt.client_conn_;
    BOOST_ASSERT(cli);
    if (cli->client_ptr()->peer().node_id_ != leader_node_id) {
      LOG(error) << "connect to node "
                 << id_2_name(cli->client_ptr()->peer().node_id_);
    }
    BOOST_ASSERT(t.ByteSizeLong() != 0);
    tracer.begin();

    result<void> send_res = cli->send_message(CLIENT_TX_REQ, t);
    if (!send_res) {
      tracer.end();
      LOG(error) << "send error " << id_2_name(cli->client_ptr()->peer().node_id_) << " term_id, " << term_id;
      continue;
    }

    tx_response response;
    result<void> recv_res = cli->recv_message(CLIENT_TX_RESP, response);
    if (!recv_res) {
      tracer.end();
      LOG(error) << "client tx response receive error" << recv_res.error().message();
      continue;
    }
    EC ec = EC(response.error_code());
    if (ec == EC::EC_OK) {
      tracer.end();
      commit++;

      duration_append_log +=
          std::chrono::microseconds(response.latency_append());
      duration_replicate_log +=
          std::chrono::microseconds(response.latency_replicate());;
      duration_read += std::chrono::microseconds(response.latency_read());
      duration_read_dsb +=
          std::chrono::microseconds(response.latency_read_dsb());
      duration_lock_wait +=
          std::chrono::microseconds(response.latency_lock_wait());
      duration_part += std::chrono::microseconds(response.latency_part());
      num_part += response.access_part();
      num_write_violate += response.num_write_violate();
      num_read_violate += response.num_read_violate();
      num_lock += response.num_lock();
    } else {
      tracer.end();
      LOG(trace) << "tx_rm response error code :" << ec;
      if (ec != EC::EC_TX_ABORT) {
        abort++;
      } else {
        abort++;
      }
    }
    if (total >= 10 && !stopped_.load()) {
      pt.update(commit, abort, total, num_part, tracer.duration(),
                duration_append_log, duration_replicate_log, duration_read,
                duration_read_dsb, duration_lock_wait, duration_part, num_lock,
                num_read_violate, num_write_violate);
      total = 0;
      abort = 0;
      commit = 0;
      num_part = 0;
      duration_lock_wait = duration_part = duration_read = duration_read_dsb =
      duration_replicate_log = duration_append_log =
          std::chrono::nanoseconds(0);
      tracer.reset();
    }
  }
  LOG(info) << "terminal " << term_id << " done";
  if (!stopped_.load()) {
    LOG(warning) << "terminal done before stopped";
    stopped_.store(true);
  }

  pt.done_.store(true);
}

per_terminal *workload::get_terminal_data(shard_id_t sd_id, uint32_t term_id) {
  std::scoped_lock l(terminal_data_mutex_);
  auto i = terminal_data_.find(term_id);
  if (i == terminal_data_.end()) {
    ptr<per_terminal> td(new per_terminal(sd_id, term_id));
    terminal_data_.insert(std::make_pair(term_id, td));
    return td.get();
  } else {
    return i->second.get();
  }
}

void workload::new_order(
    shard_id_t sd_id,
    per_terminal *td,
    id_generator & gen,
    std::default_random_engine &rng) {
  const tpcc_config &c = conf_.get_tpcc_config();

  bool is_dist = false;
  // begin transaction request
  make_begin_tx_request(td, false);

  rg_wid rg_and_wid = gen.gen_local_wid();
  BOOST_ASSERT(rg_and_wid.rg_id_ == sd_id);
  uint32_t wid = rg_and_wid.wid_;
  uint32_t cid = gen.cid_gen_.generate();
  uint32_t did = gen.did_gen_.generate();
  uint32_t oid = gen.oid_gen_.generate();
  uint32_t num_item = gen.oiid_gen_.generate();
  uint32_t w_key = wid;
  uint32_t d_key = make_district_key(wid, did, c.num_warehouse());
  uint32_t c_key = make_customer_key(wid, did, cid, c.num_warehouse(),
                                     c.num_district_per_warehouse());
  uint32_t o_key = make_order_key(wid, did, oid, c.num_warehouse(),
                                  c.num_district_per_warehouse());

  /**
    EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
    INTO :c_discount, :c_last, :c_credit, :w_tax
    FROM customer, warehouse
    WHERE w_id = :w_id AND c_w_id = w_id AND
    c_d_id = :d_id AND c_id = :c_id;
  **/
  make_read_operation(sd_id, TPCC_WAREHOUSE, w_key, td);
  make_read_operation(sd_id, TPCC_CUSTOMER, c_key, td);

  /**
    EXEC SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
    FROM district
    WHERE d_id = :d_id AND d_w_id = :w_id;

    EXEC SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
    WHERE d_id = :d_id AND d_w_id = :w_id;
        **/
  make_read_for_write_operation(sd_id, TPCC_DISTRICT, d_key, td);
  tuple_pb tuple_dist = tuple_gen_.gen_tuple(TPCC_DISTRICT);
  make_update_operation(sd_id, TPCC_DISTRICT, d_key, tuple_dist, td);

  /**
    EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id,
    o_entry_d, o_ol_cnt, o_all_local)
    VALUES (:o_id, :d_id, :w_id, :c_id,
    :datetime, :o_ol_cnt, :o_all_local);

    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
    VALUES (:o_id, :d_id, :w_id);
        **/
  tuple_pb tuple_order = tuple_gen_.gen_tuple(TPCC_ORDER);
  make_insert_operation(sd_id, TPCC_ORDER, o_key, tuple_order, td);
  tuple_pb tuple_new_order = tuple_gen_.gen_tuple(TPCC_NEW_ORDER);
  float_t non_exist = conf_.get_tpcc_config().percent_non_exist_item() *
      float_t(PERCENT_BASE);
  bool access_non_exsit =
      gen.non_exist_gen_.generate() <= uint32_t(non_exist);
  float_t distributed =
      conf_.get_tpcc_config().percent_remote() * float_t(PERCENT_BASE);
  bool remote_warehouse = gen.distributed_gen_.generate() <= distributed;
  float_t hot_item =
      conf_.get_tpcc_config().percent_hot_row() * float_t(PERCENT_BASE);
  bool access_hot_item = gen.hot_row_gen_.generate() <= uint32_t(hot_item);

  make_insert_operation(sd_id, TPCC_NEW_ORDER, o_key, tuple_new_order, td);

  struct __id {
    uint32_t w_id_;
    uint32_t rg_id_;
    uint32_t i_id_;
  };
  std::vector<__id> vec_ids;
  for (uint32_t item_index = 0; item_index < num_item; item_index++) {
    uint32_t iid = 0;

    if (access_non_exsit && item_index == num_item / 2) {
      iid = 0;
      access_non_exsit = false;
    } else {
      iid = gen.iid_gen_.generate();
      if (access_hot_item && conf_.get_tpcc_config().hot_item_num() > 0) {
        iid = iid % conf_.get_tpcc_config().hot_item_num() + 1;
        access_hot_item = false;
      }
    }
    uint32_t ol_supply_wid = wid;
    uint32_t ol_supply_rg_id = sd_id;
    if (remote_warehouse) {
      rg_wid rw = gen.gen_remote_wid(wid, item_index == 0);
      ol_supply_wid = rw.wid_;
      ol_supply_rg_id = rw.rg_id_;
    }
    __id id;
    id.rg_id_ = ol_supply_rg_id;
    id.w_id_ = ol_supply_wid;
    id.i_id_ = iid;
    vec_ids.push_back(id);
  }

  std::shuffle(std::begin(vec_ids), std::end(vec_ids), rng);

  for (size_t i = 0; i < vec_ids.size(); i++) {
    __id id = vec_ids[i];
    uint32_t item_index = i;
    uint32_t ol_supply_wid = id.w_id_;
    uint32_t ol_supply_rg_id = id.rg_id_;
    uint32_t iid = id.i_id_;
    if (ol_supply_rg_id != sd_id) {
      is_dist = on_same_node(ol_supply_rg_id, sd_id);
    }
    uint32_t s_key = make_stock_key(ol_supply_wid, iid, c.num_warehouse());
    LOG(trace) << "shard_ids:" << ol_supply_rg_id
               << " supply_wid:" << ol_supply_wid << ", iid:" << iid
               << ", sotck_key:" << s_key;
    uint32_t ol_key = make_order_line_key(
        ol_supply_wid, did, iid, item_index + 1, c.num_warehouse(),
        c.num_district_per_warehouse(), NUM_ORDER_MAX);
    /**
     EXEC SQL SELECT i_price, i_name , i_data
     INTO :i_price, :i_name, :i_data
     FROM item
     WHERE i_id := ol_i_id;
                **/

    make_read_operation(ol_supply_rg_id, TPCC_ITEM, iid, td);

    /**
      EXEC SQL SELECT s_quantity, s_data,
      s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05
      s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
      INTO :s_quantity, :s_data,
      :s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05
      :s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
      FROM stock
      WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
     **/

    /**
      EXEC SQL UPDATE stock SET s_quantity = :s_quantity
      WHERE s_i_id = :ol_i_id
      AND s_w_id = :ol_supply_w_id;
     **/

    tuple_pb tuple_stock = tuple_gen_.gen_tuple(TPCC_STOCK);
    make_read_for_write_operation(ol_supply_rg_id, TPCC_STOCK, s_key, td);
    make_update_operation(ol_supply_rg_id, TPCC_STOCK, s_key, tuple_stock,
                          td);

    /**
      EXEC SQL INSERT
      INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,
      ol_i_id, ol_supply_w_id,
      ol_quantity, ol_amount, ol_dist_info)
      VALUES (:o_id, :d_id, :w_id, :ol_number,
      :ol_i_id, :ol_supply_w_id,
      :ol_quantity, :ol_amount, :ol_dist_info);
    **/
    tuple_pb tuple_ol_item = tuple_gen_.gen_tuple(TPCC_ORDER_LINE);
    make_insert_operation(ol_supply_rg_id, TPCC_ORDER_LINE, ol_key,
                          tuple_ol_item, td);
  }

  // commit transaction request
  make_end_tx_request(td);

  uint32_t id = 0;
  for (tx_operation &op : *td->requests_.rbegin()->mutable_operations()) {
    op.set_operation_id(++id);
  }
  mutable_request(td).set_distributed(is_dist);
  if (is_dist) {
    td->num_dist_ ++;
  }
}

bool workload::on_same_node(shard_id_t shard_id, shard_id_t remote_shard_id) {
  auto i1 = conf_.rlb_shards().find(shard_id);
  auto i2 = conf_.rlb_shards().find(remote_shard_id);
  if (i1 == conf_.rlb_shards().end() || i2 == conf_.rlb_shards().end()) {
    PANIC("workload find no this shard");
    return false;
  }
  return (i1->second != i2->second);
}

void workload::after_gen_procedure() {
  uint32_t num_dist = 0;
  uint32_t total = 0;
  for (auto i = terminal_data_.begin(); i != terminal_data_.end(); i++) {
    num_dist += i->second->num_dist_;
    total += i->second->requests_.size();
  }
  LOG(info) << "distributed tx rate " << double(num_dist) / double(total) * 100.0;
}

void workload::gen_procedure(shard_id_t sd_id, uint32_t terminal_id) {
  BOOST_ASSERT(terminal_id != 0);
  // terminal_id started from 1
  uint32_t start_ord_id =
      (terminal_id - 1) * (num_new_order_ / conf_.final_num_terminal());
  uint32_t end_ord_id = terminal_id * (num_new_order_ / conf_.final_num_terminal());
  per_terminal *td = get_terminal_data(sd_id, terminal_id);
  std::default_random_engine rng(td->terminal_id_);

  if (td->terminal_id_ == 0) {
    return;
  }
  td->rg_id_ = sd_id;

  id_generator gen(conf_.get_tpcc_config(), rg2wid_boundary_, sd_id);

  bool is_readonly_terminal = terminal_id > conf_.num_terminal();
  for (uint32_t new_order_index = start_ord_id; new_order_index < end_ord_id;
       new_order_index++) {
    build_procedure(sd_id, td, gen, rng, is_readonly_terminal);
  }

  std::shuffle(std::begin(td->requests_), std::end(td->requests_), rng);
  /*
  for (auto i = td->requests_.begin(); i != td->requests_.end(); ++i) {
    tx_request &req = *i;
    LOG(info) <<"term" << td->terminal_id_ << ":, tx req:" << req.DebugString();
  }
   */
}

void workload::build_procedure(
  shard_id_t sd_id,
  per_terminal *td,
  id_generator & gen,
  std::default_random_engine &rng,
  bool read_only_terminal
  ) {
  bool is_read_only = read_only_terminal;
  if (!conf_.get_tpcc_config().additional_read_only_terminal()) {
    if (!is_read_only) {
      float_t percent_read_only = conf_.get_tpcc_config().percent_read_only();
      is_read_only = percent_read_only > DBL_EPSILON
          && gen.read_only_gen_.generate() <= percent_read_only * float_t(PERCENT_BASE);
    }
  }

  if (is_read_only) {
    read_only(sd_id, td, gen, rng);
  } else {
    new_order(sd_id, td, gen, rng);
  }
}

void workload::read_only(
    shard_id_t sd_id,
    per_terminal *td,
    id_generator & gen,
    std::default_random_engine &
    ) {
  /**
   * EXEC SELECT from order_line WHERE id XXXX
   **/
  auto c = conf_.get_tpcc_config();
  // begin transaction request
  make_begin_tx_request(td, true);
  rg_wid rg_wid = gen.gen_local_wid();
  uint32_t wid = rg_wid.wid_;
  shard_id_t sid = rg_wid.rg_id_;
  if (sid != sd_id) {
    PANIC("generate read-only tx, error shard id ")
  }
  BOOST_ASSERT(sid == sd_id);
  uint32_t num_rows = conf_.get_tpcc_config().num_read_only_rows();
  uint32_t n = 0;
  for (; n < num_rows;) {
    uint32_t did = gen.did_gen_.generate();
    uint32_t iid = gen.iid_gen_.generate();
    uint32_t num_item = gen.oiid_gen_.generate();
    for (uint32_t i = 0; n < num_rows && i < num_item; i++, n++) {
      uint32_t ol_key = make_order_line_key(
          wid, did, iid, i + 1, c.num_warehouse(),
          c.num_district_per_warehouse(), NUM_ORDER_MAX);
      make_read_operation(sid, TPCC_ORDER_LINE, ol_key, td);
    }
  }
  // commit transaction request
  make_end_tx_request(td);
}

void workload::make_read_for_write_operation(shard_id_t sd_id, table_id_t table,
                                             uint64_t key, per_terminal *td) {
  tx_operation *op = mutable_request(td).add_operations();
  if (op == nullptr) {
    return;
  }
  BOOST_ASSERT(sd_id != 0);
  op->set_sd_id(sd_id);
  op->set_op_type(TX_OP_READ_FOR_WRITE);
  op->set_operation_id(mutable_request(td).operations_size());
  op->mutable_tuple_row()->set_table_id(table);
  op->mutable_tuple_row()->set_tuple_id(uint64_to_key(key));
  op->mutable_tuple_row()->set_shard_id(sd_id);
}

void workload::make_read_operation(shard_id_t sd_id, table_id_t table,
                                   uint64_t key, per_terminal *td) {
  tx_operation *op = mutable_request(td).add_operations();
  if (op == nullptr) {
    return;
  }
  BOOST_ASSERT(sd_id != 0);
  op->set_sd_id(sd_id);
  op->set_op_type(TX_OP_READ);
  op->set_operation_id(mutable_request(td).operations_size());
  op->mutable_tuple_row()->set_table_id(table);
  op->mutable_tuple_row()->set_tuple_id(uint64_to_key(key));
  op->mutable_tuple_row()->set_shard_id(sd_id);
}

void workload::make_update_operation(shard_id_t sd_id, table_id_t table,
                                     uint64_t key, tuple_pb &tuple,
                                     per_terminal *td) {
  tx_operation *op = mutable_request(td).add_operations();
  if (op == nullptr) {
    return;
  }
  BOOST_ASSERT(sd_id != 0);
  op->set_sd_id(sd_id);
  op->set_op_type(TX_OP_UPDATE);
  op->mutable_tuple_row()->set_table_id(table);
  op->mutable_tuple_row()->set_tuple_id(uint64_to_key(key));
  *op->mutable_tuple_row()->mutable_tuple() = (tuple);
  op->mutable_tuple_row()->set_shard_id(sd_id);
}

void workload::make_insert_operation(shard_id_t sd_id, table_id_t table,
                                     uint64_t key, tuple_pb &tuple,
                                     per_terminal *td) {
  tx_operation *op = mutable_request(td).add_operations();
  if (op == nullptr) {
    return;
  }
  BOOST_ASSERT(sd_id != 0);
  op->set_sd_id(sd_id);
  op->set_op_type(TX_OP_INSERT);
  op->mutable_tuple_row()->set_table_id(table);
  op->mutable_tuple_row()->set_tuple_id(uint64_to_key(key));
  *op->mutable_tuple_row()->mutable_tuple() = tuple;
  op->mutable_tuple_row()->set_shard_id(sd_id);
  BOOST_ASSERT(!is_tuple_nil(op->tuple_row().tuple()));
}

void workload::make_begin_tx_request(per_terminal *td, bool read_only) {
  create_tx_request(td, read_only);
}

void workload::make_end_tx_request(per_terminal *) {
  if (oneshot_) {
  } else {
  }
}

tx_request &workload::mutable_request(per_terminal *td) {
  if (td->requests_.empty()) {
    create_tx_request(td, false);
  }
  return *td->requests_.rbegin();
}

std::vector<tx_request> &workload::get_tx_request(per_terminal *td) {
  return td->requests_;
}

bool workload::is_stopped() {
  return stopped_.load();
}

void workload::set_stopped() {
  stopped_.store(true);
}

tpm_statistic workload::total_stat() {
  tpm_statistic total_result;
  std::pair<uint64_t, uint64_t> range = this->choose_window_min_deviation();
  if (!(range.first < range.second)) {
    PANIC("fatal error output final result");
  }
  BOOST_ASSERT(range.first < range.second);
  BOOST_ASSERT(range.first < result_.size() && range.second <= result_.size());
  for (uint32_t i = range.first; i < range.second; ++i) {
    // remove the first ten percent and the last ten percent
    total_result.add(result_[i]);
  }
  return total_result;
}

void workload::create_tx_request(per_terminal *td, bool read_only) {
  tx_request req;
  req.set_oneshot(oneshot_);
  req.set_client_request(true);
  req.set_read_only(read_only);
  req.set_terminal_id(td->terminal_id_);
  td->requests_.emplace_back(req);
}

bool workload::output_result(std::chrono::nanoseconds duration) {
  tpm_statistic total_result;
  size_t num_term_done = 0;
  for (auto i = terminal_data_.begin(); i != terminal_data_.end(); i++) {
    per_terminal &pt = *i->second;
    tpm_statistic r = pt.get_result();

    r.duration = duration;
    total_result.add(r);
    if (pt.done_) {
      num_term_done++;
    }

  }

  if (num_term_done == terminal_data_.size()) {
    return true;
  }
  if (result_.size() >= output_result_ * 1.2) {
    if (!stopped_.load()) {
      LOG(info) << "stop run bench workload";
      stopped_.store(true);
    }
    return false;
  }

  auto result = total_result.compute_bench_result(conf_.final_num_terminal(), conf_.node_name());
  float_t tps = result.tps();
  result_.push_back(total_result);
  moving_average(tps);

  return false;
}

void workload::moving_average(float_t tps) {
  BOOST_ASSERT(output_windows_size_ > 2);
  if (output_windows_size_ > tps_.size()) {
    tps_.push_back(tps);
    if (tps_.size() == output_windows_size_) {
      float_t sum = 0;
      float_t expected_value = 0;
      float_t deviation_value = 0;
      float_t sum_square = 0;
      for (float_t v : tps_) {
        sum += v;
      }
      expected_value = sum / tps_.size();
      for (float_t v : tps_) {
        float_t abs = std::abs(expected_value - v);
        sum_square += abs * abs;
      }
      deviation_value = sum_square / tps_.size();
      tps_expected_.resize(tps_.size(), 0);
      tps_deviation_.resize(tps_.size(), 0);
      tps_expected_[tps_.size() - 1] = expected_value;
      tps_deviation_[tps_.size() - 1] = deviation_value;
    }
  } else {
    BOOST_ASSERT(output_windows_size_ <= tps_.size());
    uint64_t n = output_windows_size_;
    uint64_t i = tps_.size() - n;
    tps_.push_back(tps);
    // E_{i+1} = E_i + (X_{i+n} - X_i) / n
    // D_{i+1} = D_i +  \frac{(E_{i+1} - E_i)} {n} [(X_{i+n} + X_i)(n - 1) - 2 n
    // E_i + 2 X_i]
    BOOST_ASSERT(tps_expected_.size() == i + n &&
        tps_deviation_.size() == i + n && tps_.size() == i + n + 1);
    n = n - 1;
    uint64_t e_ni1 = tps_expected_[i + n] + (tps_[i + n] - tps_[i]) / n;
    tps_expected_.push_back(e_ni1);
    uint64_t d_ni1 = tps_deviation_[i + n] +
        ((e_ni1 - tps_expected_[n + i]) / n) *
            ((tps_[i + n] + tps_[i]) * (n - 1) -
                2 * n * tps_expected_[i + n] + 2 * tps_[i]);
    tps_deviation_.push_back(d_ni1);
  }
}

std::pair<uint64_t, uint64_t> workload::choose_window_min_deviation() {
  if (output_windows_size_ >= tps_expected_.size() + 1) {
    LOG(warning) << "output result less than output window size";
    return std::make_pair(0, result_.size());
  }
  BOOST_ASIO_ASSERT(tps_expected_.size() > 1);
  std::vector<std::pair<uint64_t, float_t>> index_tps_vec;
  for (size_t i = output_windows_size_ - 1; i < tps_expected_.size(); i++) {
    index_tps_vec.push_back(std::make_pair(i, tps_expected_[i]));
  }

  std::sort(
      index_tps_vec.begin(), index_tps_vec.end(),
      [](std::pair<uint64_t, float_t> &x, std::pair<uint64_t, float_t> &y) {
        return x.second < y.second;
      });

  std::pair<uint64_t, float_t> mid = index_tps_vec[index_tps_vec.size() / 2];
  float_t mid_value = mid.second;
  float_t min_deviation = std::numeric_limits<float_t>::max();
  uint64_t index = tps_expected_[tps_expected_.size() - 1];
  for (uint64_t i = output_windows_size_ - 1; i < tps_deviation_.size(); i++) {
    if (tps_deviation_[i] < min_deviation && tps_expected_[i] > mid_value) {
      min_deviation = tps_deviation_[i];
      index = i;
    }
  }
  if (index + 1 < output_windows_size_ || index + 1 > tps_deviation_.size()) {
    LOG(warning) << "not fit window size";
    return std::make_pair(0, result_.size());
  }
  return std::make_pair(index + 1 - output_windows_size_, index + 1);
}

