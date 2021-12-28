#include "portal/workload.h"
#include "common/make_key.h"
#include "common/table_id.h"
#include "common/tuple.h"
#include "common/bench_result.h"
#include "common/db_type.h"
#include "network/client.h"
#include "network/db_client.h"
#include <boost/date_time.hpp>
#include <boost/log/trivial.hpp>
#include <boost/filesystem.hpp>
#include <iostream>

workload::workload(const config &conf)
    : conf_(conf), num_new_order_(conf.get_tpcc_config().num_new_order()),
      num_term_(conf.num_terminal()), oneshot_(true), stopped_(false) {
  uint32_t num_rg = conf.num_rg();
  uint32_t num_wh = conf.get_tpcc_config().num_warehouse();
  BOOST_ASSERT(num_rg < num_wh);
  BOOST_ASSERT(num_rg > 0);
  BOOST_ASSERT(num_new_order_ > 0);
  BOOST_ASSERT(num_term_ > 0);
  uint32_t num_wh_per_rg = num_wh / num_rg;

  for (uint32_t rg = 1; rg <= num_rg; rg++) {
    boundary b;
    b.lower = 1 + num_wh_per_rg * (rg - 1);
    b.upper = b.lower + num_wh_per_rg;
    if (b.upper > num_wh) {
      b.upper = num_wh;
    } else {
      b.upper -= 1;
    }
    rg2wid_boundary_.insert(std::make_pair(rg, b));
  }
}

result<void> workload::connect_to_lead(per_terminal *t, bool wait_all) {
  if (t->client_conn_ != nullptr) {
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
          for (auto nid: response.lead()) {
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

void workload::random_get_replica_client(shard_id_t rg_id,
                                         per_terminal *td) {
  per_terminal &t = *td;
  node_id_t node_id = t.node_id_;
  if (node_id == 0) {
    if (t.nodes_id_set_.empty()) {
      t.nodes_id_set_ = conf_.get_rg_block_nodes(rg_id, BLOCK_TYPE_ID_CCB);
    }
    node_id = *t.nodes_id_set_.rbegin();
    t.nodes_id_set_.pop_back();
  }

  BOOST_ASSERT(is_ccb_block(node_id));
  auto i = t.client_set_.find(node_id);
  if (i != t.client_set_.end()) {
    t.client_conn_ = i->second.get();
    t.node_id_ = node_id;
  } else {
    BOOST_LOG_TRIVIAL(error) << " no such node " << id_2_name(node_id);
    t.client_conn_ = nullptr;
    BOOST_ASSERT("false");
  }
}

void workload::connect_database(shard_id_t rg_id, uint32_t term_id) {
  ptr<db_client> cli_panel(new db_client(conf_.panel_config()));
  per_terminal *td = get_terminal_data(rg_id, term_id);
  td->client_conn_ = nullptr;
  td->nodes_id_set_ = conf_.get_rg_block_nodes(rg_id, BLOCK_TYPE_ID_CCB);

  // generate client to block db
  for (node_id_t id: td->nodes_id_set_) {
    node_config cf = conf_.get_node_conf(id);
    ptr<db_client> cli(new db_client(cf));
    td->client_set_[id] = cli;
  }

  // connect to panel block and
  // guarantee all node have the largest priority node becomes leader
  while (true) {
    panel_info_request req;
    req.set_block_type(pb_block_type::PB_BLOCK_CLIENT);
    req.set_source(conf_.node_id());
    req.set_dest(conf_.panel_config().node_id());
    auto rs = cli_panel->send_message(PANEL_INFO_REQ, req);
    if (not rs) {
      if (rs.error().code() == EC_NET_UNCONNECTED) {
        cli_panel->connect();
      }
      sleep(1);
      continue;
    }
    panel_info_response res;
    auto rr = cli_panel->recv_message(PANEL_INFO_RESP_TO_CLIENT, res);
    if (not rs) {
      sleep(1);
      continue;
    }
    if (uint32_t(res.ccb_leader().size()) == conf_.num_rg() &&
        uint32_t(res.dsb_leader().size()) == conf_.num_rg() &&
        uint32_t(res.rlb_leader().size()) == conf_.num_rg()
        ) {

      node_id_t largest_priority_node = conf_.get_largest_priority_node_of_shard(rg_id);

      if (largest_priority_node != 0) {
        bool found = false;
        for (auto nid: res.ccb_leader()) {
          if (nid == largest_priority_node) {
            found = true;
          }
        }
        if (found) {
          break;
        }
      } else {
        break;
      }
    }
    sleep(1);
  }

  // connect to block-db backend
  std::set<uint32_t> connected;
  while (connected.size() != td->client_set_.size()) {
    for (auto kv: td->client_set_) {
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
    node_id_t largest_priority_node = conf_.get_largest_priority_node_of_shard(rg_id);
    if (largest_priority_node != 0) {
      td->node_id_ = largest_priority_node;
      random_get_replica_client(rg_id, td);
    } else {
      random_get_replica_client(rg_id, td);
    }
    result<void> r = connect_to_lead(td, true);
    if (r) {
      break;
    } else {
      sleep(1);
    }
  }
}

void workload::load_data(node_id_t node_id) {
  node_config conf = conf_.get_node_conf(node_id);
  db_client client(conf);
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
  auto iter = rg2wid_boundary_.find(conf.rg_id());
  if (iter == rg2wid_boundary_.end()) {
    BOOST_ASSERT(false);
    return;
  }
  boundary b = iter->second;
  request.set_wid_lower(b.lower);
  request.set_wid_upper(b.upper);
  result<void> rs = client.send_message(CLIENT_LOAD_DATA_REQ, request);
  if (!rs) {
    BOOST_LOG_TRIVIAL(error) << "send message error";
    return;
  }
  result<void> rr = client.recv_message(CLIENT_LOAD_DATA_RESP, response);
  if (!rr) {
    BOOST_LOG_TRIVIAL(error) << "client load data failed ...";
    return;
  }
  if (EC(response.error_code()) != EC::EC_OK) {
    BOOST_LOG_TRIVIAL(error) << "client load data failed ...";
    return;
  }
  BOOST_LOG_TRIVIAL(info) << "load table done ";
}

void workload::run_new_order(shard_id_t rg_id, uint32_t term_id) {
  per_terminal *td = get_terminal_data(rg_id, term_id);
  uint32_t commit = 0;
  uint32_t abort = 0;
  uint32_t total = 0;
  per_terminal &pt = *td;
  std::vector<tx_request> &requests = pt.requests_;
  for (tx_request &t: requests) {
    if (stopped_) {
      break;
    }
    BOOST_ASSERT(t.client_request());
    total++;
    if (pt.client_conn_ == nullptr) {
      random_get_replica_client(rg_id, td);
      auto r = connect_to_lead(td, false);
      if (not r) {
        BOOST_LOG_TRIVIAL(error) << "connect to lead error" << id_2_name(td->node_id_);
      }
    }
    db_client *cli = pt.client_conn_;
    if (cli == nullptr) {
      continue;
    }
    BOOST_ASSERT(t.ByteSizeLong() != 0);
    result<void> send_res = cli->send_message(CLIENT_TX_REQ, t);
    if (!send_res) {
      pt.reset_database_connection();
      continue;
    }

    tx_response response;
    result<void> recv_res = cli->recv_message(CLIENT_TX_RESP, response);
    if (!recv_res) {
      pt.reset_database_connection();
      continue;
    }
    EC ec = EC(response.error_code());
    if (ec == EC::EC_OK) {
      commit++;
    } else {
      BOOST_LOG_TRIVIAL(trace) << "tx response error code :" << ec;
      if (ec != EC::EC_TX_ABORT) {
        abort++;
      } else {
        abort++;
      }
    }
    if (total >= 10) {
      pt.update(commit, abort, total);
      total = 0;
      abort = 0;
      commit = 0;
    }
  }
  pt.done_.store(true);
}

workload::per_terminal *workload::get_terminal_data(shard_id_t rg_id, uint32_t term_id) {
  std::scoped_lock l(terminal_data_mutex_);
  auto i = terminal_data_.find(term_id);
  if (i == terminal_data_.end()) {
    ptr<per_terminal> td(new per_terminal(rg_id, term_id));
    terminal_data_.insert(std::make_pair(term_id, td));
    return td.get();
  } else {
    return i->second.get();
  }

}

void workload::gen_new_order(shard_id_t rg_id, uint32_t term_id) {
  per_terminal *td = get_terminal_data(rg_id, term_id);
  const tpcc_config &c = conf_.get_tpcc_config();
  if (td->terminal_id_ == 0 || td->terminal_id_ - 1 >= conf_.num_terminal()) {
    return;
  }
  td->rg_id_ = rg_id;
  id_generator gen(conf_.get_tpcc_config(), rg2wid_boundary_, rg_id);

  for (uint32_t new_order_index = 0; new_order_index < num_new_order_;
       new_order_index++) {
    // begin transaction request
    make_begin_tx_request(td);

    rg_wid rg_and_wid = gen.gen_local_wid();
    BOOST_ASSERT(rg_and_wid.rg_id_ == rg_id);
    uint32_t wid = rg_and_wid.wid_;
    uint32_t cid = gen.cid_gen_.generate();
    uint32_t did = gen.did_gen_.generate();
    uint32_t oid = gen.oid_gen_.generate();
    uint32_t num_item = gen.oiid_gen_.generate();
    uint32_t w_key = wid;
    uint32_t d_key = make_district_key(wid, did, c.num_warehouse());
    uint32_t c_key =
        make_customer_key(wid, did, cid, c.num_warehouse(), c.num_district());
    uint32_t o_key = make_order_key(wid, oid, c.num_district());

    /**
      EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
      INTO :c_discount, :c_last, :c_credit, :w_tax
      FROM customer, warehouse
      WHERE w_id = :w_id AND c_w_id = w_id AND
      c_d_id = :d_id AND c_id = :c_id;
    **/
    make_read_operation(rg_id, TPCC_WAREHOUSE, w_key, td);
    make_read_operation(rg_id, TPCC_CUSTOMER, c_key, td);

    /**
      EXEC SQL SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
      FROM district
      WHERE d_id = :d_id AND d_w_id = :w_id;

      EXEC SQL UPDATE district SET d_next_o_id = :d_next_o_id + 1
      WHERE d_id = :d_id AND d_w_id = :w_id;
          **/
    make_read_for_write_operation(rg_id, TPCC_DISTRICT, d_key, td);
    tuple_pb tuple_dist; // = "district";
    make_update_operation(rg_id, TPCC_DISTRICT, d_key, tuple_dist, td);

    /**
      EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id,
      o_entry_d, o_ol_cnt, o_all_local)
      VALUES (:o_id, :d_id, :w_id, :c_id,
      :datetime, :o_ol_cnt, :o_all_local);

      EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
      VALUES (:o_id, :d_id, :w_id);
          **/
    tuple_pb tuple_order; // = "order";
    make_insert_operation(rg_id, TPCC_ORDER, o_key, tuple_order, td);
    tuple_pb tuple_new_order; // = "new order";
    float_t non_exist = conf_.get_tpcc_config().percent_non_exist_item() *
        float_t(PERCENT_BASE);
    float_t distributed =
        conf_.get_tpcc_config().percent_distributed() * float_t(PERCENT_BASE);
    bool access_non_exsit =
        gen.non_exist_gen_.generate() <= uint32_t(non_exist);
    bool distributed_tx = gen.distributed_gen_.generate() <= distributed;
    mutable_request(td).set_distributed(distributed_tx);
    make_insert_operation(rg_id, TPCC_NEW_ORDER, o_key,
                          tuple_new_order, td);
    for (uint32_t item_index = 0; item_index < num_item; item_index++) {
      uint32_t iid = 0;
      if (access_non_exsit && item_index == num_item / 2) {
        iid = 0;
      } else {
        iid = gen.iid_gen_.generate();
      }
      uint32_t ol_supply_wid = wid;
      uint32_t ol_supply_rg_id = rg_id;
      if (distributed_tx && not is_shared()) {
        rg_wid rw = gen.gen_remote_wid();
        ol_supply_wid = rw.wid_;
        ol_supply_rg_id = rw.rg_id_;
      }
      uint32_t s_key = make_stock_key(ol_supply_wid, iid, c.num_warehouse());
      //BOOST_LOG_TRIVIAL(trace) << "rg_id:" << ol_supply_rg_id << " supply_wid:" << ol_supply_wid << ", iid:" << iid << ", sotck_key:" << s_key;
      uint32_t ol_key =
          make_order_line_key(ol_supply_wid, iid, item_index + 1,
                              c.num_warehouse(), c.num_max_order_line());
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

      tuple_pb tuple_stock; //TODO = "stock";
      make_read_for_write_operation(ol_supply_rg_id, TPCC_STOCK,
                                    s_key, td);
      make_update_operation(ol_supply_rg_id, TPCC_STOCK, s_key,
                            tuple_stock, td);

      /**
        EXEC SQL INSERT
        INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,
        ol_i_id, ol_supply_w_id,
        ol_quantity, ol_amount, ol_dist_info)
        VALUES (:o_id, :d_id, :w_id, :ol_number,
        :ol_i_id, :ol_supply_w_id,
        :ol_quantity, :ol_amount, :ol_dist_info);
      **/
      tuple_pb tuple_ol_item; // = "online item";
      make_insert_operation(ol_supply_rg_id, TPCC_ORDER_LINE, ol_key,
                            tuple_ol_item, td);
    }

    // commit transaction request
    make_end_tx_request(td);

    uint32_t id = 0;
    for (tx_operation &op: *td->requests_.rbegin()->mutable_operations()) {
      op.set_operation_id(++id);
    }
  }

  std::default_random_engine rng(td->terminal_id_);
  std::shuffle(std::begin(td->requests_), std::end(td->requests_), rng);
}

void workload::make_read_for_write_operation(shard_id_t rg_id,
                                             table_id_t table, uint32_t key, per_terminal *td) {
  tx_operation *op = mutable_request(td).add_operations();
  if (op == nullptr) {
    return;
  }
  op->set_rg_id(rg_id);
  op->set_op_type(TX_OP_READ_FOR_WRITE);
  op->set_operation_id(mutable_request(td).operations_size());
  op->mutable_tuple_row()->set_table_id(table);
  op->mutable_tuple_row()->set_tuple_id(uint64_to_key(key));
}

void workload::make_read_operation(shard_id_t rg_id,
                                   table_id_t table, uint32_t key, per_terminal *td) {
  tx_operation *op = mutable_request(td).add_operations();
  if (op == nullptr) {
    return;
  }
  op->set_rg_id(rg_id);
  op->set_op_type(TX_OP_READ);
  op->set_operation_id(mutable_request(td).operations_size());
  op->mutable_tuple_row()->set_table_id(table);
  op->mutable_tuple_row()->set_tuple_id(uint64_to_key(key));
}

void workload::make_update_operation(shard_id_t rg_id,
                                     table_id_t table, uint32_t key,
                                     tuple_pb &tuple,
                                     per_terminal *td) {
  tx_operation *op = mutable_request(td).add_operations();
  if (op == nullptr) {
    return;
  }
  op->set_rg_id(rg_id);
  op->set_op_type(TX_OP_UPDATE);
  op->mutable_tuple_row()->set_table_id(table);
  op->mutable_tuple_row()->set_tuple_id(uint64_to_key(key));
  *op->mutable_tuple_row()->mutable_tuple() = (tuple);
}

void workload::make_insert_operation(shard_id_t rg_id,
                                     table_id_t table, uint32_t key,
                                     tuple_pb &tuple, per_terminal *td) {
  tx_operation *op = mutable_request(td).add_operations();
  if (op == nullptr) {
    return;
  }
  op->set_rg_id(rg_id);
  op->set_op_type(TX_OP_INSERT);
  op->mutable_tuple_row()->set_table_id(table);
  op->mutable_tuple_row()->set_tuple_id(uint64_to_key(key));
  *op->mutable_tuple_row()->mutable_tuple() = tuple;
}

void workload::make_begin_tx_request(per_terminal *td) {
  create_tx_request(td);
}

void workload::make_end_tx_request(per_terminal *) {

  if (oneshot_) {

  } else {
  }
}

tx_request &workload::mutable_request(per_terminal *td) {
  if (td->requests_.empty()) {
    create_tx_request(td);
  }
  return *td->requests_.rbegin();

}

std::vector<tx_request> &workload::get_tx_request(per_terminal *td) {
  return td->requests_;
}

void workload::output_final_result() {
  tpm_statistic r;
  uint32_t num_term = terminal_data_.size();
  for (uint32_t i = 0; i != result_.size(); ++i) {
    r.add(result_[i]);
  }

  bench_result res;
  if (r.duration.total_milliseconds() != 0 && r.num_tx != 0) {
    double tps = double(r.num_commit) /
        (double(r.duration.total_microseconds()) / 1000000.0) *
        num_term;
    double ar = double(r.num_abort) / double(r.num_tx);
    BOOST_LOG_TRIVIAL(info) << "total TPS : " << tps << ", ABORT RATE : " << ar;

    res.set_abort(ar);
    res.set_tpm(tps * 60);
  }
  boost::json::object j = res.to_json();
  std::stringstream ssm;
  ssm << j;
  std::string json_str = ssm.str();
  boost::filesystem::path file_path(conf_.db_path());
  file_path.append("result.json");
  std::ofstream f(file_path.c_str());
  f << json_str << std::endl;
}

void workload::per_terminal::reset_database_connection() {
  node_id_ = 0;
  client_conn_ = nullptr;
}

void workload::per_terminal::update(uint32_t commit, uint32_t abort, uint32_t total) {
  std::scoped_lock l(mutex_);
  result_.num_commit += commit;
  result_.num_abort += abort;
  result_.num_tx += total;
}

workload::tpm_statistic workload::per_terminal::get_result() {
  std::scoped_lock l(mutex_);
  auto r = result_;
  result_.reset();
  return r;
}
void workload::create_tx_request(per_terminal *td) {
  td->requests_.emplace_back(tx_request());
  mutable_request(td).set_oneshot(oneshot_);
  mutable_request(td).set_client_request(true);
}

void workload::output_result() {
  while (true) {
    ptime start = microsec_clock::local_time();
    boost::this_thread::sleep_for(boost::chrono::milliseconds(1000));
    tpm_statistic total_result;
    size_t num_term_done = 0;
    ptime end = microsec_clock::local_time();

    for (auto i = terminal_data_.begin(); i != terminal_data_.end(); i++) {
      per_terminal &pt = *i->second;
      tpm_statistic r = pt.get_result();
      time_duration duration = end - start;
      r.duration = duration;
      total_result.add(r);
      num_term_done += pt.done_ ? 1 : 0;
    }
    if (num_term_done == terminal_data_.size()) {
      break;
    }
    if (result_.size() >= conf_.get_tpcc_config().num_output_result()) {
      stopped_.store(true);
      continue;
    }
    double tps = double(total_result.num_commit) /
        (double(total_result.duration.total_microseconds()) / 1000000.0) *
        terminal_data_.size();
    double ar = double(total_result.num_abort) / double(total_result.num_tx);
    BOOST_LOG_TRIVIAL(info) << "TPS : " << tps << ", ABORT RATE : " << ar;

    result_.push_back(total_result);
  }
  output_final_result();
}