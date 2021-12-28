#pragma once

#include "common/id.h"
#include "common/tuple.h"
#include "common/uniform_generator.hpp"
#include <random>
#include <vector>
#include <map>
#include <atomic>
#include "common/config.h"
#include "network/client.h"
#include "proto/proto.h"
#include "network/net_service.h"
#include "network/db_client.h"
#include <boost/date_time.hpp>

using std::vector;
using std::map;
using boost::posix_time::ptime;
using boost::posix_time::microsec_clock;
using boost::posix_time::time_duration;

class workload {
private:

  struct boundary {
    uint32_t lower;
    uint32_t upper;
  };
  struct rg_wid {
    rg_wid() : rg_id_(0), wid_(0) {};
    rg_wid(shard_id_t rg_id, uint32_t wid) : rg_id_(rg_id), wid_(wid) {}
    shard_id_t rg_id_;
    uint32_t wid_;
  };
  struct tpm_statistic {
    tpm_statistic() {
      reset();
    }

    time_duration duration;
    uint32_t num_tx;
    uint32_t num_commit;
    uint32_t num_abort;

    void reset() {
      duration = boost::posix_time::milliseconds(0);
      num_tx = 0;
      num_commit = 0;
      num_abort = 0;
    }

    void add(const tpm_statistic &r) {
      duration += r.duration;
      num_commit += r.num_commit;
      num_abort += r.num_abort;
      num_tx += r.num_tx;
    }
  };

  const static uint32_t PERCENT_BASE = 10000;
  struct id_generator {
    id_generator(const tpcc_config &conf, const map<uint32_t, boundary> &rg2b, uint32_t rg) :
        rg_id_(rg),
        cid_gen_(1, conf.num_customer()),
        did_gen_(1, conf.num_district()),
        oid_gen_(conf.num_order_initalize(), conf.num_order_initalize() + conf.num_new_order()),
        oiid_gen_(float(conf.num_order_item()) * 2 / 3, conf.num_order_item()),
        iid_gen_(1, conf.num_item()),
        non_exist_gen_(1, PERCENT_BASE),
        distributed_gen_(1, PERCENT_BASE),
        rg_gen_(1, (uint32_t)rg2b.size()) {
      for (auto iter = rg2b.begin(); iter != rg2b.end(); ++iter) {
        rg2_wid_gen_[iter->first] = uniform_generator<uint32_t>(iter->second.lower, iter->second.upper);
      }
    }

    rg_wid gen_remote_wid() {
      shard_id_t rg_id = rg_gen_.generate();
      while (rg_id == rg_id_) {
        rg_id = rg_gen_.generate();
      }
      auto iter = rg2_wid_gen_.find(rg_id);
      if (iter == rg2_wid_gen_.end()) {
        BOOST_ASSERT(false);
        return rg_wid();
      }
      return rg_wid(rg_id, iter->second.generate());
    }

    rg_wid gen_local_wid() {
      auto iter = rg2_wid_gen_.find(rg_id_);
      if (iter == rg2_wid_gen_.end()) {
        BOOST_ASSERT(false);
        return rg_wid();
      }
      return rg_wid(rg_id_, iter->second.generate());
    }

    rg_wid gen_wid() {
      shard_id_t rg_id = rg_gen_.generate();
      auto iter = rg2_wid_gen_.find(rg_id);
      if (iter == rg2_wid_gen_.end()) {
        BOOST_ASSERT(false);
        return rg_wid();
      }
      return rg_wid(rg_id, iter->second.generate());
    }

    shard_id_t rg_id_;
    uniform_generator<uint32_t> cid_gen_;
    uniform_generator<uint32_t> did_gen_;
    uniform_generator<uint32_t> oid_gen_;
    uniform_generator<uint32_t> oiid_gen_;
    uniform_generator<uint32_t> iid_gen_;

    uniform_generator<uint32_t> non_exist_gen_;
    uniform_generator<uint32_t> distributed_gen_;
    uniform_generator<uint32_t> rg_gen_;

    std::map<uint32_t, uniform_generator<uint32_t>> rg2_wid_gen_;
  };

  struct per_terminal {
    per_terminal(shard_id_t rid, uint32_t tid) :
        rg_id_(rid),
        terminal_id_(tid),
        node_id_(0),
        client_conn_(nullptr) {

    }
    shard_id_t rg_id_;
    uint32_t terminal_id_;
    node_id_t node_id_;
    db_client *client_conn_;
    std::vector<tx_request> requests_;
    tpm_statistic result_;
    std::map<node_id_t, ptr<db_client>> client_set_;
    std::vector<node_id_t> nodes_id_set_;

    std::mutex mutex_;
    std::atomic<bool> done_;
    void reset_database_connection();
    void update(uint32_t commit, uint32_t abort, uint32_t total);
    tpm_statistic get_result();
  };

  config conf_;
  std::mutex terminal_data_mutex_;
  std::map<uint32_t, ptr<per_terminal>> terminal_data_;
  std::vector<tpm_statistic> result_;
  uint32_t num_new_order_;
  uint32_t num_term_;
  bool oneshot_;
  std::map<shard_id_t, boundary> rg2wid_boundary_;

  std::atomic<bool> stopped_;
public:
  workload(const config &conf);
  void output_result();
  void gen_new_order(shard_id_t rg_id, uint32_t term_id);
  void run_new_order(shard_id_t rg_id, uint32_t term_id);
  void load_data(node_id_t node_id);

  void connect_database(shard_id_t rg_id, uint32_t term_id);
  void output_final_result();
private:
  per_terminal *get_terminal_data(shard_id_t rg_id, uint32_t term_id);
  void make_read_operation(shard_id_t rg_id, table_id_t table, uint32_t key, per_terminal *td);
  void make_update_operation(shard_id_t rg_id, table_id_t table, uint32_t key, tuple_pb &tuple, per_terminal *td);
  void make_insert_operation(shard_id_t rg_id, table_id_t table, uint32_t key, tuple_pb &tuple, per_terminal *td);
  void make_read_for_write_operation(shard_id_t rg_id, table_id_t table, uint32_t key, per_terminal *td);

  uint32_t wid2rg(uint32_t);

  void make_begin_tx_request(per_terminal *td);

  void make_end_tx_request(per_terminal *td);

  result<void> connect_to_lead(per_terminal *td, bool wait_all);

  void random_get_replica_client(shard_id_t rg_id, per_terminal *td);
  tx_request &mutable_request(per_terminal *td);
  void create_tx_request(per_terminal *td);
  std::vector<tx_request> &get_tx_request(per_terminal *td);
};