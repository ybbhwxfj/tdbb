#pragma once

#include "common/id.h"
#include "common/tuple.h"
#include "common/bench_result.h"
#include "common/uniform_generator.hpp"
#include "common/config.h"
#include "network/client.h"
#include "proto/proto.h"
#include "network/net_service.h"
#include "network/db_client.h"
#include <random>
#include <vector>
#include <map>
#include <atomic>
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

    std::chrono::nanoseconds duration;
    std::chrono::nanoseconds commit_duration;
    std::chrono::nanoseconds duration_append_log;
    std::chrono::nanoseconds duration_replicate_log;
    std::chrono::nanoseconds duration_read;
    std::chrono::nanoseconds duration_read_dsb;
    std::chrono::nanoseconds duration_lock_wait;
    std::chrono::nanoseconds duration_part;
    uint32_t num_tx;
    uint32_t num_commit;
    uint32_t num_abort;
    uint32_t num_part;
    uint32_t num_lock;
    uint32_t num_read_violate;
    uint32_t num_write_violate;

    void reset() {
      duration_part
          = duration_lock_wait
          = duration_replicate_log
          = duration_append_log
          = duration_read
          = duration_read_dsb
          = commit_duration
          = duration = std::chrono::nanoseconds(0);

      num_part
          = num_tx
          = num_commit
          = num_abort
          = num_lock
          = num_read_violate
          = num_write_violate =
          0;
    }

    void add(const tpm_statistic &r) {
      duration += r.duration;
      num_commit += r.num_commit;
      num_abort += r.num_abort;
      num_tx += r.num_tx;
      commit_duration += r.commit_duration;
      duration_append_log += r.duration_append_log;
      duration_replicate_log += r.duration_replicate_log;
      duration_read += r.duration_read;
      duration_read_dsb += r.duration_read_dsb;
      duration_lock_wait += r.duration_lock_wait;
      duration_part += r.duration_part;
      num_part += r.num_part;
      num_lock += r.num_lock;
      num_read_violate += r.num_read_violate;
      num_write_violate += r.num_write_violate;
    }

    bench_result compute_bench_result(uint32_t num_term);
  };

  const static uint32_t PERCENT_BASE = 10000;

  struct id_generator {
    id_generator(const tpcc_config &conf, const map<uint32_t, boundary> &rg2b, uint32_t rg) :
        rg_id_(rg),
        cid_gen_(1, conf.num_customer_per_district()),
        did_gen_(1, conf.num_district_per_warehouse()),
        oid_gen_(conf.num_order_initialize_per_district(),
                 conf.num_order_initialize_per_district() + conf.num_new_order()),
        oiid_gen_(float(conf.num_max_order_line()) * 1 / 3, conf.num_max_order_line()),
        iid_gen_(1, conf.num_item()),
        non_exist_gen_(1, PERCENT_BASE),
        distributed_gen_(1, PERCENT_BASE),
        hot_row_gen_(1, PERCENT_BASE),
        rg_gen_(1, (uint32_t) rg2b.size()) {
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
    uniform_generator<uint32_t> hot_row_gen_;
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

    void update(uint32_t commit, uint32_t abort, uint32_t total,
                uint32_t num_part,
                std::chrono::nanoseconds commit_duration,
                std::chrono::nanoseconds duration_append_log,
                std::chrono::nanoseconds duration_replicate_log,
                std::chrono::nanoseconds duration_read,
                std::chrono::nanoseconds duration_read_dsb,
                std::chrono::nanoseconds duration_lock_wait,
                std::chrono::nanoseconds duration_part,
                uint32_t num_lock,
                uint32_t num_read_violate,
                uint32_t num_write_violate
    );

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

  uint32_t prefix_non_zero_tps_ = 0;
  uint32_t prefix_zero_tps_ = 0;
  bool enable_calculate_ = false;
 public:
  workload(const config &conf);

  void output_result();

  void gen_new_order(
      shard_id_t rg_id,
      uint32_t terminal_id);

  void warm_up_cached(shard_id_t rg_id, uint32_t term_id);

  void run_new_order(shard_id_t rg_id, uint32_t term_id);

  void load_data(node_id_t node_id);

  void connect_database(shard_id_t rg_id, uint32_t term_id);

  void output_final_result();

 private:
  per_terminal *get_terminal_data(shard_id_t rg_id, uint32_t term_id);

  void make_read_operation(shard_id_t rg_id, table_id_t table, uint64_t key, per_terminal *td);

  void make_update_operation(shard_id_t rg_id, table_id_t table, uint64_t key, tuple_pb &tuple, per_terminal *td);

  void make_insert_operation(shard_id_t rg_id, table_id_t table, uint64_t key, tuple_pb &tuple, per_terminal *td);

  void make_read_for_write_operation(shard_id_t rg_id, table_id_t table, uint64_t key, per_terminal *td);

  uint32_t wid2rg(uint32_t);

  void make_begin_tx_request(per_terminal *td);

  void make_end_tx_request(per_terminal *td);

  result<void> connect_to_lead(per_terminal *td, bool wait_all);

  void random_get_replica_client(shard_id_t rg_id, per_terminal *td);

  tx_request &mutable_request(per_terminal *td);

  void create_tx_request(per_terminal *td);

  std::vector<tx_request> &get_tx_request(per_terminal *td);
};