#pragma once

#include "common/bench_result.h"
#include "common/config.h"
#include "common/id.h"
#include "common/panic.h"
#include "common/tuple.h"
#include "common/tuple_gen.h"
#include "common/uniform_generator.hpp"
#include "network/client.h"
#include "network/db_client.h"
#include "network/net_service.h"
#include "proto/proto.h"
#include <atomic>
#include <boost/date_time.hpp>
#include <boost/format.hpp>
#include <map>
#include <random>
#include <vector>

using boost::posix_time::microsec_clock;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using std::map;
using std::vector;

const static uint32_t PERCENT_BASE = 10000;

struct tpm_statistic {
  tpm_statistic() { reset(); }

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
    duration_part = duration_lock_wait = duration_replicate_log =
        duration_append_log = duration_read = duration_read_dsb =
            commit_duration = duration = std::chrono::nanoseconds(0);

    num_part = num_tx = num_commit = num_abort = num_lock = num_read_violate =
        num_write_violate = 0;
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

  void to_proto(tpm_stat & proto) {
    proto.set_duration(duration.count());
    proto.set_commit_duration(commit_duration.count());
    proto.set_duration_append_log(duration_append_log.count());
    proto.set_duration_part(duration_part.count());
    proto.set_duration_read(duration_read.count());
    proto.set_duration_read_dsb(duration_read_dsb.count());
    proto.set_duration_lock_wait(duration_lock_wait.count());
    proto.set_duration_replicate_log(duration_replicate_log.count());

    proto.set_num_tx(num_tx);
    proto.set_num_commit(num_commit);
    proto.set_num_abort( num_abort);
    proto.set_num_part( num_part);
    proto.set_num_lock( num_lock);
  }

  void from_proto(const tpm_stat &proto) {
    duration = std::chrono::nanoseconds(proto.duration());
    commit_duration = std::chrono::nanoseconds(proto.commit_duration());
    duration_append_log = std::chrono::nanoseconds(proto.duration_append_log());
    duration_replicate_log = std::chrono::nanoseconds(proto.duration_replicate_log());
    duration_read = std::chrono::nanoseconds(proto.duration_read());
    duration_read_dsb = std::chrono::nanoseconds(proto.duration_read_dsb());
    duration_lock_wait = std::chrono::nanoseconds(proto.duration_lock_wait());
    duration_part = std::chrono::nanoseconds(proto.duration_part());
    num_tx = proto.num_tx();
    num_commit = proto.num_commit();
    num_abort = proto.num_abort();
    num_part = proto.num_part();
    num_lock = proto.num_lock();
  }

  bench_result compute_bench_result(uint32_t num_term, const std::string & name);
};

struct per_terminal {
  per_terminal(shard_id_t rid, uint32_t tid)
      : rg_id_(rid), terminal_id_(tid), node_id_(0), num_dist_(0) {}

  shard_id_t rg_id_;
  uint32_t terminal_id_;
  uint32_t num_dist_;
  node_id_t node_id_;
  ptr<db_client> client_conn_;
  std::vector<tx_request> requests_;
  tpm_statistic result_;
  std::map<node_id_t, ptr<db_client>> client_set_;
  std::vector<node_id_t> nodes_id_set_;

  std::mutex mutex_;
  std::atomic<bool> done_;

  void reset_database_connection();

  void update(uint32_t commit, uint32_t abort, uint32_t total,
              uint32_t num_part, std::chrono::nanoseconds commit_duration,
              std::chrono::nanoseconds duration_append_log,
              std::chrono::nanoseconds duration_replicate_log,
              std::chrono::nanoseconds duration_read,
              std::chrono::nanoseconds duration_read_dsb,
              std::chrono::nanoseconds duration_lock_wait,
              std::chrono::nanoseconds duration_part, uint32_t num_lock,
              uint32_t num_read_violate, uint32_t num_write_violate);

  tpm_statistic get_result();
};

struct boundary {
  uint32_t lower;
  uint32_t upper;
};

struct rg_wid {
  rg_wid() : rg_id_(0), wid_(0){};

  rg_wid(shard_id_t sd_id, uint32_t wid) : rg_id_(sd_id), wid_(wid) {}

  shard_id_t rg_id_;
  uint32_t wid_;
};

struct id_generator {
  id_generator(const tpcc_config &conf, const map<uint32_t, boundary> &rg2b,
               uint32_t sd_id)
      : rg_id_(sd_id), wid_gen_(1, conf.num_warehouse()),
        cid_gen_(1, conf.num_customer_per_district()),
        did_gen_(1, conf.num_district_per_warehouse()),
        oid_gen_(conf.num_order_initialize_per_district(),
                 conf.num_order_initialize_per_district() +
                     conf.num_new_order()),
        oiid_gen_(float(conf.num_max_order_line()) * 1 / 3,
                  conf.num_max_order_line()),
        iid_gen_(1, conf.num_item()), non_exist_gen_(1, PERCENT_BASE),
        distributed_gen_(1, PERCENT_BASE), hot_row_gen_(1, PERCENT_BASE),
        rg_gen_(1, (uint32_t)rg2b.size()),
        read_only_gen_(1, PERCENT_BASE),
        control_dist_(conf.control_percent_dist_tx())
        {
    for (auto iter = rg2b.begin(); iter != rg2b.end(); ++iter) {
      rg2_wid_gen_[iter->first] =
          uniform_generator<uint32_t>(iter->second.lower, iter->second.upper);
    }
    for (auto iter = rg2b.begin(); iter != rg2b.end(); ++iter) {
      for (auto i = iter->second.lower; i <= iter->second.upper; i++) {
        wid2rg_.insert(std::make_pair(i, iter->first));
        LOG(trace) << "w_id" << i << ", SHARD:" << iter->first;
      }
    }
  }

  rg_wid gen_remote_wid(uint32_t w_id, bool first) {
    if (!control_dist_) {
      uint32_t remote_w_id = wid_gen_.generate();
      while (remote_w_id == w_id && first) {
        remote_w_id = wid_gen_.generate();
      }
      auto iter = wid2rg_.find(remote_w_id);
      if (iter != wid2rg_.end()) {
        return rg_wid(iter->second, remote_w_id);
      } else {
        PANIC(boost::format("not found warehouse id %d") % w_id);
        return rg_wid();
      }
    } else {
      shard_id_t sd_id = rg_gen_.generate();
      while (sd_id == rg_id_ && first) {
        sd_id = rg_gen_.generate();
      }
      auto iter = rg2_wid_gen_.find(sd_id);
      if (iter == rg2_wid_gen_.end()) {
        PANIC(
            (boost::format("not found replication group id %d") % sd_id).str());
        return rg_wid();
      }
      return rg_wid(sd_id, iter->second.generate());
    }
  }

  rg_wid gen_local_wid() {
    auto iter = rg2_wid_gen_.find(rg_id_);
    if (iter == rg2_wid_gen_.end()) {
      PANIC(boost::format("gen_local_wid:not found replication group id %d") % rg_id_);
      return rg_wid();
    }
    return rg_wid(rg_id_, iter->second.generate());
  }

  rg_wid gen_wid() {
    shard_id_t sd_id = rg_gen_.generate();
    auto iter = rg2_wid_gen_.find(sd_id);
    if (iter == rg2_wid_gen_.end()) {
      BOOST_ASSERT(false);
      return rg_wid();
    }
    return rg_wid(sd_id, iter->second.generate());
  }

  shard_id_t rg_id_;
  uniform_generator<uint32_t> wid_gen_;
  uniform_generator<uint32_t> cid_gen_;
  uniform_generator<uint32_t> did_gen_;
  uniform_generator<uint32_t> oid_gen_;
  uniform_generator<uint32_t> oiid_gen_;
  uniform_generator<uint32_t> iid_gen_;

  uniform_generator<uint32_t> non_exist_gen_;
  uniform_generator<uint32_t> distributed_gen_;
  uniform_generator<uint32_t> hot_row_gen_;
  uniform_generator<uint32_t> rg_gen_;
  uniform_generator<uint32_t> read_only_gen_;
  bool control_dist_;
  std::map<uint32_t, uniform_generator<uint32_t>> rg2_wid_gen_;
  wid2rg_map_t wid2rg_;
};

class workload {
private:
  config conf_;
  std::mutex terminal_data_mutex_;
  std::map<uint32_t, ptr<per_terminal>> terminal_data_;
  uint32_t num_new_order_;
  uint32_t num_term_;
  bool oneshot_;
  std::map<shard_id_t, boundary> rg2wid_boundary_;
  std::atomic<bool> stopped_;
  std::atomic<bool> ended_;
  tuple_gen tuple_gen_;
  uint64_t output_result_;
  uint64_t output_windows_size_;
  std::vector<tpm_statistic> result_;
  std::vector<float_t> tps_;
  std::vector<float_t> tps_expected_;
  std::vector<float_t> tps_deviation_;
  std::mutex warm_up_req_mutex_;
  std::unordered_map<shard_id_t, std::vector<ptr<warm_up_req>>> warm_up_req_;
public:
  workload(const config &conf);

  bool output_result(std::chrono::nanoseconds duration);

  void gen_procedure(shard_id_t sd_id, uint32_t terminal_id);



  void warm_up_cached1(shard_id_t sd_id, uint32_t term_id);
  void warm_up_cached2(shard_id_t sd_id, uint32_t term_id);

  void run_new_order(shard_id_t sd_id, uint32_t term_id);

  void load_data(node_id_t node_id);

  void connect_database(shard_id_t shard_id, uint32_t term_id);

  bool is_stopped();

  void set_stopped();

  tpm_statistic total_stat();
private:
  void build_procedure(
      shard_id_t sd_id,
      per_terminal *td,
      id_generator & gen,
      std::default_random_engine &rng,
      bool read_only_terminal
  );

  void read_only(
      shard_id_t sd_id,
      per_terminal *td,
      id_generator & gen,
      std::default_random_engine &rng
  );

  void new_order(
      shard_id_t sd_id,
      per_terminal *td,
      id_generator & gen,
      std::default_random_engine &rng);

  bool on_same_node(shard_id_t shard_id, shard_id_t remote_shard_id);

  per_terminal *get_terminal_data(shard_id_t sd_id, uint32_t term_id);

  void make_read_operation(shard_id_t sd_id, table_id_t table, uint64_t key,
                           per_terminal *td);

  void make_update_operation(shard_id_t sd_id, table_id_t table, uint64_t key,
                             tuple_pb &tuple, per_terminal *td);

  void make_insert_operation(shard_id_t sd_id, table_id_t table, uint64_t key,
                             tuple_pb &tuple, per_terminal *td);

  void make_read_for_write_operation(shard_id_t sd_id, table_id_t table,
                                     uint64_t key, per_terminal *td);

  void make_begin_tx_request(per_terminal *td, bool read_only);

  void make_end_tx_request(per_terminal *td);

  result<void> connect_to_lead(per_terminal *td, bool wait_all);

  void random_get_replica_client(shard_id_t sd_id, per_terminal *td);

  tx_request &mutable_request(per_terminal *td);

  void create_tx_request(per_terminal *td, bool read_only);

  std::vector<tx_request> &get_tx_request(per_terminal *td);

  void moving_average(float_t tps);

  std::pair<uint64_t, uint64_t> choose_window_min_deviation();
};