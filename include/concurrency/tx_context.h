#pragma once
#include "common/db_type.h"
#include "proto/proto.h"
#include <boost/asio.hpp>
#include <boost/asio/awaitable.hpp>

#ifdef DB_TYPE_NON_DETERMINISTIC

#include "common/callback.h"
#include "common/enum_str.h"
#include "common/id.h"
#include "common/ptr.hpp"
#include "common/time_tracer.h"
#include "common/tuple.h"
#include "concurrency/lock_mgr_global.h"
#include "concurrency/tx.h"
#include "concurrency/deadlock.h"
#include "concurrency/write_ahead_log.h"
#include "access/access_mgr.h"
#include "network/net_service.h"
#include "network/sender.h"
#include "proto/proto.h"
#include <deque>
#include <functional>
#include <mutex>

enum rm_state {
  RM_IDLE,
  RM_COMMITTING,
  RM_ABORTING,
  RM_PREPARE_COMMITTING,
  RM_PREPARE_ABORTING,
  RM_ENDED,
};

typedef std::function<void(EC ec, tuple_pb &tuple)> fn_read_done;
typedef std::function<void(EC ec)> fn_write_done;
typedef std::function<void(uint64_t, rm_state)> fn_tx_state;
typedef std::function<void(EC ec)> fn_lock_acquire;

template<> enum_strings<rm_state>::e2s_t enum_strings<rm_state>::enum2str;

typedef std::pair<table_id_t, tuple_id_t> read_key;
struct hash_key {
  std::size_t operator()(read_key const &s) const noexcept {
    std::size_t h1 = std::hash<table_id_t>{}(s.first);
    std::size_t h2 = std::hash<tuple_id_t>{}(s.first);
    return h1 ^ (h2 << 1); // or use boost::hash_combine
  }
};

class tx_context : public std::enable_shared_from_this<tx_context>,
                   public tx_rm {
private:
  uint64_t cno_;
  node_id_t node_id_;
  std::string node_name_;
  std::optional<node_id_t> ctx_opt_dsb_node_id_;
  std::unordered_map<shard_id_t, node_id_t> shard_id_2_node_id_;
  xid_t xid_;
  bool distributed_;
  node_id_t coord_node_id_;
  oid_t oid_;
  uint32_t max_ops_;
  lock_mgr_global *mgr_;
  access_mgr *access_;
  net_service *service_;
  std::deque<tx_operation> ops_;
  tx_response response_;

  ptr<connection> cli_conn_;
  EC error_code_;
  rm_state state_;
  std::map<uint32_t, fn_ec_tuple> ds_read_handler_;
  fn_lock_acquire lock_acquire_;

  std::map<oid_t, ptr<lock_item>> locks_;
  std::vector<tx_operation> logs_;
  std::vector<tx_log_proto> log_entry_;
  write_ahead_log *wal_;

  std::stringstream trace_message_;
  std::recursive_mutex mutex_;
  uint64_t start_;
  bool has_respond_;
  fn_tx_state fn_tx_state_;
  bool prepare_commit_log_synced_;
  bool commit_log_synced_;
  deadlock *dl_;
  bool victim_;

#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  bool dependency_committed_;

  uint64_t dep_in_count_;
  bool dlv_prepare_;
  bool dlv_commit_;
  std::unordered_map<xid_t, ptr<tx_context>> dep_in_set_;
  std::unordered_map<xid_t, ptr<tx_context>> dep_out_set_;
#endif // DB_TYPE_GEO_REP_OPTIMIZE  
  time_tracer read_time_tracer_;
  time_tracer append_time_tracer_;
  time_tracer lock_wait_time_tracer_;
  time_tracer part_time_tracer_;
  uint64_t log_rep_delay_;
  uint64_t latency_read_dsb_;
  uint32_t num_read_violate_;
  uint32_t num_write_violate_;
  uint32_t num_lock_;
  ptr<timer> timer_tick_;
  bool timeout_invoked_;
  bool read_only_;
public:
  tx_context(boost::asio::io_context::strand s, uint64_t xid, uint32_t node_id,
             std::optional<node_id_t> rlb_node_id,
             const std::unordered_map<shard_id_t, node_id_t> &shard2node,
             uint64_t cno, bool distributed,
             lock_mgr_global *mgr, access_mgr *access, net_service *sender, ptr<connection> conn,
             write_ahead_log *write_ahead_log, fn_tx_state callback,
             deadlock *dl);

  virtual ~tx_context() = default;

  void begin();

  void async_lock_acquire(EC ec, oid_t oid) override;

  bool distributed() const { return distributed_; }

  void notify_lock_acquire(EC ec, const ptr<std::vector<ptr<tx_context>>> &in);

  void process_tx_request(const tx_request &req);

  void async_read(table_id_t table_id, shard_id_t shard_id, tuple_id_t key, bool read_for_write,
                  fn_ec_tuple fn_read_done);

  void async_update(uint32_t table_id, shard_id_t shard_id, tuple_id_t key, tuple_pb &&tuple,
                    fn_ec fn_update_done);

  void async_insert(uint32_t table_id, shard_id_t shard_id, tuple_id_t key, tuple_pb &&tuple,
                    fn_ec fn_write_done);

  void async_remove(uint32_t table_id, shard_id_t shard_id, tuple_id_t key, fn_ec_tuple fn_removed);

  void read_data_from_dsb_response(const ptr<dsb_read_response> resp,
                                   std::chrono::steady_clock::time_point ts);

  void on_committed_log_commit();

  void on_aborted_log_commit();

  void tx_committed();

  void tx_aborted();

  void tx_ended();

  void on_log_entry_commit(tx_cmd_type type,
                           std::chrono::steady_clock::time_point end_ts);

  rm_state state() const;

  void timeout_clean_up();

  void abort(EC ec);

  void debug_tx(std::ostream &os);

  void log_rep_delay(uint64_t us);

private:
  void read_data_from_dsb(uint32_t table_id, shard_id_t shard_id, tuple_id_t key, uint32_t oid,
                          std::function<void(EC, tuple_pb &&)> fn_read_done);

  void co_handle_operation(tx_operation &op);

  void handle_operation(tx_operation &op, const fn_ec op_done);

  void handle_next_operation();

  void invoke_done(fn_ec op_done, EC ec);

  void send_tx_response();

  void abort_tx_1p();

  void release_lock();

  void async_force_log();

  void set_tx_cmd_type(tx_cmd_type type);

  void append_operation(const tx_operation &op);

  void handle_finish_tx_phase1_commit();

  void handle_finish_tx_phase1_abort();

#ifdef DB_TYPE_SHARE_NOTHING
public:
  void on_prepare_committed_log_commit();

  void on_prepare_aborted_log_commit();

  void handle_tx_tm_commit(const tx_tm_commit &msg);

  void handle_tx_tm_abort(const tx_tm_abort &msg);

private:
  void abort_tx_2p();

  void tx_prepare_committed();

  void tx_prepare_aborted();

  void prepare_commit_tx();

  void prepare_abort_tx();

  void handle_finish_tx_phase2_commit();

  void handle_finish_tx_phase2_abort();

  void send_prepare_message(bool commit);

  void send_ack_message(bool commit);

  void handle_finish_tx_phase1_prepare_commit();

  void handle_finish_tx_phase1_prepare_abort();

#ifdef DB_TYPE_GEO_REP_OPTIMIZE
 public:
  void handle_tx_enable_violate();

 private:
  void register_dependency(const ptr<tx_context> &out);

  void report_dependency();

  void dependency_commit();

  void dlv_try_tx_commit();

  void dlv_try_tx_prepare_commit();

  void dlv_abort();

  void dlv_make_violable();

  void send_tx_enable_violate();

#endif // DB_TYPE_GEO_REP_OPTIMIZE
#endif // DB_TYPE_SHARE_NOTHING
  node_id_t shard2node(shard_id_t shard_id);
};

#endif // #ifdef DB_TYPE_NON_DETERMINISTIC