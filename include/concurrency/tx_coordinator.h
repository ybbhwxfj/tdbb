#pragma once

#include "common/db_type.h"
#include "concurrency/tx.h"
#ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_SHARE_NOTHING

#include "common/error_code.h"
#include "common/result.hpp"
#include "common/enum_str.h"
#include "concurrency/write_ahead_log.h"
#include "network/connection.h"
#include "network/net_service.h"
#include "proto/proto.h"
#include <mutex>
#include <unordered_map>
#include <functional>

enum tm_state {
  TM_IDLE,
  TM_COMMITTED,
  TM_ABORTED,
  TM_DONE,
};

enum tm_trace_state {
  TTS_INVALID,
  TTS_HANDLE_TX_REQUEST,
  TTM_STATE_ADVANCE,
  TTS_WAIT_APPEND_LOG,
  TTS_APPEND_LOG_DONE,
};

typedef std::function<void(uint64_t xid, tm_state)> fn_tm_state;

template<>
enum_strings<tm_state>::e2s_t  enum_strings<tm_state>::enum2str;
template<>
enum_strings<tm_trace_state>::e2s_t enum_strings<tm_trace_state>::enum2str;

class tx_coordinator : public std::enable_shared_from_this<tx_coordinator>, public tx_base {
 private:
  enum tx_rm_state {
    RM_IDLE,
    RM_PREPARE_COMMIT,
    RM_PREPARE_ABORT,
    RM_COMMITTED,
    RM_ABORTED,
  };

  struct tx_rm_tracer {
    tx_rm_tracer() :
        lead_(0),
        rm_state_(RM_IDLE) {
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
      violate_ = false;
#endif // DB_TYPE_GEO_REP_OPTIMIZE
    }

    node_id_t lead_;
    tx_rm_state rm_state_;
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
    bool violate_;
#endif // DB_TYPE_GEO_REP_OPTIMIZE
    tx_request message_;
  };

  uint64_t xid_;
  node_id_t node_id_;
  std::unordered_map<shard_id_t, node_id_t> lead_node_;
  net_service *service_;
  ptr<connection> connection_;
  write_ahead_log *wal_;
  std::map<shard_id_t, std::vector<tx_operation>> ops_;
  std::unordered_map<shard_id_t, tx_rm_tracer> rm_tracer_;
  tm_state tm_state_;

  bool write_commit_log_;
  bool write_abort_log_;
  bool write_end_done_{};
  std::recursive_mutex mutex_;
  tx_response response_;
  EC error_code_;
  bool victim_;
  std::string trace_message_;
  std::chrono::steady_clock::time_point start_;
  bool responsed_;
  fn_tm_state fn_tm_state_;

  uint64_t latency_read_;
  uint64_t latency_read_dsb_;
  uint64_t latency_replicate_;
  uint64_t latency_append_;
  uint64_t latency_lock_wait_;
  uint64_t latency_part_;
  uint32_t num_lock_;
  uint32_t num_read_violate_;
  uint32_t num_write_violate_;
 public:
  tx_coordinator(
      boost::asio::io_context::strand s,
      uint64_t xid, uint32_t node_id,
      std::unordered_map<shard_id_t, node_id_t> lead_node,
      net_service *service, ptr<connection> connection, write_ahead_log *write_ahead_log,
      fn_tm_state fn
  );

  uint64_t xid() const { return xid_; }

  result<void> handle_tx_request(const tx_request &req);

  void handle_tx_rm_prepare(const tx_rm_prepare &msg);

  void handle_tx_rm_ack(const tx_rm_ack &msg);

  tm_state state() const { return tm_state_; }

  void debug_tx(std::ostream &os);

  const std::string &trace_message() const { return trace_message_; }

  void timeout_clean_up();

  void abort(EC ec);

  void on_log_entry_commit(tx_cmd_type type);

#ifdef DB_TYPE_GEO_REP_OPTIMIZE

  void handle_tx_enable_violate(const tx_enable_violate &msg);

#endif // DB_TYPE_GEO_REP_OPTIMIZE
 private:
  void send_commit();

  void send_abort();

  void send_end();

  void step_tm_state_advance();

  void write_commit_log();

  void write_abort_log();

  void write_end_log();

  void on_committed();

  void on_aborted();

  void on_ended();

  void send_tx_response();

#ifdef DB_TYPE_GEO_REP_OPTIMIZE

  void send_tx_enable_violate();

#endif // DB_TYPE_GEO_REP_OPTIMIZE
};

#endif // DB_TYPE_SHARE_NOTHING
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC