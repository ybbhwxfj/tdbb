#pragma once

#include "common/db_type.h"

#ifdef DB_TYPE_CALVIN

#include "common/id.h"
#include "common/ptr.hpp"
#include "common/tuple.h"
#include "common/tx_log.h"
#include "concurrency/access_mgr.h"
#include "concurrency/calvin_epoch_ops.h"
#include "concurrency/tx.h"
#include "network/net_service.h"
#include "proto/proto.h"
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

typedef std::function<void(EC)> fn_lock_callback;

class calvin_context;

class calvin_scheduler;

typedef std::function<ptr<calvin_context>(const tx_request &)>
    fn_calvin_context_find;
typedef std::function<void(xid_t)> fn_calvin_context_remove;

class calvin_context : public tx_rm,
                       public std::enable_shared_from_this<calvin_context> {
  friend calvin_scheduler;

private:
  xid_t xid_;
  node_id_t node_id_;
  std::optional<node_id_t> ctx_opt_dsb_node_id_;
  std::unordered_map<shard_id_t, node_id_t> dsb_node_ids_;
  node_id_t collector_id_;
  uint64_t cno_;
  ptr<tx_request> ops_request_;
  net_service *service_;
  access_mgr *access_;
  bool log_committed_;
  tx_response response_;

  std::map<uint32_t, ptr<tx_operation>> op_response_;
  std::set<uint32_t> op_read_;
  ptr<std::atomic_ulong> num_ops_;
  std::unordered_map<oid_t, fn_lock_callback> callback_;
  std::recursive_mutex mutex_;
  std::stringstream trace_message_;
  uint64_t num_lock_ok_;
  uint64_t num_read_resp_;
  fn_calvin_context_remove fn_remove_;
  bool read_only_;
  bool commit_;
  bool timeout_invoked_;
  uint64_t start_ms_;
  ptr<timer> timer_ticker_;
public:
  calvin_context(
      boost::asio::io_context::strand s, xid_t xid,
      node_id_t node_id,
      std::optional<node_id_t> dsb_node_id,
      const std::unordered_map<shard_id_t, node_id_t> &shard2node,
      uint64_t cno,
      ptr<tx_request> ops, net_service *service, access_mgr *access,
      fn_calvin_context_remove fn_remove);

  virtual ~calvin_context() {}

  void begin();

  void set_read_only() { read_only_ = true; }

  void async_lock_acquire(EC ec, oid_t oid) override;

  void set_epoch_num_ops(ptr<std::atomic_ulong> p) { num_ops_ = p; }

  bool on_operation_done(const tx_operation &op, const tuple_pb &tp);

  bool on_operation_committed(const tx_log_proto &op);

  bool tx_commit();

  void add_lock_acquire_callback(oid_t oid, fn_lock_callback fn);

  void debug_tx(std::ostream &os) const;

  void read_response(const dsb_read_response &res);

private:
  void send_read(const tx_operation &op);
  node_id_t shard2node(shard_id_t shard_id);
};

#endif // DB_TYPE_CALVIN