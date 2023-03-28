#pragma once

#include "common/callback.h"
#include "common/db_type.h"
#include "common/enum_str.h"
#include "common/id.h"
#include "common/lock_mode.h"
#include "common/ptr.hpp"
#include "common/tuple.h"
#include "common/tx_wait.h"
#include "concurrency/lock.h"
#include "concurrency/lock_mgr_trait.h"
#include "concurrency/tx.h"
#include "concurrency/violate.h"
#include "proto/proto.h"
#include <deque>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <stdint.h>

class tx_context;
class lock_mgr;

struct xid_oid_t {
  xid_t xid_;
  oid_t oid_;

  xid_oid_t(xid_t xid,
            oid_t oid) : xid_(xid), oid_(oid) {

  }
};
class lock_slot : public std::enable_shared_from_this<lock_slot> {
private:
  lock_mgr *mgr_;
  table_id_t table_id_;
  shard_id_t shard_id_;
  tuple_id_t tuple_id_;
  fn_schedule_before fn_before_;
  fn_schedule_after fn_after_;
  uint32_t read_count_;
  uint32_t write_count_;

  std::unordered_set<xid_t> read_;
  std::unordered_set<xid_t> write_;
  std::deque<xid_oid_t> wait_;
  std::unordered_map<xid_t, ptr<tx_lock_ctx>> info_;
#ifdef TEST_TRACE_LOCK
  std::stringstream trace_;
#endif
  std::mutex mutex_;
public:
  lock_slot(lock_mgr *mgr, table_id_t table_id, shard_id_t shard_id, tuple_id_t tuple_id,
            fn_schedule_before fn_before, fn_schedule_after fn_after);

  ~lock_slot();

  bool lock(lock_mode type, const ptr<tx_rm> &, oid_t oid);

  void unlock(xid_t);

  void debug_lock(std::ostream &os);

  void build_dependency(tx_wait_set &ds);

  tuple_id_t tuple_id() const { return tuple_id_; }

  bool predicate_conflict(xid_t xid, oid_t oid, ptr<tx_rm> txn);

private:

  void assert_check();

  std::pair<ptr<tx_lock_ctx>, bool> add_lock_info(lock_mode type, const ptr<tx_rm> &, oid_t oid);

  // make xid's locks are violable...
  void tx_build_dependency(ptr<tx_wait> w);

  void make_read(xid_t xid);

  bool read_lock(ptr<tx_lock_ctx> info, oid_t oid);

  bool write_lock(ptr<tx_lock_ctx> info, oid_t oid);

  std::pair<bool, bool> acquire_read_lock(ptr<tx_lock_ctx> info, oid_t oid);

  void unlock_gut(xid_t xid);

  bool remove_lock(xid_t xid);

  void add_wait(ptr<tx_lock_ctx> ctx, oid_t);

  void add_read(ptr<tx_lock_ctx> ctx, oid_t);

  void add_write(ptr<tx_lock_ctx> ctx, oid_t);

  void notify_lock_acquire();

  void async_add_dependency(const ptr<tx_rm> &ctx);

  result<void> tx_wait_for(ptr<tx_wait> ws);

  void on_lock_acquired(EC ec, lock_mode mode, ptr<tx_rm> txn, oid_t oid);
};
