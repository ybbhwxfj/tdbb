#pragma once
#include "common/db_type.h"
#include "common/tuple.h"
#include "common/tx_wait.h"
#include "proto/proto.h"
#include "common/id.h"
#include "common/ptr.hpp"
#include "common/enum_str.h"
#include "concurrency/tx.h"
#include "common/lock_mode.h"
#include "common/callback.h"
#include <map>
#include <set>
#include <stdint.h>
#include <functional>
#include <deque>
#include "concurrency/lock_mgr_trait.h"

class tx_context;

class lock_slot : public std::enable_shared_from_this<lock_slot> {
private:
  lock_mgr_trait *mgr_;
  table_id_t table_id_;
  tuple_id_t tuple_id_;
  fn_schedule_before fn_before_;
  fn_schedule_after fn_after_;
  uint32_t read_count_;
  uint32_t write_count_;
  uint32_t read_violate_count_;
  uint32_t write_violate_count_;
  std::vector<xid_t> read_;
  std::vector<xid_t> write_;
  std::deque<xid_t> wait_;
  std::map<xid_t, tx_info> info_;
  // must be recursive
  // notify_acquire could call lock in the same thread
  std::recursive_mutex mutex_;
public:
  lock_slot(
      lock_mgr_trait *mgr,
      table_id_t table_id,
      tuple_id_t tuple_id,
      fn_schedule_before fn_before,
      fn_schedule_after fn_after
  );

  ~lock_slot();
  bool lock(lock_mode type, const ptr<tx> &, oid_t oid);
  void unlock(xid_t);
  void make_violable(lock_mode type, xid_t);
  void debug_lock(std::ostream &os);
  void build_dependency(tx_wait_set &ds);
  void assert_check();

  std::shared_ptr<lock_slot> get_ptr() {
    return shared_from_this();
  }

  bool empty() {
    std::scoped_lock l(mutex_);
    return info_.empty();
  }

  bool predicate_conflict(xid_t xid, oid_t oid, ptr<tx> txn);
private:
  // make xid's locks are violable...

  void make_read(xid_t xid);
  bool read_lock(const ptr<tx> &ctx, oid_t oid, lock_mode mode);
  bool write_lock(const ptr<tx> &ctx, oid_t oid);
  std::pair<bool, bool> acquire_read_lock(const ptr<tx> &ctx, oid_t oid, lock_mode mode);
  void unlock_gut(xid_t xid);
  void make_violable(xid_t xid);

  bool remove_lock(xid_t xid);
  bool add_wait(const ptr<tx> &ctx, oid_t oid, lock_mode type);
  bool add_read(const ptr<tx> &ctx, oid_t oid, lock_mode mode);
  bool add_write(const ptr<tx> &ctx, oid_t oid);
  void notify_lock_acquire();

  void async_add_dependency(const ptr<tx> &ctx);
  result<void> wait_timeout(xid_t xid, tx_wait_set &ws);
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
  void dependency_write_read(std::vector<ptr<tx_context>> &in);
  static void dlv_acquire(EC ec, const ptr<tx> &ctx, oid_t oid, const ptr<std::vector<ptr<tx_context>>> &in);
#endif
  void on_lock_acquired(EC ec, lock_mode mode, ptr<tx> txn, oid_t oid);
};
