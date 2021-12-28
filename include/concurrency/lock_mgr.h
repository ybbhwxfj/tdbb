#pragma once

#include "common/callback.h"
#include "common/hash_table.h"
#include "common/tx_wait.h"
#include "proto/proto.h"
#include "common/ptr.hpp"
#include "concurrency/lock.h"
#include "concurrency/tx.h"
#include "concurrency/deadlock.h"
#include "concurrency/lock_slot.h"
#include "common/tuple.h"
#include "concurrency/lock_pred.h"
#include "concurrency/lock_mgr_trait.h"
#include <functional>
#include <atomic>
#include <boost/icl/interval_map.hpp>
#include <map>
#include <mutex>
#include <unordered_map>
#include <boost/icl/interval_map.hpp>

struct tx_conflict {
  tx_conflict(ptr<tx> txn, oid_t oid) : txn_(txn), oid_(oid) {}
  ptr<tx> txn_;
  oid_t oid_;
  std::set<tuple_id_t> write_;
  ptr<lock_slot> slot_;

  bool operator<(const tx_conflict &c) const {
    if (txn_->xid() == c.txn_->xid()) {
      return oid_ < c.oid_;
    } else {
      return txn_->xid() < c.txn_->xid();
    }
  }

  bool operator==(const tx_conflict &c) const {
    return txn_->xid() == c.txn_->xid() && oid_ == c.oid_;
  }
};

typedef std::map<xid_t, tx_conflict> tx_conflict_set;
typedef boost::icl::interval_map<tuple_id_t, tx_conflict_set> predicate_map;

class lock_mgr : public lock_mgr_trait {
private:
  class key_hash {
  public:
    key_hash() {}
    size_t operator()(tuple_id_t i) const {
      std::hash<tuple_id_t> h;
      return h(i);
    }
  };

  class key_equal {
  public:
    key_equal() {}
    bool operator()(tuple_id_t x, tuple_id_t y) const {
      return x == y;
    }
  };

  typedef hash_table<tuple_id_t, ptr<lock_slot>, key_hash, key_equal> lock_table_t;

  table_id_t table_id_;
  ptr<deadlock> dl_;
  predicate_map predicate_;
  std::unordered_map<tuple_id_t, ptr<lock_slot>> write_key_;
  fn_schedule_before fn_before_;
  fn_schedule_after fn_after_;
  std::recursive_mutex mutex_;
  lock_table_t key_row_locks_;

public:
  lock_mgr(
      table_id_t table_id,
      ptr<deadlock> dl,
      fn_schedule_before fn_before,
      fn_schedule_after fn_after
  );
  ~lock_mgr();

  virtual bool conflict(xid_t xid,
                        oid_t oid,
                        const predicate &pred
  );
  virtual void async_wait_lock(fn_wait_lock fn);
  void lock(
      xid_t xid,
      oid_t oid,
      lock_mode lt,
      const predicate &key,
      const ptr<tx> &txn);

  void unlock(uint64_t xid, lock_mode mode, const predicate &key);

  void make_violable(lock_mode lt, uint64_t xid, tuple_id_t key);

  void debug_lock(std::ostream &os);
  void debug_dependency(tx_wait_set &dep);
private:
  void row_lock(
      xid_t xid,
      oid_t oid,
      lock_mode lt,
      tuple_id_t key,
      const ptr<tx> &tx);
  ptr<lock_slot> get_lock_slot(
      tuple_id_t key
  );
  std::pair<ptr<lock_slot>, bool> find_slot(tuple_id_t key);

  bool find_conflict(xid_t xid,
                     oid_t oid,
                     ptr<tx> txn,
                     std::set<tuple_id_t> tuple_id,
                     const predicate &pred
  );
};
