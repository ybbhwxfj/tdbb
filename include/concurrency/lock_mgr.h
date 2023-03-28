#pragma once
#include "common/callback.h"
#include "common/define.h"
#include "common/hash_table.h"
#include "common/ptr.hpp"
#include "common/tuple.h"
#include "common/tx_wait.h"
#include "concurrency/deadlock.h"
#include "concurrency/lock.h"
#include "concurrency/lock_mgr_trait.h"
#include "concurrency/lock_pred.h"
#include "concurrency/lock_slot.h"
#include "concurrency/tx.h"
#include "concurrency/violate.h"
#include "proto/proto.h"
#include <atomic>
#include <boost/icl/interval_map.hpp>
#include <functional>
#include <map>
#include <mutex>
#include <unordered_map>

struct tx_conflict {
  tx_conflict(ptr<tx_rm> txn, oid_t oid) : txn_(txn), oid_(oid) {}

  ptr<tx_rm> txn_;
  oid_t oid_;
  std::set<tuple_id_t> write_;
  ptr<lock_slot> slot_;

  bool operator<(const tx_conflict &c) const {
    if (txn_->xid()==c.txn_->xid()) {
      return oid_ < c.oid_;
    } else {
      return txn_->xid() < c.txn_->xid();
    }
  }

  bool operator==(const tx_conflict &c) const {
    return txn_->xid()==c.txn_->xid() && oid_==c.oid_;
  }
};

typedef std::map<xid_t, tx_conflict> tx_conflict_set;
typedef boost::icl::interval_map<tuple_id_t, tx_conflict_set> predicate_map;

class lock_mgr : public lock_mgr_trait {
private:
  typedef concurrent_hash_table<tuple_id_t, ptr<lock_slot>> lock_table_t;
  table_id_t table_id_;
  shard_id_t shard_id_;
  deadlock *dl_;

  fn_schedule_before fn_before_;
  fn_schedule_after fn_after_;
  lock_table_t key_row_locks_;
  boost::asio::io_context::strand strand_;

public:
  lock_mgr(table_id_t table_id, shard_id_t, boost::asio::io_context &context, deadlock *dl,
           fn_schedule_before fn_before, fn_schedule_after fn_after);

  ~lock_mgr();

  virtual bool conflict(xid_t xid, oid_t oid, const predicate &pred);

  boost::asio::io_context::strand &get_strand() { return strand_; }
  virtual void async_wait_lock(fn_wait_lock fn);

  void lock(xid_t xid, oid_t oid, lock_mode lt, predicate key, ptr<tx_rm> txn);

  void unlock(uint64_t xid, lock_mode mode, predicate key);

  void debug_lock(std::ostream &os);

  void debug_dependency(tx_wait_set &dep);

private:
  void lock_gut(

      oid_t oid, lock_mode lt, predicate key, ptr<tx_rm> txn);

  void unlock_gut(uint64_t xid, lock_mode mode, predicate key);

  void row_lock(oid_t oid, lock_mode lt, tuple_id_t key, const ptr<tx_rm> &tx);

  std::pair<ptr<lock_slot>, bool> get_lock_slot(tuple_id_t key);

  std::pair<ptr<lock_slot>, bool> find_slot(tuple_id_t key);

  bool find_conflict(xid_t xid, oid_t oid, ptr<tx_rm> txn,
                     std::set<tuple_id_t> tuple_id, const predicate &pred);
};
