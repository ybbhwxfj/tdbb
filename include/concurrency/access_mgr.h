#pragma once

#include "common/callback.h"
#include "common/hash_table.h"
#include "common/ptr.hpp"
#include "concurrency/lock.h"
#include "concurrency/lock_mgr.h"
#include "concurrency/data_mgr.h"
#include "common/tuple.h"
#include "proto/proto.h"
#include <functional>
#include <atomic>
#include <boost/icl/interval_map.hpp>
#include <map>
#include <mutex>
#include <unordered_map>
#include "common/callback.h"

class access_mgr {
private:
  typedef hash_table<uint64_t, ptr<lock_mgr>, std::hash<uint64_t>,
                     std::equal_to<uint64_t>> lock_table_t;
  typedef hash_table<uint64_t, ptr<data_mgr>, std::hash<uint64_t>,
                     std::equal_to<uint64_t>> data_table_t;
  ptr<deadlock> dl_;
  lock_table_t lock_table_;
  data_table_t data_table_;
  fn_schedule_before fn_before_;
  fn_schedule_after fn_after_;
public:
  access_mgr(
      ptr<deadlock> dl,
      fn_schedule_before fn_before,
      fn_schedule_after fn_after
  );

  void lock_row(
      xid_t xid,
      oid_t op_id,
      lock_mode lt,
      uint32_t table_id,
      const predicate &key,
      const ptr<tx> &tx);

  void unlock(
      xid_t xid,
      lock_mode lt,
      uint32_t table_id,
      const predicate &pred);

  void make_violable(
      xid_t xid,
      lock_mode lt,
      uint32_t table_id,
      tuple_id_t key);

  std::pair<tuple_pb, bool> get(uint32_t table_id, tuple_id_t key);

  void put(uint32_t table_id, tuple_id_t key,
           const tuple_pb &data);

  void debug_lock(std::ostream &os);
  void debug_dependency(tx_wait_set &dep);
};
