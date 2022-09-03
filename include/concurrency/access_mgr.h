#pragma once

#include "common/callback.h"
#include "common/hash_table.h"
#include "common/ptr.hpp"
#include "concurrency/violate.h"
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
  typedef hash_table<uint64_t, ptr<lock_mgr>> lock_table_t;
  typedef hash_table<uint64_t, ptr<data_mgr>> data_table_t;
  net_service *service_;
  deadlock *dl_;
  lock_table_t lock_table_;
  data_table_t data_table_;
  fn_schedule_before fn_before_;
  fn_schedule_after fn_after_;
 public:
  access_mgr(
      net_service *service,
      deadlock *dl,
      fn_schedule_before fn_before,
      fn_schedule_after fn_after
  );

  void lock_row(
      xid_t xid,
      oid_t op_id,
      lock_mode lt,
      uint32_t table_id,
      const predicate &key,
      const ptr<tx_rm> &tx);

  void unlock(
      xid_t xid,
      lock_mode lt,
      uint32_t table_id,
      const predicate &pred);

  void make_violable(
      xid_t xid,
      lock_mode lt,
      uint32_t table_id,
      tuple_id_t key,
      violate &v);

  std::pair<tuple_pb, bool> get(uint32_t table_id, tuple_id_t key);

  void put(uint32_t table_id, tuple_id_t key,
           tuple_pb &&data);

  void debug_lock(std::ostream &os);

  void debug_dependency(tx_wait_set &dep);

  void debug_print_tuple();
};
