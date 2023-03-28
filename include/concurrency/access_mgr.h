#pragma once

#include "common/callback.h"
#include "common/hash_table.h"
#include "common/ptr.hpp"
#include "common/tuple.h"
#include "concurrency/data_mgr.h"
#include "concurrency/lock.h"
#include "concurrency/lock_mgr.h"
#include "concurrency/violate.h"
#include "proto/proto.h"
#include <atomic>
#include <boost/icl/interval_map.hpp>
#include <functional>
#include <map>
#include <mutex>
#include <unordered_map>

class access_mgr {
private:
  typedef concurrent_hash_table<uint64_t, ptr<lock_mgr>> lock_table_t;
  typedef concurrent_hash_table<uint64_t, ptr<data_mgr>> data_table_t;
  net_service *service_;
  deadlock *dl_;
  std::vector<std::unordered_map<shard_id_t, ptr<lock_mgr>>> lock_table_;
  std::vector<std::unordered_map<shard_id_t, ptr<data_mgr>>> data_table_;
  fn_schedule_before fn_before_;
  fn_schedule_after fn_after_;

public:
  access_mgr(net_service *service, deadlock *dl, fn_schedule_before fn_before,
             fn_schedule_after fn_after,
             const std::vector<shard_id_t> &shards,
             uint64_t max_table_id);

  void lock_row(xid_t xid, oid_t op_id, lock_mode lt, uint32_t table_id, uint32_t shard_id,
                const predicate &key, const ptr<tx_rm> &tx);

  void unlock(xid_t xid, lock_mode lt, uint32_t table_id, uint32_t shard_id,
              const predicate &pred);

  std::pair<tuple_pb, bool> get(uint32_t table_id, uint32_t shard_id, tuple_id_t key);

  void put(uint32_t table_id, uint32_t shard_id, tuple_id_t key, tuple_pb &&data);

  void debug_lock(std::ostream &os);

  void debug_dependency(tx_wait_set &dep);

  void debug_print_tuple();
};
