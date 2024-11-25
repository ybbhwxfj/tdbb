#pragma once

#include "common/callback.h"
#include "common/hash_table.h"
#include "common/ptr.hpp"
#include "common/tuple.h"
#include "data_mgr.h"

#include "proto/proto.h"
#include <atomic>
#include <boost/icl/interval_map.hpp>
#include <functional>
#include <map>
#include <mutex>
#include <unordered_map>

class access_mgr {
private:
  typedef concurrent_hash_table<uint64_t, ptr<data_mgr>> data_table_t;
  std::vector<std::unordered_map<shard_id_t, ptr<data_mgr>>> data_table_;
public:
    access_mgr(
             const std::vector<shard_id_t> &shards,
             uint64_t max_table_id);

  std::pair<tuple_pb, bool> get(uint32_t table_id, uint32_t shard_id, tuple_id_t key);

  void put(uint32_t table_id, uint32_t shard_id, tuple_id_t key, tuple_pb &&data);

  void debug_print_tuple();
};
