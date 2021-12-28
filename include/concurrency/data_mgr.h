#pragma once

#include "common/hash_table.h"
#include "common/ptr.hpp"
#include "common/tuple.h"
#include <functional>
#include <atomic>
#include <map>
#include <mutex>
#include <unordered_map>
#include "common/id.h"

class data_mgr {
private:
  struct tuple_list {
    tuple_list() {}
    std::recursive_mutex mutex_;
    std::list<tuple_pb> versions_;
  };
  class key_equal {
  public:
    key_equal() {}
    bool operator()(tuple_id_t x, tuple_id_t y) const {
      return x == y;
    }
  };
  class key_hash {
  public:
    key_hash() {}
    size_t operator()(tuple_id_t i) const {
      std::hash<tuple_id_t> h;
      return h(i);
    }
  };
  typedef hash_table<tuple_id_t, ptr<tuple_list>, key_hash, key_equal> data_table_t;

  std::atomic_bool has_range_lock_;
  std::mutex range_lock_mutex_;
  data_table_t key_row_locks_;

public:
  data_mgr() : has_range_lock_(false) {}
  ~data_mgr();

  void put(tuple_id_t key, const tuple_pb &tuple);

  std::pair<tuple_pb, bool> get(tuple_id_t key);
};
