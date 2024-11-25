#pragma once

#include "common/hash_table.h"
#include "common/id.h"
#include "common/ptr.hpp"
#include "common/tuple.h"
#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <unordered_map>

class data_mgr {
private:
  struct tuple_list {
    tuple_list() {}

    std::mutex mutex_;
    std::list<tuple_pb> versions_;
  };

  typedef concurrent_hash_table<tuple_id_t, ptr<tuple_list>> data_table_t;

  data_table_t key_row_locks_;

public:
  data_mgr() {}

  ~data_mgr();

  void put(tuple_id_t key, tuple_pb &&tuple);

  std::pair<tuple_pb, bool> get(tuple_id_t key);

  void print();
};
