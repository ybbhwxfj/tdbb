#include "access/access_mgr.h"

#include <utility>

access_mgr::access_mgr(
    const std::vector<shard_id_t> &shards,
    uint64_t max_table_id
)  {
  data_table_.resize(max_table_id + 1);
  for (table_id_t id = 0; id <= max_table_id; id++) {
    for (auto shard_id : shards) {
      ptr<data_mgr> d(new data_mgr());
      data_table_[id].insert(std::make_pair(shard_id, d));
    }
  }
}

void access_mgr::put(uint32_t table_id, shard_id_t shard_id, tuple_id_t key, tuple_pb &&data) {
  BOOST_ASSERT(!is_tuple_nil(data));
  ptr<data_mgr> dm = data_table_[table_id][shard_id];
  if (dm) {
    dm->put(key, std::move(data));
    // lmc->put(key, data);
  } else {
    LOG(fatal) << "data manager put error";
  }
}

std::pair<tuple_pb, bool> access_mgr::get(uint32_t table_id, shard_id_t shard_id, tuple_id_t key) {
  ptr<data_mgr> dm = data_table_[table_id][shard_id];
  if (dm) {
    return dm->get(key);
  } else {
    LOG(fatal) << "data manager get error";
    return std::make_pair(tuple_pb(), false);
  }
}


void access_mgr::debug_print_tuple() {
  for (table_id_t i = 0; i < data_table_.size(); i++) {
    for (const auto &kv : data_table_[i]) {
      kv.second->print();
    }
  }
}