#include "concurrency/access_mgr.h"

#include <utility>

access_mgr::access_mgr(
    net_service *service, deadlock *dl,
    fn_schedule_before fn_before,
    fn_schedule_after fn_after,
    const std::vector<shard_id_t> &shards,
    uint64_t max_table_id
)
    : service_(service), dl_(dl), fn_before_(std::move(fn_before)),
      fn_after_(std::move(fn_after)) {
  lock_table_.resize(max_table_id + 1);
  data_table_.resize(max_table_id + 1);
  for (table_id_t id = 0; id <= max_table_id; id++) {
    for (auto shard_id : shards) {
      ptr<lock_mgr> l(new lock_mgr(id, shard_id, service_->get_service(SERVICE_CC), dl_,
                                   fn_before_, fn_after_));
      lock_table_[id].insert(std::make_pair(shard_id, l));

      ptr<data_mgr> d(new data_mgr());
      data_table_[id].insert(std::make_pair(shard_id, d));
    }
  }
}

void access_mgr::lock_row(xid_t xid, oid_t oid, lock_mode lt, uint32_t table_id,
                          uint32_t shard_id,
                          const predicate &key, const ptr<tx_rm> &tx) {
  ptr<lock_mgr> lm = lock_table_[table_id][shard_id];
  if (lm) {
    lm->lock(xid, oid, lt, key, tx);
  } else {
    LOG(fatal) << "lock row error";
  }
}

void access_mgr::unlock(xid_t xid, lock_mode mode, uint32_t table_id, uint32_t shard_id,
                        const predicate &key) {
  ptr<lock_mgr> lm = lock_table_[table_id][shard_id];
  if (lm) {
    lm->unlock(xid, mode, key);
  } else {
    LOG(fatal) << "unlock row error";
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

void access_mgr::debug_lock(std::ostream &os) {
  std::map<table_id_t, std::map<shard_id_t, ptr<lock_mgr>>> mgrs;
  for (table_id_t i = 0; i < lock_table_.size(); i++) {
    for (const auto &kv : lock_table_[i]) {
      mgrs[i][kv.first] = kv.second;
    }
  }
  for (const auto &table_kv : mgrs) {
    for (auto &shard_kv : table_kv.second) {
      std::stringstream ssm;
      shard_kv.second->debug_lock(ssm);
      if (!ssm.str().empty()) {
        os << "table " << table_kv.first << shard_kv.first << std::endl;
        os << ssm.str();
      }
    }
  }
}

void access_mgr::debug_dependency(tx_wait_set &dep) {
  std::map<table_id_t, std::map<shard_id_t, ptr<lock_mgr>>> mgrs;
  for (table_id_t i = 0; i < lock_table_.size(); i++) {
    for (const auto &kv : lock_table_[i]) {
      mgrs[i][kv.first] = kv.second;
    }
  }
  for (const auto &table_kv : mgrs) {
    for (auto &shard_kv : table_kv.second) {
      shard_kv.second->debug_dependency(dep);
    }
  }
}

void access_mgr::debug_print_tuple() {
  for (table_id_t i = 0; i < data_table_.size(); i++) {
    for (const auto &kv : data_table_[i]) {
      kv.second->print();
    }
  }
}