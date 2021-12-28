#include "concurrency/access_mgr.h"

#include <utility>

access_mgr::access_mgr(
    ptr<deadlock> dl,
    fn_schedule_before fn_before,
    fn_schedule_after fn_after

) : dl_(dl), fn_before_(std::move(fn_before)), fn_after_(std::move(fn_after)) {

}

void access_mgr::lock_row(
    xid_t xid,
    oid_t oid,
    lock_mode lt,
    uint32_t table_id,
    const predicate &key,
    const ptr<tx> &tx
) {
  lock_mgr *lmc = nullptr;
  lock_table_.find_or_insert(
      table_id, [&lmc](const ptr<lock_mgr> &ctx) { lmc = ctx.get(); },
      [table_id, &lmc, this]() {
        ptr<lock_mgr> l(new lock_mgr(
            table_id,
            dl_,
            fn_before_, fn_after_));
        lmc = l.get();
        return std::make_pair(table_id, l);
      });
  if (lmc != nullptr) {
    lmc->lock(xid, oid, lt, key, tx);
  } else {
    BOOST_ASSERT(false);
  }
}

void access_mgr::unlock(
    xid_t xid,
    lock_mode mode,
    uint32_t table_id,
    const predicate &key) {
  std::pair<ptr<lock_mgr>, bool> p = lock_table_.find(table_id);
  if (p.second) {
    p.first->unlock(xid, mode, key);
  } else {
    BOOST_ASSERT(false);
  }
}

void access_mgr::make_violable(
    xid_t xid,
    lock_mode lt,
    uint32_t table_id,
    tuple_id_t key) {
  std::pair<ptr<lock_mgr>, bool> p = lock_table_.find(table_id);
  if (p.second) {
    p.first->make_violable(lt, xid, key);
  } else {
    BOOST_ASSERT(false);
  }
}

void access_mgr::put(uint32_t table_id, tuple_id_t key, const tuple_pb &data) {
  uint64_t content_id = table_id;
  ptr<data_mgr> lmc;
  data_table_.find_or_insert(
      content_id, [&lmc](ptr<data_mgr> ctx) { lmc = std::move(ctx); },
      [content_id, &lmc]() {
        ptr<data_mgr> l(new data_mgr());
        lmc = l;
        return std::make_pair(content_id, l);
      });
  if (lmc) {
    lmc->put(key, data);
    //lmc->put(key, data);
  } else {

  }
}

std::pair<tuple_pb, bool> access_mgr::get(uint32_t table_id, tuple_id_t key) {
  uint64_t content_id = table_id;
  ptr<data_mgr> mgr;
  data_table_.find_or_insert(
      content_id, [&mgr](ptr<data_mgr> ctx) { mgr = std::move(ctx); },
      [content_id, &mgr]() {
        ptr<data_mgr> l(new data_mgr());
        mgr = l;
        return std::make_pair(content_id, l);
      });
  if (mgr) {
    return mgr->get(key);
  } else {
    BOOST_ASSERT(false);
    return std::make_pair(tuple_pb(), false);
  }
}

void access_mgr::debug_lock(std::ostream &os) {
  std::map<table_id_t, ptr<lock_mgr>> mgrs;
  lock_table_.traverse([&mgrs](uint64_t table_id, ptr<lock_mgr> mgr) {
    mgrs[table_id] = std::move(mgr);
  });
  for (const auto &kv: mgrs) {
    std::stringstream ssm;
    kv.second->debug_lock(ssm);
    if (!ssm.str().empty()) {
      os << "table " << kv.first << std::endl;
      os << ssm.str();
    }
  }
}

void access_mgr::debug_dependency(tx_wait_set &dep) {
  std::map<table_id_t, ptr<lock_mgr>> mgrs;
  lock_table_.traverse([&mgrs](uint64_t table_id, ptr<lock_mgr> mgr) {
    mgrs[table_id] = mgr;
  });
  for (const auto &kv: mgrs) {
    kv.second->debug_dependency(dep);
  }
}