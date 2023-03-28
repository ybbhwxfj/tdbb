#include "concurrency/lock_mgr.h"
#include "common/scoped_time.h"
#include <boost/icl/rational.hpp>
#include <memory>
#include <utility>

lock_mgr::lock_mgr(table_id_t table_id, shard_id_t shard_id, boost::asio::io_context &context,
                   deadlock *dl, fn_schedule_before fn_before,
                   fn_schedule_after fn_after)
    : table_id_(table_id), shard_id_(shard_id), dl_(dl), fn_before_(std::move(fn_before)),
      fn_after_(std::move(fn_after)), key_row_locks_(1024 * 128),
      strand_(context) {}

lock_mgr::~lock_mgr() = default;

void lock_mgr::lock(xid_t xid, oid_t oid, lock_mode lt, predicate pred,
                    ptr<tx_rm> txn) {

  auto fn = [this, xid, oid, lt, pred, txn] {
    scoped_time _t("lock_mgr::lock_gut");
    this->lock_gut(oid, lt, pred, txn);
  };
  boost::asio::post(strand_, fn);
}

void lock_mgr::unlock(uint64_t xid, lock_mode mode, predicate pred) {

  auto fn = [this, xid, mode, pred] {
    scoped_time _t("lock_mgr::unlock_gut");
    this->unlock_gut(xid, mode, pred);
  };
  boost::asio::post(strand_, fn);
}

void lock_mgr::lock_gut(

    oid_t oid, lock_mode lt, predicate pred, ptr<tx_rm> txn) {

  // BOOST_ASSERT(xid == tx_rm->xid());
  if (lt == LOCK_WRITE_ROW || lt == LOCK_READ_ROW) {
    tuple_id_t key = pred.key_;
    row_lock(oid, lt, key, txn);
  } else if (lt == LOCK_READ_PREDICATE) {
  }
}

void lock_mgr::unlock_gut(uint64_t xid, lock_mode mode, predicate pred) {

  if (mode == LOCK_WRITE_ROW || mode == LOCK_READ_ROW) {
    std::pair<ptr<lock_slot>, bool> p = key_row_locks_.find(pred.key_);
    if (p.second) {
      p.first->unlock(xid);
    } else {
      BOOST_ASSERT(false);
    }
  } else if (mode == LOCK_READ_PREDICATE) {
  }
}

void lock_mgr::debug_lock(std::ostream &os) {
  std::map<uint64_t, ptr<lock_slot>> locks;
  key_row_locks_.traverse([&locks](tuple_id_t key, const ptr<lock_slot> &l) {
    uint64_t k = (key);
    locks.insert(std::make_pair(k, l));
  });
  for (const auto &kv : locks) {
    std::stringstream ssm;
    kv.second->debug_lock(ssm);
    if (!ssm.str().empty()) {
      os << "key :" << kv.first << std::endl;
      os << ssm.str();
    }
  }
}

void lock_mgr::debug_dependency(tx_wait_set &dep) {
  std::map<uint64_t, ptr<lock_slot>> locks;
  key_row_locks_.traverse([&locks](tuple_id_t key, const ptr<lock_slot> &l) {
    uint64_t k = (key);
    locks.insert(std::make_pair(k, l));
  });
  for (const auto &kv : locks) {
    kv.second->build_dependency(dep);
  }
}

void lock_mgr::row_lock(

    oid_t oid, lock_mode lt, tuple_id_t key, const ptr<tx_rm> &tx) {
  auto pair = get_lock_slot(key);
  pair.first->lock(lt, tx, oid);
}

std::pair<ptr<lock_slot>, bool> lock_mgr::find_slot(tuple_id_t key) {
  return key_row_locks_.find(key);
}

std::pair<ptr<lock_slot>, bool> lock_mgr::get_lock_slot(tuple_id_t key) {

  // LOG(info) << xid << " " << oid << " " << enum2str(lt) <<  " lock "  << "
  // key:" << binary_2_tuple_id(key);
  std::pair<ptr<lock_slot>, bool> ret = key_row_locks_.find_or_insert(
      key, [](const ptr<lock_slot> &) {},
      [key, this]() {
        ptr<lock_slot> slot = cs_new<lock_slot>(
            this, table_id_, shard_id_, key, fn_before_, fn_after_);
        return slot;
      });
  assert(ret.first->tuple_id() == key);
  return ret;
}

bool lock_mgr::conflict(xid_t xid, oid_t oid, const predicate &pred) {

  std::set<tuple_id_t> tuple_ids;
  return find_conflict(xid, oid, nullptr, tuple_ids, pred);
}

void lock_mgr::async_wait_lock(fn_wait_lock fn) {
  if (dl_) {
    dl_->async_wait_lock(fn);
  }
}

bool lock_mgr::find_conflict(xid_t xid, oid_t oid, ptr<tx_rm> txn,
                             std::set<tuple_id_t> tuple_ids,
                             const predicate &) {

  int blocking = 0;
  for (tuple_id_t tuple_id : tuple_ids) {
    auto p = find_slot(tuple_id);
    if (p.second) {
      if (p.first->predicate_conflict(xid, oid, txn)) {
        blocking++;
      }
    }
  }
  if (blocking == 0) {
    return false;
  } else {
    return true;
  }
}