#include "concurrency/lock_mgr.h"
#include <memory>
#include <utility>
#include <boost/icl/rational.hpp>

lock_mgr::lock_mgr(
    table_id_t table_id,
    ptr<deadlock> dl,
    fn_schedule_before fn_before,
    fn_schedule_after fn_after
) : table_id_(table_id),
    dl_(std::move(dl)),
    fn_before_(std::move(fn_before)), fn_after_(std::move(fn_after)) {}

lock_mgr::~lock_mgr() = default;

void lock_mgr::lock(
    xid_t xid,
    oid_t oid,
    lock_mode lt,
    const predicate &pred,
    const ptr<tx> &txn) {
  std::scoped_lock l(mutex_);
  //BOOST_ASSERT(xid == tx->xid());
  if (lt == LOCK_WRITE_ROW || lt == LOCK_READ_ROW) {
    tuple_id_t key = pred.key_;
    if (predicate_.empty()) {
      row_lock(xid, oid, lt, key, txn);
    } else {
      //TODO ...
      if (lt == LOCK_WRITE_ROW) {
        auto i = predicate_.find(key);
        if (i != predicate_.end()) {
          auto slot = get_lock_slot(key);
          if (slot) {

          }
          slot->lock(lt, txn, oid);
        } else {
          row_lock(xid, oid, lt, key, txn);
        }
      } else if (lt == LOCK_READ_ROW) {
        row_lock(xid, oid, lt, key, txn);
      } else {
        BOOST_ASSERT(false);
      }
    }
  } else if (lt == LOCK_READ_PREDICATE) {
    std::set<tuple_id_t> tuple_ids;
    bool conflict = find_conflict(xid, oid, txn, tuple_ids, pred);
    tx_conflict tc(txn, oid);
    tc.write_ = tuple_ids;
    auto i = predicate_.find(pred.interval_);
    if (i == predicate_.end()) {
      tx_conflict_set tc_set;
      tc_set.insert(std::make_pair(xid, tc));
      predicate_.insert(std::make_pair(pred.interval_, tc_set));
    } else {
      const tx_conflict_set &s = i->second;
      const_cast<tx_conflict_set &>(s).insert(std::make_pair(xid, tc));
    }
    //predicate_ += std::make_pair(pred.interval_, tc_set);

    if (not conflict) {
      txn->lock_acquire(EC::EC_OK, oid);
    }
  }
}

void lock_mgr::unlock(uint64_t xid, lock_mode mode,
                      const predicate &pred) {
  std::scoped_lock l(mutex_);
  if (mode == LOCK_WRITE_ROW || mode == LOCK_READ_ROW) {
    std::pair<ptr<lock_slot>, bool> p = key_row_locks_.find(pred.key_);
    if (p.second) {
      p.first->unlock(xid);
    } else {
      BOOST_ASSERT(false);
    }
  } else if (mode == LOCK_READ_PREDICATE) {
    auto i = predicate_.lower_bound(pred.interval_);
    while (i != predicate_.end()) {
      if (boost::icl::contains(pred.interval_, i->first)) {
        tx_conflict_set &s = i->second;
        auto ic = s.find(xid);
        if (ic != s.end()) {
          for (tuple_id_t tuple_id: ic->second.write_) {
            auto iwk = write_key_.find(tuple_id);
            if (iwk != write_key_.end()) {
              iwk->second->unlock(xid);
              write_key_.erase(iwk);
            }
          }
          s.erase(ic);
        }
        if (s.empty()) {
          auto to_remove_i = i;
          i++;
          predicate_.erase(to_remove_i);
        } else {
          i++;
        }
      } else {
        break;
      }
    }
  }

}

void lock_mgr::make_violable(lock_mode lt, uint64_t xid,
                             tuple_id_t key) {
  std::scoped_lock l(mutex_);
  std::pair<ptr<lock_slot>, bool> p = key_row_locks_.find(key);
  if (p.second) {
    p.first->make_violable(lt, xid);
  } else {
    BOOST_ASSERT(false);
  }
}

void lock_mgr::debug_lock(std::ostream &os) {
  std::map<uint64_t, ptr<lock_slot>> locks;
  key_row_locks_.traverse([&locks](tuple_id_t key, const ptr<lock_slot> &l) {
    uint64_t k = (key);
    locks.insert(std::make_pair(k, l));
  });
  for (const auto &kv: locks) {
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
  for (const auto &kv: locks) {
    kv.second->build_dependency(dep);
  }
}

void lock_mgr::row_lock(
    xid_t xid,
    oid_t oid,
    lock_mode lt,
    tuple_id_t key,
    const ptr<tx> &tx) {
  auto slot = get_lock_slot(key);
  if (slot) {
    slot->lock(lt, tx, oid);
    slot->assert_check();
  } else {
    BOOST_LOG_TRIVIAL(error) << "lock " << xid << " error";
    BOOST_ASSERT(false);
  }
}

std::pair<ptr<lock_slot>, bool> lock_mgr::find_slot(tuple_id_t key) {
  return key_row_locks_.find(key);
}

ptr<lock_slot> lock_mgr::get_lock_slot(
    tuple_id_t key
) {
  ptr<lock_slot> slot;
  //BOOST_LOG_TRIVIAL(info) << xid << " " << oid << " " << enum2str(lt) <<  " lock "  << " key:" << binary_2_tuple_id(key);
  key_row_locks_.find_or_insert(
      key,
      [&slot](const ptr<lock_slot> &value) {
        slot = value; // TODO
        value->assert_check();
      },
      [key, &slot, this]() {
        slot = std::make_shared<lock_slot>(
            this,
            table_id_,
            key,
            fn_before_,
            fn_after_
        );
        write_key_.insert(std::make_pair(key, slot));
        slot->assert_check();
        return std::make_pair(key, slot);
      });
  return slot;
}

bool lock_mgr::conflict(
    xid_t xid,
    oid_t oid,
    const predicate &pred
) {
  std::scoped_lock l(mutex_);
  std::set<tuple_id_t> tuple_ids;
  return find_conflict(xid, oid, nullptr, tuple_ids, pred);
}

void lock_mgr::async_wait_lock(fn_wait_lock fn) {
  if (dl_) {
    dl_->async_wait_lock(fn);
  }
}

bool lock_mgr::find_conflict(xid_t xid,
                             oid_t oid,
                             ptr<tx> txn,
                             std::set<tuple_id_t> tuple_ids,
                             const predicate &pred
) {
  for (const auto &kv: write_key_) {
    if (boost::icl::contains(pred.interval_, kv.first)) {
      tuple_ids.insert(kv.first);
    }
  }
  int blocking = 0;
  for (tuple_id_t tuple_id: tuple_ids) {
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