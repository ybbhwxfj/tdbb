#include "common/db_type.h"
#include "concurrency/tx_context.h"
#include "proto/proto.h"
#include <utility>
#include "concurrency/lock_slot.h"

lock_slot::lock_slot(
    lock_mgr_trait *mgr,
    table_id_t table_id,
    tuple_id_t tuple_id,
    fn_schedule_before fn_before,
    fn_schedule_after fn_after) :
    mgr_(mgr),
    table_id_(table_id),
    tuple_id_(tuple_id),
    fn_before_(std::move(fn_before)),
    fn_after_(std::move(fn_after)),
    read_count_(0),
    write_count_(0),
    read_violate_count_(0),
    write_violate_count_(0) {

}

lock_slot::~lock_slot() {
  read_count_ = 0;
}

bool lock_slot::lock(lock_mode type, const ptr<tx> &tx, oid_t oid) {
  std::scoped_lock l(mutex_);
  bool ok = false;
  if (type == LOCK_READ_ROW || type == LOCK_READ_PREDICATE) {
    ok = read_lock(tx, oid, type);
  } else if (type == LOCK_WRITE_ROW) {
    ok = write_lock(tx, oid);
  } else {
    BOOST_ASSERT(false);
  }
  assert_check();
  return ok;
}

void lock_slot::unlock(xid_t tx) {
  std::scoped_lock l(mutex_);
  unlock_gut(tx);
  assert_check();
}

void lock_slot::make_violable(lock_mode, xid_t tx) {
  std::scoped_lock l(mutex_);
  make_violable(tx);
}

bool lock_slot::add_wait(const ptr<tx> &ctx, oid_t oid, lock_mode type) {
  xid_t xid = ctx->xid();
  auto i = info_.find(xid);
  if (i != info_.end()) {
    BOOST_ASSERT(not i->second.acquired_);
    i->second.oid_.push_back(oid);
    return false;
  } else {
#ifdef DB_TYPE_NON_DETERMINISTIC
    if (is_non_deterministic()) {
      async_add_dependency(ctx);
    }
#endif
    info_.insert(std::make_pair(ctx->xid(),
                                tx_info(ctx, oid, type, false)));
    wait_.push_back(ctx->xid());
    return true;
  }
}

bool lock_slot::add_read(const ptr<tx> &ctx, oid_t oid, lock_mode mode) {
  auto i = info_.find(ctx->xid());
  if (i != info_.end()) {
    if (i->second.acquired_) {
      i->second.oid_.push_back(oid);
    } else {
      i->second.acquired_ = true;
      read_count_++;
      read_violate_count_++;
      read_.push_back(ctx->xid());
    }
    return false;
  } else {
    read_count_++;
    read_violate_count_++;
    read_.push_back(ctx->xid());
    info_.insert(std::make_pair(ctx->xid(), tx_info(ctx, oid, mode, true)));
    return true;
  }
}

bool lock_slot::add_write(const ptr<tx> &ctx, oid_t oid) {
  auto i = info_.find(ctx->xid());
  if (i != info_.end()) {
    if (i->second.acquired_) {
      i->second.oid_.push_back(oid);
    } else {
      i->second.acquired_ = true;
      write_count_++;
      write_violate_count_++;
      write_.push_back(ctx->xid());
    }
    return false;
  } else {
    write_count_++;
    write_violate_count_++;
    write_.push_back(ctx->xid());
    info_.insert(std::make_pair(
        ctx->xid(),
        tx_info(ctx, oid, LOCK_WRITE_ROW, true)));
    return true;
  }
}

bool lock_slot::read_lock(const ptr<tx> &ctx, oid_t oid, lock_mode mode) {
  BOOST_ASSERT(read_count_ == read_.size());

  auto p = acquire_read_lock(ctx, oid, mode);
  bool acquire = p.first;
  bool violate = p.second;
  if (fn_before_) {
    fn_before_(tx_op(tx_cmd_type::TX_CMD_RM_READ,
                     ctx->xid(),
                     oid,
                     table_id_,
                     tuple_id_));
  }
  if (acquire) {
    if (violate) {
#ifdef DB_TYPE_GEO_REP_OPTIMIZE
      ptr<std::vector<ptr<tx_context>>> in(new std::vector<ptr<tx_context>>());
      dependency_write_read(*in);
      dlv_acquire(EC::EC_OK, ctx, oid, in);
#endif
    } else {
      on_lock_acquired(EC::EC_OK, LOCK_READ_ROW, ctx, oid);
    }
  } else {
    BOOST_ASSERT(std::find(read_.begin(), read_.end(), ctx->xid()) == read_.end());
    BOOST_ASSERT(std::find(wait_.begin(), wait_.end(), ctx->xid()) != wait_.end());
    BOOST_LOG_TRIVIAL(trace) << " cannot acquire lock";
  }

  BOOST_ASSERT(read_count_ == read_.size());

  return acquire;
}

bool lock_slot::write_lock(const ptr<tx> &ctx, oid_t oid) {
  bool acquire = false;
  bool violate = false;
  if (write_count_ == 0 && read_count_ == 0) {
    // lock acquire
    add_write(ctx, oid);
    acquire = true;
    BOOST_ASSERT(wait_.empty());
  } else if (write_violate_count_ == 0 && read_violate_count_ == 0) {
    // lock violate
    add_write(ctx, oid);
    acquire = true;
    violate = true;
    BOOST_ASSERT(wait_.empty());
  } else {
    auto iw = info_.find(ctx->xid());
    if (iw != info_.end()) {
      if (iw->second.acquired_) {
        iw->second.oid_.push_back(oid);
        acquire = true;
      } else {
        iw->second.oid_.push_back(oid);
      }
    } else {
      // lock wait ..
      add_wait(ctx, oid, LOCK_WRITE_ROW);
    }
  }
  if (fn_before_) {
    fn_before_(tx_op(
        tx_cmd_type::TX_CMD_RM_WRITE,
        ctx->xid(),
        oid,
        table_id_,
        tuple_id_));
  }
  if (acquire) {
    on_lock_acquired(EC::EC_OK, LOCK_WRITE_ROW, ctx, oid);
    if (violate) {

    }
  } else {
    BOOST_ASSERT(std::find(write_.begin(), write_.end(), ctx->xid()) == write_.end());
    BOOST_ASSERT(std::find(wait_.begin(), wait_.end(), ctx->xid()) != wait_.end());
    BOOST_LOG_TRIVIAL(trace) << " cannot acquire lock";
  }

  BOOST_ASSERT(write_count_ == write_.size());
  return acquire;
}

std::pair<bool, bool> lock_slot::acquire_read_lock(const ptr<tx> &ctx, oid_t oid, lock_mode mode) {
  bool acquire = false;
  bool violate = false;
  if (write_count_ == 0) {
    // lock acquire
    add_read(ctx, oid, mode);
    acquire = true;
    BOOST_ASSERT(read_count_ == read_.size());
  } else if (write_violate_count_ == 0) {
    // lock violate
    add_read(ctx, oid, mode);
    acquire = true;
    violate = true;
  } else {
    auto ir = info_.find(ctx->xid());
    if (ir != info_.end()) {
      if (ir->second.acquired_) {
        acquire = true;  // we have acquired this lock
        ir->second.oid_.push_back(oid);
      } else {
        ir->second.oid_.push_back(oid);
      }
    } else {
      // lock wait ..
      add_wait(ctx, oid, mode);
    }
  }
  return std::make_pair(acquire, violate);
}

void lock_slot::unlock_gut(xid_t xid) {
  remove_lock(xid);
  assert_check();
  notify_lock_acquire();
  assert_check();
  BOOST_ASSERT(not(write_violate_count_ == 0 && read_violate_count_ == 0 && (not wait_.empty())));
  BOOST_ASSERT(not(write_violate_count_ == 0
      && (not wait_.empty() && info_.find(wait_.front())->second.type_ == LOCK_READ_ROW)));
}

void lock_slot::make_violable(xid_t xid) {
  bool notify = false;
  auto iter = info_.find(xid);
  if (iter != info_.end()) {
    if (not iter->second.violate_ && iter->second.acquired_) {
      if (iter->second.type_ == LOCK_WRITE_ROW) {
        BOOST_ASSERT(iter->second.type_ == LOCK_WRITE_ROW);
        if (not iter->second.violate_) { // exactly once
          iter->second.violate_ = true;
          write_violate_count_--;
          notify = write_violate_count_ == 0;
        }
      } else {
        if (not iter->second.violate_) { // exactly once
          read_violate_count_--;
          iter->second.violate_ = true;
          notify = read_violate_count_ == 0;
        }
      }
    }
  }
  if (notify) {
    notify_lock_acquire();
  }
  BOOST_ASSERT(not(write_violate_count_ == 0 && read_violate_count_ == 0 && (not wait_.empty())));
  BOOST_ASSERT(not(write_violate_count_ == 0
      && (not wait_.empty() && info_.find(wait_.front())->second.type_ == LOCK_READ_ROW)));
}

bool lock_slot::remove_lock(xid_t xid) {
  auto i = info_.find(xid);
  if (i != info_.end()) {
    tx_info &ti = i->second;
    if (ti.acquired_) {
      assert_check();
      if (ti.type_ == LOCK_READ_ROW || ti.type_ == LOCK_READ_PREDICATE) {
        BOOST_ASSERT(read_count_ > 0);
        read_count_--;
        if (not ti.violate_) {
          read_violate_count_--;
        }
        info_.erase(i);
        auto rb = std::remove(read_.begin(), read_.end(), xid);
        read_.erase(rb, read_.end());
      } else if (ti.type_ == LOCK_WRITE_ROW) {
        BOOST_ASSERT(write_count_ > 0);
        write_count_--;
        if (not ti.violate_) {
          write_violate_count_--;
        }
        info_.erase(i);
        auto rb = std::remove(write_.begin(), write_.end(), xid);
        write_.erase(rb, write_.end());
      }
    } else {
      info_.erase(i);
      auto rb = std::remove(wait_.begin(), wait_.end(), xid);
      wait_.erase(rb, wait_.end());
    }
    assert_check();
    return true;
  } else {
    assert_check();
    return false;
  }
}

void lock_slot::notify_lock_acquire() {
  assert_check();

  if (wait_.empty()) {
    return;
  }

  std::vector<xid_t> notify_tx;
  uint32_t read = 0;
  uint32_t write = 0;
  while (not wait_.empty()) {
    xid_t x = wait_.front();
    auto i = info_.find(x);
    if (i == info_.end()) {
      BOOST_ASSERT(false);
      continue;
    }
    if ((i->second.type_ == LOCK_READ_ROW
        || i->second.type_ == LOCK_READ_PREDICATE)) {
      if (write == 0
          && (write_count_ == 0
              || write_violate_count_ == 0)) {
        if (i->second.type_ == LOCK_READ_PREDICATE) {
          if (mgr_) {
            // TODO, possible multiple same access predicate in one tx
            if (mgr_->conflict(i->second.ctx_->xid(), 0, *i->second.predicate_)) {
              break;
            }
          }
        }
        notify_tx.push_back(x);
        wait_.pop_front();
        read++;
      } else {
        break;
      }
    } else if (i->second.type_ == LOCK_WRITE_ROW) {
      if ((read == 0 && write == 0)
          && ((read_count_ == 0 && write_count_ == 0)
              || (read_violate_count_ == 0 && write_violate_count_ == 0))
          ) {
        notify_tx.push_back(x);
        wait_.pop_front();
        write++;
      }
      break;
    }
  }

  for (xid_t x: notify_tx) {
    auto i = info_.find(x);
    if (i != info_.end()) {
      i->second.acquired_ = true;
      if (i->second.type_ == LOCK_READ_ROW || i->second.type_ == LOCK_READ_PREDICATE) {
        read_count_++;
        read_violate_count_++;
        read_.push_back(x);
      } else if (i->second.type_ == LOCK_WRITE_ROW) {
        write_count_++;
        write_violate_count_++;
        write_.push_back(x);
      }
      for (oid_t oid: i->second.oid_) {
        on_lock_acquired(EC::EC_OK, i->second.type_, i->second.ctx_, oid);
      }
    } else {
      BOOST_ASSERT(false);
    }
  }

  // not exists such case:
  // 1. no read or write locks are holding, but there is any waiting tx;
  // 2. no write locks are holding, but there is any waiting tx want to acquire read lock.
  BOOST_ASSERT(not(read_count_ == 0 && write_count_ == 0 && (not wait_.empty())));
  BOOST_ASSERT(not(write_count_ == 0
      && (not wait_.empty() && info_.find(wait_.front())->second.type_ == LOCK_READ_ROW)));
  BOOST_ASSERT(not(write_violate_count_ == 0 && read_violate_count_ == 0 && (not wait_.empty())));
  BOOST_ASSERT(not(write_violate_count_ == 0
      && (not wait_.empty() && info_.find(wait_.front())->second.type_ == LOCK_READ_ROW)));

  assert_check();
}

void lock_slot::debug_lock(std::ostream &os) {
  for (auto x: read_) {
    os << " " << x << " R lock ";
    os << "oid [";
    for (oid_t oid: info_.find(x)->second.oid_) {
      os << oid;
    }
    os << "]" << std::endl;
  }
  for (auto x: write_) {
    os << " " << x << " W lock ";
    os << "oid [";
    for (oid_t oid: info_.find(x)->second.oid_) {
      os << oid;
    }
    os << "]" << std::endl;
  }
  for (auto x: wait_) {
    os << " " << x << " Wait " << enum2str(info_.find(x)->second.type_);
    os << " oid [";
    for (oid_t oid: info_.find(x)->second.oid_) {
      os << oid;
    }
    os << "]" << std::endl;
  }
}

void lock_slot::build_dependency(tx_wait_set &ds) {
  std::scoped_lock l(mutex_);
  for (auto x: wait_) {
    auto i = info_.find(x);
    if (i != info_.end()) {
      if (i->second.type_ == LOCK_READ_ROW) {
        ds.add(write_, x);
      } else if (i->second.type_ == LOCK_WRITE_ROW) {
        ds.add(read_, x);
        ds.add(write_, x);
      }
    }
  }
}

void lock_slot::assert_check() {
  BOOST_ASSERT(write_count_ == write_.size());
  BOOST_ASSERT(read_count_ == read_.size());
  BOOST_ASSERT(write_count_ + read_count_ + wait_.size() == info_.size());
}

#ifdef DB_TYPE_GEO_REP_OPTIMIZE

void lock_slot::dlv_acquire(EC ec, const ptr<tx> &ctx, oid_t, const ptr<std::vector<ptr<tx_context>>> &in) {
  dynamic_cast<tx_context *>(ctx.get())->notify_lock_acquire(ec, in);
}

void lock_slot::dependency_write_read(std::vector<ptr<tx_context>> &in) {
  if (write_violate_count_ == 0 && write_count_ > 0) {
    for (const auto &i: info_) {
      if (i.second.type_ == LOCK_WRITE_ROW) {
        in.push_back(static_pointer_cast<tx_context>(i.second.ctx_));
      }
    }
  }
}

#endif  // DB_TYPE_GEO_REP_OPTIMIZE

result<void> lock_slot::wait_timeout(xid_t xid, tx_wait_set &ws) {
  std::scoped_lock l(mutex_);
  auto i = info_.find(xid);
  if (i == info_.end()) {
    return outcome::failure(EC::EC_NOT_FOUND_ERROR);
  } else {
    if (i->second.acquired_) {
      return outcome::failure(EC::EC_NOT_FOUND_ERROR);
    } else {
      build_dependency(ws);
      return outcome::success();
    }
  }
}

void lock_slot::async_add_dependency(const ptr<tx> &ctx) {
  ptr<lock_slot> lock = shared_from_this();
  xid_t xid = ctx->xid();
  auto fn = [xid, lock](tx_wait_set &ws) {
    return lock->wait_timeout(xid, ws);
  };
  mgr_->async_wait_lock(fn);
}

bool lock_slot::predicate_conflict(xid_t xid, oid_t oid, ptr<tx> txn) {
  std::scoped_lock l(mutex_);
  bool acquire = false;
  if (write_count_ == 0) {
    // lock acquire
    acquire = true;
    BOOST_ASSERT(read_count_ == read_.size());
  } else if (write_violate_count_ == 0) {
    // lock violate
    acquire = true;
  }

  auto ir = info_.find(xid);
  if (ir != info_.end()) {
    if (ir->second.acquired_) {
      acquire = true;
    }
  } else {
    if (acquire) {
      read_count_++;
      read_violate_count_++;
    } else {
      wait_.push_back(xid);
    }
    info_.insert(std::make_pair(xid, tx_info(txn, oid, LOCK_READ_PREDICATE, acquire)));
  }

  return acquire;
}
void lock_slot::on_lock_acquired(EC ec, lock_mode mode, ptr<tx> txn, oid_t oid) {
  if (fn_after_) {
    tx_cmd_type cmd =
        mode == LOCK_READ_ROW || mode == LOCK_READ_PREDICATE
        ? TX_CMD_RM_READ : TX_CMD_RM_WRITE;
    fn_after_(tx_op(cmd,
                    txn->xid(),
                    oid,
                    table_id_,
                    tuple_id_));
  }
  txn->lock_acquire(ec, oid);
}