#include "concurrency/lock_slot.h"
#include "common/db_type.h"
#include "common/define.h"
#include "common/result.hpp"
#include "concurrency/tx_context.h"
#include "proto/proto.h"
#include <utility>

#include "concurrency/lock_mgr.h"

lock_slot::lock_slot(lock_mgr *mgr, table_id_t table_id, shard_id_t shard_id, tuple_id_t tuple_id,
                     fn_schedule_before fn_before, fn_schedule_after fn_after)
    : mgr_(mgr), table_id_(table_id), shard_id_(shard_id), tuple_id_(tuple_id),
      fn_before_(std::move(fn_before)), fn_after_(std::move(fn_after)),
      read_count_(0), write_count_(0) {}

lock_slot::~lock_slot() { read_count_ = 0; }

bool lock_slot::lock(lock_mode type, const ptr<tx_rm> &tx, oid_t oid) {
  std::unique_lock l(mutex_);
  bool ok = false;
  std::pair<ptr<tx_lock_ctx>, bool> pair = add_lock_info(type, tx, oid);
  if (pair.second) {
    on_lock_acquired(EC::EC_OK, LOCK_READ_ROW, pair.first->ctx_, oid);
    return true;
  }
  if (type == LOCK_READ_ROW || type == LOCK_READ_PREDICATE) {
    ok = read_lock(pair.first, oid);
  } else if (type == LOCK_WRITE_ROW) {
    ok = write_lock(pair.first, oid);
  } else {
    BOOST_ASSERT(false);
  }
  assert_check();
  return ok;
}

void lock_slot::unlock(xid_t tx) {
  std::unique_lock l(mutex_);
  unlock_gut(tx);
  assert_check();
}

void lock_slot::add_wait(ptr<tx_lock_ctx> info, oid_t oid) {
#ifdef TEST_TRACE_LOCK
  trace_ << "wait " << info->ctx_->xid() << ":" << oid << "@@";
#endif
  wait_.push_back(xid_oid_t(info->ctx_->xid(), oid));
  // after insert tx_info, build dependency
#ifdef DB_TYPE_NON_DETERMINISTIC
  if (is_non_deterministic()) {
    async_add_dependency(info->ctx_);
  }
#endif
  assert_check();
}

void lock_slot::add_read(ptr<tx_lock_ctx> info, oid_t oid) {
  info->acquired_ = true;
  if (!read_.contains(info->ctx_->xid())) {
    read_count_++;
    read_.insert(info->ctx_->xid());
    POSSIBLE_UNUSED(oid);
#ifdef TEST_TRACE_LOCK
    trace_ << "read " << info->ctx_->xid() << ":" << oid << "@@";
#endif
  }

  assert_check();
}

void lock_slot::add_write(ptr<tx_lock_ctx> info, oid_t oid) {
  info->acquired_ = true;
  if (!write_.contains(info->ctx_->xid())) {
    write_count_++;
    write_.insert(info->ctx_->xid());
    BOOST_ASSERT(write_.contains(info->ctx_->xid()));
    POSSIBLE_UNUSED(oid);
#ifdef TEST_TRACE_LOCK
    trace_ << "write " << info->ctx_->xid() << ":" << oid  << " @@";
#endif
  }

  assert_check();
}

bool lock_slot::read_lock(ptr<tx_lock_ctx> info, oid_t oid) {
  BOOST_ASSERT(read_count_ == read_.size());
  auto p = acquire_read_lock(info, oid);
  bool acquire = p.first;
  if (fn_before_) {
    fn_before_(tx_op(tx_cmd_type::TX_CMD_RM_READ, info->ctx_->xid(), oid, table_id_, shard_id_,
                     tuple_id_));
  }
  if (acquire) {
    on_lock_acquired(EC::EC_OK, LOCK_READ_ROW, info->ctx_, oid);
  } else {

    LOG(trace) << " cannot acquire lock";
  }

  BOOST_ASSERT(read_count_ == read_.size());

  return acquire;
}

bool lock_slot::write_lock(ptr<tx_lock_ctx> info, oid_t oid) {
  bool acquire = false;
  if (write_count_ == 0 && read_count_ == 0) {
    // lock acquire
    add_write(info, oid);
    acquire = true;
    BOOST_ASSERT(wait_.empty());
  } else {
    // lock wait ..
    add_wait(info, oid);

  }
  if (fn_before_) {
    fn_before_(tx_op(tx_cmd_type::TX_CMD_RM_WRITE, info->ctx_->xid(), oid, table_id_,
                     shard_id_,
                     tuple_id_));
  }
  if (acquire) {
    on_lock_acquired(EC::EC_OK, LOCK_WRITE_ROW, info->ctx_, oid);
  } else {
    LOG(trace) << " cannot acquire lock";
  }

  BOOST_ASSERT(write_count_ == write_.size());
  return acquire;
}

std::pair<bool, bool> lock_slot::acquire_read_lock(ptr<tx_lock_ctx> info,
                                                   oid_t oid) {
  bool acquire = false;
  bool violate = false;
  if (write_count_ == 0) {
    // lock acquire
    add_read(info, oid);
    acquire = true;
    BOOST_ASSERT(read_count_ == read_.size());
  } else {
    add_wait(info, oid);
  }
  return std::make_pair(acquire, violate);
}

void lock_slot::unlock_gut(xid_t xid) {
  remove_lock(xid);
  assert_check();
  notify_lock_acquire();
  assert_check();
}

bool lock_slot::remove_lock(xid_t xid) {
  auto i = info_.find(xid);
  if (i != info_.end()) {
    ptr<tx_lock_ctx> ti = i->second;
    if (ti->acquired_) {
      assert_check();
      if (ti->type_ == LOCK_READ_ROW || ti->type_ == LOCK_READ_PREDICATE) {
        BOOST_ASSERT(read_count_ > 0);

        info_.erase(i);
        if (read_.contains(xid)) {
          read_count_--;
          read_.erase(xid);
        }
      } else if (ti->type_ == LOCK_WRITE_ROW) {
        BOOST_ASSERT(write_count_ > 0);
        info_.erase(i);
        if (write_.contains(xid)) {
          write_.erase(xid);
          write_count_--;
#ifdef TEST_TRACE_LOCK
          ti->trace_ << "remove write @@";
#endif
        }
      }
    } else {
      info_.erase(i);
      for (auto wait_i = wait_.begin(); wait_i < wait_.end();) {
        if (wait_i->xid_ == xid) {
          wait_i = wait_.erase(wait_i);
        } else {
          ++wait_i;
        }
      }
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
    xid_oid_t xid_oid = wait_.front();
    xid_t x = xid_oid.xid_;
    oid_t oid = xid_oid.oid_;
    auto i = info_.find(x);
    if (i == info_.end()) {
      wait_.pop_front();
      continue;
    }
    if ((i->second->type_ == LOCK_READ_ROW ||
        i->second->type_ == LOCK_READ_PREDICATE)) {
      if (write == 0 && (write_count_ == 0)) {
        if (i->second->type_ == LOCK_READ_PREDICATE) {
          if (mgr_) {
            // TODO, possible multiple same access predicate in one tx_rm
            if (mgr_->conflict(i->second->ctx_->xid(), oid,
                               *i->second->predicate_)) {
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
    } else if (i->second->type_ == LOCK_WRITE_ROW) {
      if ((read == 0 && write == 0) &&
          (read_count_ == 0 && write_count_ == 0)) {
        notify_tx.push_back(x);
        wait_.pop_front();
        write++;
      }
      break;
    }
  }

  for (xid_t x : notify_tx) {
    auto i = info_.find(x);
    if (i != info_.end()) {
      i->second->acquired_ = true;
      if (i->second->type_ == LOCK_READ_ROW ||
          i->second->type_ == LOCK_READ_PREDICATE) {
        if (!read_.contains(x)) {
          read_count_++;
          read_.insert(x);
        }
      } else if (i->second->type_ == LOCK_WRITE_ROW) {
        if (!write_.contains(x)) {
          write_count_++;
          LOG(trace) << "notify insert " << x;
          write_.insert(x);
        }
      }
      for (oid_t oid : i->second->oid_) {
        on_lock_acquired(EC::EC_OK, i->second->type_, i->second->ctx_, oid);
      }
    } else {
      BOOST_ASSERT(false);
    }
  }

  // not exists such case:
  // 1. no read or write locks are holding, but there is any waiting tx_rm;
  // 2. no write locks are holding, but there is any waiting tx_rm want to
  // acquire read lock.
  BOOST_ASSERT(
      not(read_count_ == 0 && write_count_ == 0 && (not wait_.empty())));
  BOOST_ASSERT(not(write_count_ == 0 &&
      (not wait_.empty() &&
          info_.find(wait_.front().xid_)->second->type_ == LOCK_READ_ROW)));
  assert_check();
}

void lock_slot::debug_lock(std::ostream &os) {
  std::unique_lock l(mutex_);
#ifdef TEST_TRACE_LOCK
  os << trace_.str() << std::endl;
#endif
  for (auto x : read_) {
    os << " " << x << " R lock";

    auto iter = info_.find(x);
    if (iter != info_.end()) {
#ifdef TEST_TRACE_LOCK
      os << " ctime: "
         << boost::posix_time::to_iso_extended_string(
                iter->second->time_acquire_);
#endif
      os << " oid [";
      for (oid_t oid : iter->second->oid_) {
        os << oid;
      }
      os << "] ";
#ifdef TEST_TRACE_LOCK
      os << iter->second->trace_.str() << std::endl;
#endif
    }
  }
  for (auto x : write_) {
    os << " " << x << " W lock ";
    auto iter = info_.find(x);
    if (iter != info_.end()) {
#ifdef TEST_TRACE_LOCK
      os << "ctime: "
         << boost::posix_time::to_iso_extended_string(
                iter->second->time_acquire_);
#endif
      os << " oid [";
      for (oid_t oid : iter->second->oid_) {
        os << oid;
      }
      os << "] ";
#ifdef TEST_TRACE_LOCK
      os << iter->second->trace_.str() << std::endl;
#endif
    }
  }
  for (auto x : wait_) {
    os << " " << x.xid_ << " Wait " << enum2str(info_.find(x.xid_)->second->type_);
    auto iter = info_.find(x.xid_);
    if (iter != info_.end()) {
#ifdef TEST_TRACE_LOCK
      os << "ctime: "
         << boost::posix_time::to_iso_extended_string(
                iter->second->time_acquire_);
#endif
      os << " oid [";
      for (oid_t oid : iter->second->oid_) {
        os << oid;
      }
      os << "]";
#ifdef TEST_TRACE_LOCK
      os << iter->second->trace_.str() << std::endl;
#endif
    }
  }
}

void lock_slot::build_dependency(tx_wait_set &ds) {
  std::unique_lock l(mutex_);
  for (auto x : wait_) {
    ptr<tx_wait> w = cs_new<tx_wait>(x.xid_);
    tx_build_dependency(w);
    ds.add(w);
  }
}

void lock_slot::assert_check() {
  if (write_count_ != write_.size()) {
    std::stringstream ssm;
    debug_lock(ssm);
    LOG(error) << ssm.str();
  }
  BOOST_ASSERT(write_count_ == write_.size());
  BOOST_ASSERT(read_count_ == read_.size());
  // BOOST_ASSERT(write_count_ + read_count_ + wait_.size() == info_.size());
}

result<void> lock_slot::tx_wait_for(ptr<tx_wait> ws) {
  std::unique_lock l(mutex_);
  auto i = info_.find(ws->xid());
  if (i == info_.end()) {
    return outcome::failure(EC::EC_NOT_FOUND_ERROR);
  } else {
    if (i->second->acquired_) {
      return outcome::failure(EC::EC_NOT_FOUND_ERROR);
    } else {
      tx_build_dependency(ws);
      return outcome::success();
    }
  }
}

void lock_slot::async_add_dependency(const ptr<tx_rm> &ctx) {
  ptr<lock_slot> lock = shared_from_this();
  ptr<tx_rm> rm = ctx;
  fn_wait_lock fn = [rm, lock](fn_handle_wait_set fn_handle) {
    result<void> ret = outcome::failure(EC::EC_NOT_FOUND_ERROR);
    if (!rm->is_end()) {
      ptr<tx_wait> ws = cs_new<tx_wait>(rm->xid());
      ret = lock->tx_wait_for(ws);
      if (ret) {
        fn_handle(ws);
      }
    }
    return ret;
  };
  auto fn_bind = boost::asio::bind_executor(mgr_->get_strand(), fn);
  mgr_->async_wait_lock(fn_bind);
}

std::pair<ptr<tx_lock_ctx>, bool> lock_slot::add_lock_info(lock_mode mode, const ptr<tx_rm> &ctx, oid_t oid) {
  bool acquire = false;
  ptr<tx_lock_ctx> info;
  auto iw = info_.find(ctx->xid());
  if (iw != info_.end()) {
    if (iw->second->acquired_) {
      iw->second->oid_.push_back(oid);
      acquire = true;
    } else {
      iw->second->oid_.push_back(oid);
    }
    info = iw->second;
  } else {
    info = cs_new<tx_lock_ctx>(ctx, oid, mode, false);
#ifdef TEST_TRACE_LOCK
    trace_ << std::to_string(tuple_id_) << "new lock" << ctx->xid() << ":" << oid << "@@";
#endif
    info_.insert(std::make_pair(ctx->xid(), info));
  }
  return std::make_pair(info, acquire);
}
void lock_slot::tx_build_dependency(ptr<tx_wait> w) {
  auto i = info_.find(w->xid());
  if (i != info_.end()) {
    if (i->second->type_ == LOCK_READ_ROW) {
      w->in_set_.insert(write_.begin(), write_.end());
    } else if (i->second->type_ == LOCK_WRITE_ROW) {
      w->in_set_.insert(read_.begin(), read_.end());
      w->in_set_.insert(write_.begin(), write_.end());
    }
  }
}

bool lock_slot::predicate_conflict(xid_t xid, oid_t oid, ptr<tx_rm> txn) {

  bool acquire = false;
  if (write_count_ == 0) {
    // lock acquire
    acquire = true;
  }

  auto ir = info_.find(xid);
  if (ir != info_.end()) {
    if (ir->second->acquired_) {
      acquire = true;
    }
  } else {
    if (acquire) {
      read_count_++;
    } else {
      wait_.push_back(xid_oid_t(xid, oid));
    }
    auto info = cs_new<tx_lock_ctx>(txn, oid, LOCK_READ_PREDICATE, acquire);
#ifdef TEST_TRACE_LOCK
    trace_ << "pred @@";
#endif
    info_.insert(std::make_pair(xid, info));
  }

  return acquire;
}
void lock_slot::on_lock_acquired(EC ec, lock_mode mode, ptr<tx_rm> txn,
                                 oid_t oid) {
  if (fn_after_) {
    tx_cmd_type cmd = mode == LOCK_READ_ROW || mode == LOCK_READ_PREDICATE
                      ? TX_CMD_RM_READ
                      : TX_CMD_RM_WRITE;
    fn_after_(tx_op(cmd, txn->xid(), oid, table_id_, shard_id_, tuple_id_));
  }
  txn->async_lock_acquire(ec, oid);
}