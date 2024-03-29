#pragma once
#include "concurrency/deadlock.h"
#include "concurrency/lock_pred.h"
#include "concurrency/tx.h"

class lock_mgr_trait {
public:
  virtual ~lock_mgr_trait() {}

  virtual bool conflict(xid_t xid, oid_t oid, const predicate &pred) = 0;

  virtual void async_wait_lock(fn_wait_lock fn) = 0;
};