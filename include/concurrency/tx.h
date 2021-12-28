#pragma once

#include "common/id.h"
#include "proto/proto.h"
#include "common/error_code.h"

class tx {
private:
  xid_t xid_;
public:
  explicit tx(xid_t xid) : xid_(xid) {}
  virtual ~tx() = default;
  [[nodiscard]] virtual xid_t xid() const { return xid_; }
  virtual void lock_acquire(EC ec, oid_t oid) = 0;
};