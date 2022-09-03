#pragma once

#include "common/id.h"
#include "proto/proto.h"
#include "common/ctx_strand.h"
#include "common/error_code.h"
#include <boost/asio.hpp>

class tx_base : public ctx_strand {
 private:
  xid_t xid_;
 public:
  explicit tx_base(boost::asio::io_context::strand s, xid_t xid) : ctx_strand(s), xid_(xid) {}

  virtual ~tx_base() = default;

  xid_t xid() const { return xid_; }

};

class tx_rm : public tx_base {
 public:
  explicit tx_rm(boost::asio::io_context::strand s, xid_t xid) : tx_base(s, xid) {}

  virtual ~tx_rm() = default;

  // async lock acquire would be invoked when a tx lock has been acquire
  // this function is thread safe
  virtual void async_lock_acquire(EC ec, oid_t oid) = 0;
};