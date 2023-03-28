#pragma once

#include "common/ctx_strand.h"
#include "common/error_code.h"
#include "common/id.h"
#include "proto/proto.h"
#include <boost/asio.hpp>

class tx_base : public ctx_strand {
private:
  xid_t xid_;
  std::atomic_bool end_;

public:
  explicit tx_base(boost::asio::io_context::strand s, xid_t xid)
      : ctx_strand(s), xid_(xid), end_(false) {}

  virtual ~tx_base() = default;

  xid_t xid() const { return xid_; }

  void set_end() { end_.store(true); }

  bool is_end() { return end_.load(); }
};

class tx_rm : public tx_base {
public:
  explicit tx_rm(boost::asio::io_context::strand s, xid_t xid)
      : tx_base(s, xid) {}

  virtual ~tx_rm() = default;

  // async lock acquire would be invoked when a tx lock has been acquire
  // this function is thread safe
  virtual void async_lock_acquire(EC ec, oid_t oid) = 0;
};