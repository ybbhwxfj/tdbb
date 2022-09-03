#pragma once

#include "proto/proto.h"
#include <vector>
#include "common/ptr.hpp"
#include "common/slice.h"
#include "replog/log_write_option.h"
#include "proto/proto.h"

enum log_entry_type {
  TX_LOG_WRITE,
  TX_LOG_END,
  TX_LOG_REPLAY,
  RAFT_LOG,
};

class tx_log_index {
 private:
  uint64_t xid_;
  uint64_t index_;
  uint32_t type_;
 public:
  tx_log_index(uint64_t index) : xid_(0), index_(index), type_(RAFT_LOG) {

  }

  tx_log_index(const char *data, size_t size) {
    if (size != sizeof(tx_log_index)) {
      abort();
    }
    memcpy((void *) this, data, sizeof(tx_log_index));
  }

  tx_log_index(uint64_t xid, uint64_t index, uint32_t type) :
      xid_(xid), index_(index), type_(type) {

  }

  uint64_t xid() const {
    return xid_;
  }

  uint64_t index() const {
    return index_;
  }

  uint32_t type() const {
    return type_;
  }

  static tx_log_index tx_max_index(uint64_t xid) {
    return tx_log_index(xid, UINT64_MAX, UINT32_MAX);
  }

  static tx_log_index tx_min_index(uint64_t xid) {
    return tx_log_index(xid, 0, 0);
  }

  static tx_log_index state_index() {
    return tx_log_index(0, 0, 0);
  }

  bool equal(const tx_log_index &i) {
    return i.xid_ == xid_ && i.index_ == xid_ && i.type_ == type_;
  }

  int compare(const tx_log_index &i) {
    if (xid_ < i.xid_) {
      return -1;
    } else if (xid_ > i.xid_) {
      return 1;
    } else {
      if (index_ < i.index_) {
        return -1;
      } else if (index_ > i.index_) {
        return 1;
      } else {
        if (type_ < i.type_) {
          return -1;
        } else if (type_ > i.type_) {
          return 1;
        } else {
          return 0;
        }
      }
    }
  }

  const char *data() const {
    return (const char *) this;
  }

  size_t size() const {
    return sizeof(*this);
  }
};

typedef std::function<void(const slice &)> fn_state;
typedef std::function<void(const tx_log_index &, const slice &)> fn_tx_log;

class log_service {
 public:
  log_write_option write_option() { return log_write_option(); }

  virtual void commit_log(const std::vector<ptr<log_entry>> &, const log_write_option &) = 0;

  virtual void write_log(const std::vector<ptr<log_entry>> &, const log_write_option &) = 0;

  virtual void write_state(const ptr<log_state> &, const log_write_option &) = 0;

  virtual result<ptr<log_entry>> get_log_entry(uint64_t index) = 0;

  virtual void retrieve_state(fn_state fn1) = 0;

  virtual void retrieve_log(fn_tx_log fn2) = 0;

  ~log_service() {};
};