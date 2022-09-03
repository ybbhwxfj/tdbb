#pragma once

#include <functional>
#include "common/result.hpp"
#include "common/tuple.h"
#include "common/id.h"
#include "common/lock_mode.h"
#include "proto/proto.h"
#include "common/ptr.hpp"
#include <utility>
#include <vector>

typedef std::function<void(EC)> fn_ec;
typedef std::function<void(EC, tuple_pb && /*move*/)> fn_ec_tuple;

class log_entry;

struct tx_op {
  tx_op(tx_cmd_type cmd,
        xid_t xid,
        oid_t oid) : cmd_(cmd),
                     xid_(xid),
                     oid_(oid),
                     table_id_(0),
                     tuple_id_(0) {

  }

  tx_op(tx_cmd_type cmd,
        xid_t xid,
        oid_t oid,
        table_id_t table_id,
        tuple_id_t tuple_id
  ) : cmd_(cmd),
      xid_(xid),
      oid_(oid),
      table_id_(table_id),
      tuple_id_(tuple_id) {}

  tx_cmd_type cmd_;
  xid_t xid_;
  oid_t oid_;
  table_id_t table_id_;
  tuple_id_t tuple_id_;
};

typedef std::function<void(const tx_op &)> fn_schedule_before;

typedef std::function<void(const tx_op &)> fn_schedule_after;

typedef std::function<void(uint64_t term)> fn_become_leader;
typedef std::function<void(uint64_t term)> fn_become_follower;
typedef std::function<void(EC, bool, const std::vector<ptr<log_entry>> &)> fn_commit_entries;

struct callback {
  callback() :
      schedule_before_(nullptr),
      schedule_after_(nullptr),
      become_leader_(nullptr),
      become_follower_(nullptr),
      commit_entries_(nullptr) {

  }

  fn_schedule_before schedule_before_;
  fn_schedule_after schedule_after_;
  fn_become_leader become_leader_;
  fn_become_follower become_follower_;
  fn_commit_entries commit_entries_;
};

extern callback global_callback_;

inline void set_global_callback(callback c) {
  global_callback_ = std::move(c);
}

inline const callback &get_global_callback() {
  return global_callback_;
}