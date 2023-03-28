#pragma once

#include "common/define.h"
#include "common/enum_str.h"
#include "common/id.h"
#include "common/lock_mode.h"
#include "common/ptr.hpp"
#include "concurrency/lock_pred.h"
#include "concurrency/tx.h"
#include <boost/date_time.hpp>

template<> enum_strings<lock_mode>::e2s_t enum_strings<lock_mode>::enum2str;

struct tx_lock_ctx {
  tx_lock_ctx(ptr<tx_rm> ctx, oid_t oid, lock_mode type, bool acquire)
      : ctx_(ctx), type_(type), acquired_(acquire) {
    oid_.push_back(oid);
#ifdef TEST_TRACE_LOCK
    time_acquire_ = boost::posix_time::microsec_clock::universal_time();
#endif
  };

  ~tx_lock_ctx() {}

  ptr<tx_rm> ctx_;
  std::vector<oid_t> oid_;
  lock_mode type_;
  bool acquired_;
  ptr<predicate> predicate_;
#ifdef TEST_TRACE_LOCK
  std::stringstream trace_;
  boost::posix_time::ptime time_acquire_;
#endif
};

class lock_item {
private:
  lock_mode lock_type_;
  uint64_t xid_;
  uint32_t oid_;
  table_id_t table_id_;
  shard_id_t shard_id_;
  predicate predicate_;

public:
  lock_item(xid_t xid, uint32_t oid, lock_mode type, table_id_t table_id,
            shard_id_t shard_id,
            predicate key)
      : lock_type_(type), xid_(xid), oid_(oid), table_id_(table_id),
        shard_id_(shard_id),
        predicate_(key) {}

  xid_t xid() const { return xid_; }

  uint32_t oid() const { return oid_; }

  lock_mode type() const { return lock_type_; }

  const predicate &get_predicate() const { return predicate_; }

  tuple_id_t key() const { return predicate_.key_; }

  table_id_t table_id() const { return table_id_; }

  shard_id_t shard_id() const { return shard_id_; }
};
