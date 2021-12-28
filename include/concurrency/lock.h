#pragma once

#include "common/id.h"
#include "common/ptr.hpp"
#include "common/enum_str.h"
#include "concurrency/tx.h"
#include "common/lock_mode.h"
#include "concurrency/lock_pred.h"

template<>
enum_strings<lock_mode>::e2s_t enum_strings<lock_mode>::enum2str;

struct tx_info {
  tx_info(ptr<tx> ctx, oid_t oid, lock_mode type, bool acquire)
      : ctx_(ctx), type_(type), violate_(false), acquired_(acquire) {
    oid_.push_back(oid);
  };

  ptr<tx> ctx_;
  std::vector<oid_t> oid_;
  lock_mode type_;
  bool violate_;
  bool acquired_;
  ptr<predicate> predicate_;
};

class lock_item {
private:
  lock_mode lock_type_;
  uint64_t xid_;
  uint32_t oid_;
  table_id_t table_id_;
  tuple_id_t key_;
  predicate predicate_;
public:
  lock_item(xid_t xid, uint32_t oid, lock_mode type, table_id_t table_id, predicate key)
      : lock_type_(type), xid_(xid), oid_(oid), table_id_(table_id), predicate_(key) {}
  xid_t xid() const { return xid_; }

  uint32_t oid() const { return oid_; }

  lock_mode type() const { return lock_type_; }

  const predicate &get_predicate() const { return predicate_; }

  tuple_id_t key() const { return predicate_.key_; }

  table_id_t table_id() const { return table_id_; }
};
