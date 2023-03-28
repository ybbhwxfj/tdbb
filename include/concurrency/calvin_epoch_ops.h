#pragma once

#include "common/db_type.h"

#ifdef DB_TYPE_CALVIN

#include "common/id.h"
#include "proto/proto.h"
#include <set>
#include <vector>

struct calvin_epoch_ops {
  uint64_t epoch_;
  std::set<shard_id_t> shard_ids_;
  std::set<node_id_t> node_ids_;
  std::vector<ptr<tx_request>> reqs_;
};

#endif
