#pragma once

#include "proto/proto.h"
#include "common/id.h"
#include <vector>
#include <set>

struct calvin_epoch_ops {
  uint64_t epoch_;
  std::set<shard_id_t> shard_ids_;
  std::set<node_id_t> node_ids_;
  std::vector<ptr<tx_request>> reqs_;
};