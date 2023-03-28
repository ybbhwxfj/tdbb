#pragma once

#include <optional>
#include <unordered_map>
#include "common/id.h"
#include "common/panic.h"

inline node_id_t node_id_of_shard(
    shard_id_t shard_id,
    const std::optional<node_id_t> &opt_node_id,
    const std::unordered_map<shard_id_t, node_id_t> &shard2node) {
  if (opt_node_id.has_value()) {
    return opt_node_id.value();
  } else {
    auto i = shard2node.find(shard_id);
    if (i==shard2node.end()) {
      PANIC(boost::format("could not find shard id %d")%shard_id);
      return 0;
    } else {
      return i->second;
    }
  }
}