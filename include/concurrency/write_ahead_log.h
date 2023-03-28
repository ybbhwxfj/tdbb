#pragma once

#include "common/callback.h"
#include "common/tx_log.h"
#include "network/net_service.h"
#include "proto/proto.h"
#include <atomic>
#include <mutex>
#include <unordered_map>

class write_ahead_log {
private:
  std::vector<raft_log_entry> logs_;
  node_id_t node_id_;
  std::string node_name_;
  node_id_t rlb_node_id_;
  uint64_t cno_;
  net_service *service_;
  std::recursive_mutex mutex_;

public:
  write_ahead_log(node_id_t node_id, node_id_t rlb_node, net_service *service);

  void set_cno(uint64_t cno);

  void async_append(tx_log_binary &entry);

  void async_append(std::vector<tx_log_binary> &entry);
};