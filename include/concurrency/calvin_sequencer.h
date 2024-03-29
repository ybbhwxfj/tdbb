#pragma once

#include "common/db_type.h"

#ifdef DB_TYPE_CALVIN

#include "common/enum_str.h"
#include "common/ptr.hpp"
#include "concurrency/calvin_context.h"
#include "concurrency/calvin_epoch_ops.h"
#include "network/net_service.h"
#include "proto/proto.h"
#include <mutex>

typedef std::function<void(const ptr<calvin_epoch_ops> ops)> fn_scheduler;

enum epoch_state {
  EPOCH_IDLE,
  EPOCH_PENDING,
};

template<>
enum_strings<epoch_state>::e2s_t enum_strings<epoch_state>::enum2str;

class calvin_sequencer : public std::enable_shared_from_this<calvin_sequencer> {
private:
  struct local_epoch_ctx {
    local_epoch_ctx() : epoch_(0) {}

    uint64_t epoch_;
    std::set<shard_id_t> ack_;
    epoch_state state_;
    std::stringstream ssm_;
  };

  config conf_;

  uint64_t epoch_;
  net_service *service_;
  fn_scheduler scheduler_;
  epoch_state state_;
  std::recursive_mutex mutex_;
  std::map<shard_id_t, std::map<xid_t, ptr<tx_request>>> tx_request_next_epoch_;
  std::unordered_map<uint64_t, ptr<local_epoch_ctx>> local_epoch_;
  // TODO ... retrieve current leader node
  std::unordered_map<shard_id_t, node_id_t> node_id_;
  std::map<uint64_t, ptr<calvin_epoch_ops>> epoch_ctx_receive_;
  std::stringstream trace_message_;
  bool is_lead_;
  bool is_priority_leader_;
  bool start_;
  boost::asio::io_context::strand strand_;

public:
  calvin_sequencer(const config &conf, boost::asio::io_context::strand strand,
                   net_service *service, fn_scheduler fn);

  void start();

  void tick();

  void epoch_broadcast_request();

  void handle_tx_request(const tx_request &tx);

  void handle_epoch(const calvin_epoch &msg);

  void handle_epoch_ack(const calvin_epoch_ack &msg);

  void update_local_lead_state(bool leader);

  void debug_tx(std::ostream &os);

private:
  void reorder_request(const ptr<calvin_epoch_ops> &ctx);

  void schedule(const ptr<calvin_epoch_ops> &ops);
};

#endif // DB_TYPE_CALVIN