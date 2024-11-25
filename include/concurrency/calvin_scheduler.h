#pragma once

#include "common/db_type.h"

#ifdef DB_TYPE_CALVIN

#include "common/hash_table.h"
#include "common/ptr.hpp"
#include "concurrency/lock_mgr_global.h"
#include "concurrency/calvin_context.h"
#include "concurrency/calvin_epoch_ops.h"
#include "concurrency/write_ahead_log.h"
#include "proto/proto.h"
#include <vector>

class calvin_scheduler {
private:
  fn_calvin_context_find fn_find_;
  config conf_;
  node_id_t node_id_;
  lock_mgr_global *access_mgr_;
  write_ahead_log *wal_;
  net_service *service_;
  std::string trace_message_;

public:
  calvin_scheduler(fn_calvin_context_find fn_find, const config &conf,
                   lock_mgr_global *lm, write_ahead_log *wal, net_service *service);

  void schedule(const ptr<calvin_epoch_ops> &);

  void tx_op_done(const ptr<calvin_context> &tx, const tx_operation &op);

  void debug_tx(std::ostream &os);

private:
  void send_ack(uint64_t epoch, const std::set<node_id_t> &ids);

  ptr<calvin_context>
  create_or_find(std::unordered_map<xid_t, ptr<calvin_context>> &ctx_set,
                 const ptr<tx_request> &tx, bool read_only);
};

#endif // DB_TYPE_CALVIN