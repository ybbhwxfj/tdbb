#pragma once
#include "common/db_type.h"
#ifdef DB_TYPE_CALVIN
#include "proto/proto.h"
#include "common/ptr.hpp"
#include "concurrency/calvin_epoch_ops.h"
#include "concurrency/calvin_context.h"
#include "concurrency/write_ahead_log.h"
#include "concurrency/access_mgr.h"
#include "common/hash_table.h"
#include <vector>

class calvin_scheduler {
private:
  fn_calvin_context_find fn_find_;
  config conf_;
  node_id_t node_id_;
  access_mgr *access_mgr_;
  write_ahead_log *wal_;
  net_service *service_;
  std::string trace_message_;
public:
  calvin_scheduler(
      fn_calvin_context_find fn_find,
      const config &conf,
      access_mgr *lm,
      write_ahead_log *wal,
      net_service *service);

  void schedule(const ptr<calvin_epoch_ops> &);
  void tx_op_done(const ptr<calvin_context> &tx, const tx_operation &op);
  void debug_tx(std::ostream &os);
private:
  void send_ack(uint64_t epoch, const std::set<node_id_t> &ids);
  ptr<calvin_context> create_or_find(std::unordered_map<xid_t, ptr<calvin_context>> &ctx_set,
                                     const ptr<tx_request> &tx);
};

#endif // DB_TYPE_CALVIN