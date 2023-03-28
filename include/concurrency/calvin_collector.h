#pragma once

#include "common/db_type.h"

#ifdef DB_TYPE_CALVIN

#include "common/id.h"
#include "concurrency/tx.h"
#include "network/connection.h"
#include "network/net_service.h"
#include "proto/proto.h"
#include <map>
#include <mutex>
#include <set>
#include <unordered_map>

class calvin_collector : public tx_base {
private:
  xid_t xid_;
  ptr<connection> conn_;
  net_service *service_;
  std::set<shard_id_t> shard_;
  std::set<shard_id_t> commit_shard_;
  std::unordered_map<uint32_t, tx_operation> ops_;
  std::recursive_mutex mutex_;
  std::unordered_map<shard_id_t, node_id_t> lead_node_;
  ptr<tx_request> request_;
  std::stringstream trace_message_;

public:
  calvin_collector(boost::asio::io_context::strand s, xid_t xid,
                   ptr<connection> client_conn, net_service *service,
                   const ptr<tx_request> req);

  bool part_commit(const ptr<calvin_part_commit> msg);

  const tx_request &request() const { return *request_; }

  void debug_tx(std::ostream &os) const;

private:
  void respond();
};

#endif // DB_TYPE_CALVIN