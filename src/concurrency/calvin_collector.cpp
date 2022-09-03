#include "concurrency/calvin_collector.h"
#include "common/define.h"
#include <utility>

#ifdef DB_TYPE_CALVIN

calvin_collector::calvin_collector(
    boost::asio::io_context::strand s,
    xid_t xid,
    ptr<connection> client_conn,
    net_service *service,
    const tx_request &req)
    :
    tx_base(s, xid),
    xid_(xid),
    conn_(std::move(client_conn)),
    service_(service),
    request_(req) {
  for (const auto &op : req.operations()) {
    shard_.insert(op.rg_id());
  }
  request_.set_xid(xid);
  for (tx_operation &op : *request_.mutable_operations()) {
    op.set_xid(xid);
  }
}

bool calvin_collector::part_commit(const calvin_part_commit &msg) {
#ifdef TX_TRACE
  trace_message_ += "recv PC;";
#endif
  commit_shard_.insert(TO_RG_ID(msg.source()));
  for (const auto &op : msg.response()) {
    ops_.insert(std::make_pair(op.operation_id(), op));
  }
  if (commit_shard_.size() == shard_.size()) {
    // response message ...
    respond();
    return true;
  } else {
    return false;
  }
}

void calvin_collector::respond() {
#ifdef TX_TRACE
  trace_message_ = "resp;";
#endif
  auto response = std::make_shared<tx_response>();

  for (const auto &op : request_.operations()) {
    auto i = ops_.find(op.operation_id());
    if (i != ops_.end()) {
      response->mutable_operations()->Add(tx_operation(i->second));
    }
  }
  response->set_error_code(EC::EC_OK);
  service_->conn_async_send(conn_, CLIENT_TX_RESP, response);
}

void calvin_collector::debug_tx(std::ostream &os) const {
  os << "calvin TM: " << xid_ << " trace: " << trace_message_ << std::endl;
}
#endif // DB_TYPE_CALVIN