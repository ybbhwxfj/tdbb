#include "concurrency/calvin_collector.h"

#include <utility>

#ifdef DB_TYPE_CALVIN

calvin_collector::calvin_collector(
    xid_t xid,
    ptr<connection> client_conn,
    const tx_request &req)
    :
    xid_(xid),
    conn_(std::move(client_conn)),
    request_(req) {
  for (const auto &op: req.operations()) {
    shard_.insert(op.rg_id());
  }
  request_.set_xid(xid);
  for (tx_operation &op: *request_.mutable_operations()) {
    op.set_xid(xid);
  }
}

bool calvin_collector::part_commit(const calvin_part_commit &msg) {
  trace_message_ += "recv PC;";
  commit_shard_.insert(TO_RG_ID(msg.source()));
  for (const auto &op: msg.response()) {
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
  trace_message_ = "resp;";
  tx_response response;
  for (const auto &op: request_.operations()) {
    auto i = ops_.find(op.operation_id());
    if (i != ops_.end()) {
      response.mutable_operations()->Add(tx_operation(i->second));
    }
  }
  response.set_error_code(EC::EC_OK);
  result<void> r = conn_->async_send(CLIENT_TX_RESP, response);
  if (!r) {
    BOOST_LOG_TRIVIAL(error) << "send client response error " << r.error().message();
  }
}

void calvin_collector::debug_tx(std::ostream &os) const {
  os << "calvin TM: " << xid_ << " trace: " << trace_message_ << std::endl;
}
#endif // DB_TYPE_CALVIN