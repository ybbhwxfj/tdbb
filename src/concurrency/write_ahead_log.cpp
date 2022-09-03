#include "concurrency/write_ahead_log.h"
#include "common/result.hpp"
#include <boost/log/trivial.hpp>
#include <mutex>

write_ahead_log::write_ahead_log(node_id_t node_id, node_id_t rlb_node, net_service *service) :
    node_id_(node_id),
    node_name_(id_2_name(node_id)),
    rlb_node_id_(rlb_node),
    cno_(0),
    service_(service) {

}

void write_ahead_log::set_cno(uint64_t cno) {

  cno_ = cno;
}

void write_ahead_log::async_append(tx_log &entry) {
  std::vector<tx_log> log;
  log.push_back(tx_log());
  log.rbegin()->Swap(&entry);
  async_append(log);
}

void write_ahead_log::async_append(std::vector<tx_log> &entry) {

  auto req = std::make_shared<ccb_append_log_request>();
  req->set_source(node_id_);
  req->set_dest(rlb_node_id_);
  req->set_cno(cno_);
  for (tx_log &log : entry) {
    BOOST_LOG_TRIVIAL(trace) << node_name_ << " write ahead log async_append tx_rm log " << log.xid();
    req->add_logs()->Swap(&log);
  }
  result<void> sr = service_->async_send(rlb_node_id_, C2R_APPEND_LOG_REQ, req);
  if (not sr) {
    BOOST_LOG_TRIVIAL(error) << "WAL send append_log message error";
    return;
  }
}

