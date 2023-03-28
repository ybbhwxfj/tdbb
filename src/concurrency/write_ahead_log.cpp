#include "concurrency/write_ahead_log.h"
#include "common/define.h"
#include "common/logger.hpp"
#include "common/result.hpp"
#include <mutex>

write_ahead_log::write_ahead_log(node_id_t node_id, node_id_t rlb_node,
                                 net_service *service)
    : node_id_(node_id), node_name_(id_2_name(node_id)), rlb_node_id_(rlb_node),
      cno_(0), service_(service) {}

void write_ahead_log::set_cno(uint64_t cno) { cno_ = cno; }

void write_ahead_log::async_append(tx_log_binary &entry) {
  std::vector<tx_log_binary> log;
  log.push_back(tx_log_binary());
  log.rbegin()->swap(entry);
  async_append(log);
}

void write_ahead_log::async_append(std::vector<tx_log_binary> &entry) {
  auto req = cs_new<ccb_append_log_request>();
  req->set_source(node_id_);
  req->set_dest(rlb_node_id_);
  req->set_cno(cno_);
  BOOST_ASSERT(cno_ != 0);
  size_t total_size = 0;
  for (tx_log_binary &log : entry) {
    total_size += log_buffer::add_header_size(log.size());
  }
  std::string *repeated_tx_logs = req->mutable_repeated_tx_logs();
  repeated_tx_logs->resize(total_size);
  size_t offset = 0;
  for (tx_log_binary &log : entry) {
    log_buffer::format(const_cast<char *>(repeated_tx_logs->data()) + offset,
                       repeated_tx_logs->size() - offset, log.data(),
                       log.size(), 0, node_id_);
    offset += log_buffer::add_header_size(log.size());
  }

#ifdef TEST_APPEND_TIME
  req->set_debug_send_ts(steady_clock_ms_since_epoch());
#endif
  result<void> sr = service_->async_send(rlb_node_id_, C2R_APPEND_LOG_REQ, req, true);
  if (not sr) {
    LOG(error) << "WAL send append_log message error";
    return;
  }
}
