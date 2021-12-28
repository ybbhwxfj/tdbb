#include "panel/pn_block.h"

#include <utility>
#include "common/message.h"
#include "common/debug_url.h"

pn_block::pn_block(const config &conf, ptr<net_service> service)
    : conf_(conf), service_(std::move(service)) {

}

void pn_block::handle_debug(const std::string &url, std::ostream &os) {
  if (boost::regex_match(url, url_panel)) {
    os << "RLB:" << std::endl;
    for (auto kv: rlb_leader_) {
      os << "  node: " << id_2_name(kv.first) << " " << (kv.second ? "leader" : "follower") << std::endl;
    }
    os << "CCB:" << std::endl;
    for (auto kv: ccb_leader_) {
      os << "  node: " << id_2_name(kv.first) << " " << (kv.second ? "leader" : "follower") << std::endl;
    }
    os << "DSB:" << std::endl;
    for (auto kv: dsb_leader_) {
      os << "  node: " << id_2_name(kv.first) << " " << (kv.second ? "leader" : "follower") << std::endl;
    }
  }
}

void pn_block::on_start() {

}
void pn_block::on_stop() {

}

void pn_block::handle_panel_report(const panel_report &msg) {
  switch (msg.report_type()) {
  case RLB_NEW_TERM: {
    rlb_leader_[msg.source()] = msg.lead();
    break;
  }
  case CCB_REGISTERED_RLB: {
    ccb_leader_[msg.source()] = msg.lead();
    break;
  }
  case DSB_REGISTERED_RLB: {
    dsb_leader_[msg.source()] = msg.lead();
    break;
  }
  default:break;
  }
}

result<void> pn_block::pnb_handle_message(const ptr<connection> &, message_type, const panel_report &m) {
  handle_panel_report(m);
  return outcome::success();
}

result<void> pn_block::pnb_handle_message(const ptr<connection> &c, message_type, const panel_info_request &m) {
  handle_panel_info_request(m, c);
  return outcome::success();
}

void pn_block::handle_panel_info_request(const panel_info_request &msg, const ptr<connection> &conn) {
  panel_info_response res;
  res.set_dest(msg.source());
  res.set_source(conf_.node_id());
  for (auto n: ccb_leader_) {
    if (n.second) {
      res.mutable_ccb_leader()->Add(n.first);
    } else {
      res.mutable_ccb_follower()->Add(n.first);
    }
  }
  for (auto n: rlb_leader_) {
    if (n.second) {
      res.mutable_rlb_leader()->Add(n.first);
    } else {
      res.mutable_rlb_follower()->Add(n.first);
    }
  }
  for (auto n: dsb_leader_) {
    if (n.second) {
      res.mutable_dsb_leader()->Add(n.first);
    } else {
      res.mutable_dsb_follower()->Add(n.first);
    }
  }

  if (msg.block_type() == pb_block_type::PB_BLOCK_CCB) {
    auto r = service_->async_send(res.dest(), PANEL_INFO_RESP_TO_CCB, res);
    if (not r) {
      BOOST_LOG_TRIVIAL(error) << "async send report panel info to ccb error";
    }
  } else if (msg.block_type() == pb_block_type::PB_BLOCK_CLIENT) {
    auto r = conn->async_send(PANEL_INFO_RESP_TO_CLIENT, res);
    if (not r) {
      BOOST_LOG_TRIVIAL(error) << "async send report panel info to client error";
    }
  } else {
    BOOST_ASSERT(false);
  }
}