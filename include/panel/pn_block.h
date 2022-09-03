#pragma once

#include "common/ptr.hpp"
#include "common/block.h"
#include "common/config.h"
#include "network/net_service.h"
#include "proto/proto.h"

// panel block cached global state, this block is only for simply our code
// and not a core part of our paper. we do not discuss this in our paper.
class pn_block : public block, public std::enable_shared_from_this<pn_block> {
 private:
  config conf_;
  ptr<net_service> service_;
  std::recursive_mutex mutex_;
  std::map<node_id_t, bool> ccb_leader_;
  std::map<node_id_t, bool> dsb_leader_;
  std::map<node_id_t, bool> rlb_leader_;
 public:
  pn_block(const config &conf, ptr<net_service> service);

  virtual void handle_debug(const std::string &path, std::ostream &os);

  virtual void on_start();

  virtual void on_stop();

  template<typename T>
  result<void> handle_message(const ptr<connection> &c, message_type t, const ptr<T> m) {
    return pnb_handle_message(c, t, m);
  }

 private:
  template<typename T>
  result<void> pnb_handle_message(const ptr<connection> &, message_type, const ptr<T> &) {
    BOOST_ASSERT(false);
    return outcome::success();
  }

  result<void> pnb_handle_message(const ptr<connection> &, message_type, const ptr<panel_report> p);

  result<void> pnb_handle_message(const ptr<connection> &, message_type, const ptr<panel_info_request> p);

  void handle_panel_info_request(const panel_info_request &msg, const ptr<connection> &conn);

  void handle_panel_report(const panel_report &msg);
};