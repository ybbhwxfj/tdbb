#pragma once

#include "common/block.h"
#include "network/net_service.h"
#include "common/config.h"
#include <boost/asio.hpp>
#include "common/ptr.hpp"
#include "common/callback.h"
#include "proto/proto.h"
#include "store/store.h"

using boost::asio::steady_timer;

class ds_block : public block, public std::enable_shared_from_this<ds_block> {
private:
  config conf_;
  net_service *service_;
  uint32_t node_id_;
  std::string node_name_;
  uint32_t rlb_node_id_;
  std::recursive_mutex register_mutex_;
  ptr<steady_timer> timer_send_register_;
  bool registered_;
  uint32_t cno_;
  ptr<store> store_;
  std::vector<uint32_t> wid_;
  schema_mgr sch_mgr_;
  std::unordered_map<table_id_t, tuple_pb> default_tuple_;
  std::recursive_mutex mutex_;
public:
  ds_block(const config &conf, net_service *service);

  virtual ~ds_block() {};

  virtual void on_start();
  virtual void on_stop();

  virtual void handle_debug(const std::string &path, std::ostream &os);

  template<typename T>
  result<void> handle_message(const ptr<connection> &c, message_type t, const T &m) {
    std::scoped_lock l(mutex_);
    return dsb_handle_message(c, t, m);
  }
private:
  template<typename T>
  result<void> dsb_handle_message(const ptr<connection> &, message_type, const T &) {
    BOOST_ASSERT(false);
    return outcome::success();
  }

  result<void> dsb_handle_message(const ptr<connection> &, message_type, const client_load_data_request &);
  result<void> dsb_handle_message(const ptr<connection> &, message_type, const rlb_register_dsb_response &);
  result<void> dsb_handle_message(const ptr<connection> &, message_type, const ccb_read_request &);
  result<void> dsb_handle_message(const ptr<connection> &, message_type, const replay_to_dsb_request &);
  void handle_load_data_request(const client_load_data_request &, ptr<connection> conn);
  void handle_register_dsb_response(const rlb_register_dsb_response &response);
  void handle_read_data(const ccb_read_request &request);
  void handle_replay_to_dsb(const replay_to_dsb_request &msg);

  tuple_pb gen_tuple(table_id_t table_id);
  void send_register();

  void load_data();

  void load_item();
  void load_customer();
  void load_stock();
  void load_warehouse();
  void load_district();
  void load_order();

  void read_request();

};
