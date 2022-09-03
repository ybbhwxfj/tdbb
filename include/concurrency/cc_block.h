#pragma once

#include "common/db_type.h"
#include "common/block.h"
#include "common/timer.h"
#include "common/config.h"
#include "common/hash_table.h"
#include "concurrency/deadlock.h"
#include "concurrency/access_mgr.h"
#include "concurrency/tx_context.h"
#include "concurrency/tx_coordinator.h"
#include "concurrency/write_ahead_log.h"
#include "concurrency/calvin_scheduler.h"
#include "concurrency/calvin_sequencer.h"
#include "concurrency/calvin_context.h"
#include "concurrency/calvin_collector.h"
#include "network/net_service.h"
#include "network/sender.h"
#include "common/msg_time.h"
#include <boost/date_time.hpp>
#include <functional>
#include <unordered_map>
#include <boost/lockfree/spsc_queue.hpp>

class cc_block : public block, public std::enable_shared_from_this<cc_block> {
 private:
#ifdef DB_TYPE_NON_DETERMINISTIC
  typedef hash_table<uint64_t, ptr<tx_context>>
      context_table_t;
#ifdef DB_TYPE_SHARE_NOTHING
  typedef hash_table<uint64_t, ptr<tx_coordinator>>
      coordinator_table_t;
#endif // DB_TYPE_SHARE_NOTHING
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_CALVIN
  typedef hash_table<uint64_t, ptr<calvin_context>>
      calvin_context_table_t;
  typedef hash_table<uint64_t, ptr<calvin_collector>>
      calvin_collector_table_t;
#endif // DB_TYPE_CALVIN
  config conf_;
  uint64_t cno_;
  bool leader_;
  uint32_t node_id_;
  std::string node_name_;
  uint32_t rlb_node_id_;
  uint32_t dsb_node_id_;
  shard_id_t neighbour_shard_;
  bool registered_;
  access_mgr *mgr_;
  net_service *service_;

  std::atomic<uint32_t> sequence_;
  ptr<write_ahead_log> wal_;
  std::map<node_id_t, bool> send_status_acked_;
  std::unordered_map<shard_id_t, node_id_t> rg_lead_;

  ptr<deadlock> deadlock_;
  std::mutex timer_send_status_mutex_;
  ptr<timer> timer_send_status_;
  std::mutex timer_send_register_mutex_;
  ptr<timer> timer_send_register_;

  notify timer_send_status_stop_;
  notify timer_send_register_stop_;
  notify timer_clean_up_stop_;

  std::recursive_mutex mutex_;
#ifdef DB_TYPE_NON_DETERMINISTIC
  std::vector<context_table_t> tx_context_;
#ifdef DB_TYPE_SHARE_NOTHING
  std::vector<coordinator_table_t> tx_coordinator_;
#endif // DB_TYPE_SHARE_NOTHING
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_CALVIN
  boost::asio::io_context::strand strand_calvin_;
  ptr<calvin_scheduler> calvin_scheduler_;
  ptr<calvin_sequencer> calvin_sequencer_;
  std::vector<calvin_context_table_t> calvin_context_;
  std::vector<calvin_collector_table_t> calvin_collector_;
#endif // DB_TYPE_CALVIN
  fn_schedule_after fn_schedule_after_;
  msg_time time_;
  boost::asio::io_context::strand strand_send_register_;
  boost::asio::io_context::strand strand_send_status_;
  boost::asio::io_context::strand timer_strand_;
 public:
  cc_block(const config &conf, net_service *service,
           fn_schedule_before fn_before,
           fn_schedule_after fn_after
  );

  virtual ~cc_block();

  virtual void on_start();

  virtual void on_stop();

  virtual void handle_debug(const std::string &path, std::ostream &os);

  template<typename T>
  result<void> handle_message(const ptr<connection> &c, message_type t, ptr<T> m) {
    auto r = ccb_handle_message(c, t, m);
    if (not r) {

    }

    return outcome::success();
  }
 private:
  template<typename T>
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const T &) {
    BOOST_ASSERT(false);
    return outcome::success();
  }

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<rlb_register_ccb_response> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_request> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<rlb_commit_entries> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<ccb_broadcast_status> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<ccb_broadcast_status_response> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<lead_status_request> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<panel_info_response> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<rlb_report_status_to_ccb> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<dsb_read_response> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<calvin_part_commit> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_rm_prepare> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_rm_ack> m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_tm_commit>);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_tm_abort>);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_tm_end>);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<dependency_set>);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<tx_enable_violate>);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<calvin_epoch>);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ptr<calvin_epoch_ack>);

  uint64_t gen_xid(uint32_t terminal_id);
  uint32_t xid_to_terminal_id(xid_t xid);

  void tick();

  void send_register();

  void send_broadcast_status(bool lead);

  void handle_register_ccb_response(const rlb_register_ccb_response &response);

  void handle_client_tx_request(const ptr<connection> &conn, const tx_request &request);

  void handle_log_entries_commit(const rlb_commit_entries &response);

  void handle_report_status(const rlb_report_status_to_ccb &);

  void handle_broadcast_status_req(const ccb_broadcast_status &);

  void handle_broadcast_status_resp(const ccb_broadcast_status_response &);

  void handle_lead_status_request(const ptr<connection> &conn, const lead_status_request &msg);

  void handle_panel_info_resp(const panel_info_response &);

#ifdef DB_TYPE_NON_DETERMINISTIC

  void create_tx_context(const ptr<connection> &conn, const tx_request &req);

  ptr<tx_context> create_tx_context_gut(xid_t xid, bool distributed, ptr<connection> conn);

  void handle_append_log_response(const rlb_commit_entries &response);

  void handle_read_data_response(ptr<dsb_read_response> response);

  void handle_non_deterministic_tx_request(const ptr<connection> &conn,
                                           const tx_request &request);

#ifdef DB_TYPE_SHARE_NOTHING

  void handle_tx_tm_request(const tx_request &req);

  ptr<tx_coordinator> create_tx_coordinator_gut(const ptr<connection> &conn, const tx_request &req);

  void create_tx_coordinator(const ptr<connection> &conn, const tx_request &req);

  void handle_tx_rm_prepare(const tx_rm_prepare &msg);

  void handle_tx_rm_ack(const tx_rm_ack &msg);

  void handle_tx_tm_commit(const tx_tm_commit &msg);

  void handle_tx_tm_abort(const tx_tm_abort &msg);

  void handle_dependency_set(const ptr<dependency_set> msg);

  void handle_tx_tm_end(const tx_tm_end &msg);
#endif
#ifdef DB_TYPE_GEO_REP_OPTIMIZE

  void handle_tx_tm_enable_violate(const tx_enable_violate &msg);

  void handle_tx_rm_enable_violate(const tx_enable_violate &msg);

#endif
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_CALVIN

  void remove_calvin_context(xid_t xid);

  ptr<calvin_context> create_calvin_context(const tx_request &req);

  void handle_calvin_tx_request(ptr<connection> conn,
                                const tx_request &request);

  void handle_calvin_log_commit(const rlb_commit_entries &msg);

  void handle_calvin_part_commit(const calvin_part_commit &msg);

  void handle_calvin_read_response(const ptr<dsb_read_response> msg);

#endif // DB_TYPE_CALVIN

  void abort_tx(xid_t xid, EC ec);

  void debug_tx(std::ostream &os, xid_t xid);

  void debug_lock(std::ostream &os);

  void debug_dependency(std::ostream &os);

  void debug_deadlock(std::ostream &os);

  static void async_run_tx_routine(
      boost::asio::io_context::strand strand,
      std::function<void()> routine);
};
