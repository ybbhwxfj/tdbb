#pragma once

#include "common/db_type.h"
#include "common/block.h"
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
#include <boost/date_time.hpp>
#include <functional>
#include <unordered_map>
#include <boost/lockfree/spsc_queue.hpp>

class cc_block : public block, public std::enable_shared_from_this<cc_block> {
private:
#ifdef DB_TYPE_NON_DETERMINISTIC
  typedef hash_table<uint64_t, ptr<tx_context>, std::hash<uint64_t>,
                     std::equal_to<uint64_t>>
      context_table_t;
#ifdef DB_TYPE_SHARE_NOTHING
  typedef hash_table<uint64_t, ptr<tx_coordinator>, std::hash<uint64_t>,
                     std::equal_to<uint64_t>>
      coordinator_table_t;
#endif // DB_TYPE_SHARE_NOTHING
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_CALVIN
  typedef hash_table<uint64_t, ptr<calvin_context>, std::hash<uint64_t>,
                     std::equal_to<>>
      calvin_context_table_t;
  typedef hash_table<uint64_t, ptr<calvin_collector>, std::hash<uint64_t>,
                     std::equal_to<>>
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

  std::recursive_mutex register_mutex_;
  std::atomic<uint32_t> sequence_;
  ptr<write_ahead_log> wal_;
  std::map<node_id_t, bool> send_status_acked_;
  std::unordered_map<shard_id_t, node_id_t> rg_lead_;

  ptr<deadlock> deadlock_;
  ptr<boost::asio::steady_timer> timer_send_status_;
  ptr<boost::asio::steady_timer> timer_send_register_;
  ptr<boost::asio::steady_timer> timer_clean_up_;
  std::recursive_mutex mutex_;
#ifdef DB_TYPE_NON_DETERMINISTIC
  context_table_t tx_context_;
#ifdef DB_TYPE_SHARE_NOTHING
  coordinator_table_t tx_coordinator_;
#endif // DB_TYPE_SHARE_NOTHING
#endif // #ifdef DB_TYPE_NON_DETERMINISTIC
#ifdef DB_TYPE_CALVIN
  ptr<calvin_scheduler> calvin_scheduler_;
  ptr<calvin_sequencer> calvin_sequencer_;
  calvin_context_table_t calvin_context_;
  calvin_collector_table_t calvin_collector_;
#endif // DB_TYPE_CALVIN
  fn_schedule_after fn_schedule_after_;
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
  result<void> handle_message(const ptr<connection> &c, message_type t, const T &m) {
    std::scoped_lock l(mutex_);
    return ccb_handle_message(c, t, m);
  }

  result<void> handle_message(message_type, const rlb_commit_entries &response);
private:
  template<typename T>
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const T &) {
    BOOST_ASSERT(false);
    return outcome::success();
  }

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const rlb_register_ccb_response &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const tx_request &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const rlb_commit_entries &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ccb_broadcast_status &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const ccb_broadcast_status_response &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const lead_status_request &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const panel_info_response &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const rlb_report_status_to_ccb &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const dsb_read_response &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const calvin_part_commit &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const tx_rm_prepare &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const tx_rm_ack &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const tx_tm_commit &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const tx_tm_abort &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const dependency_set &m);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const tx_enable_violate &m);

  result<void> ccb_handle_message(const ptr<connection> &, message_type, const calvin_epoch &);
  result<void> ccb_handle_message(const ptr<connection> &, message_type, const calvin_epoch_ack &);

  uint64_t gen_xid();
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
  void handle_append_log_response(const rlb_commit_entries &response);
  void handle_read_data_response(const dsb_read_response &response);
  void timeout_clean_up();
  void timeout_clean_up_tx();
  void handle_non_deterministic_tx_request(const ptr<connection> &conn,
                                           const tx_request &request);
#ifdef DB_TYPE_SHARE_NOTHING
  void handle_tx_tm_request(const tx_request &req);
  void create_tx_coordinator(const ptr<connection> &conn, const tx_request &req);
  void handle_tx_rm_prepare(const tx_rm_prepare &msg);
  void handle_tx_rm_ack(const tx_rm_ack &msg);
  void handle_tx_tm_commit(const tx_tm_commit &msg);
  void handle_tx_tm_abort(const tx_tm_abort &msg);
  void handle_dependency_set(const dependency_set &msg);
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
  void handle_calvin_read_response(const dsb_read_response &msg);
#endif // DB_TYPE_CALVIN
  void abort_tx(xid_t xid, EC ec);
  void debug_tx(std::ostream &os, xid_t xid);
  void debug_lock(std::ostream &os);
  void debug_dependency(std::ostream &os);
  void debug_deadlock(std::ostream &os);
};
