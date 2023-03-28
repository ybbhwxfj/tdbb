#pragma once

#include "common/ctx_strand.h"
#include "common/notify.h"
#include "common/timer.h"
#include "common/define.h"
#include "common/wait_path.h"
#include "network/net_service.h"
#include <boost/asio.hpp>
#include <memory>

typedef std::function<void(xid_t)> fn_victim;
typedef std::function<void(ptr<tx_wait> ds_out)> fn_handle_wait_set;
typedef std::function<result<void>(fn_handle_wait_set)> fn_wait_lock;
class deadlock : public ctx_strand,
                 public std::enable_shared_from_this<deadlock> {
private:
  bool wait_die_;
  bool detecting_;
  bool distributed_;
  fn_victim fn_victim_;
  node_id_t send_to_node_{};
  net_service *service_;
  wait_path wait_path_;

  std::recursive_mutex mutex_;
  std::vector<ptr<dependency_set>> dep_;
  std::vector<ptr<tx_wait>> set_;
  std::vector<xid_t> removed_;
  ptr<timer> timer_;
  std::random_device rd_;
  std::uniform_int_distribution<uint64_t> rnd_;
  uint64_t deadlock_detect_ms_;
  uint64_t lock_wait_timeout_ms_;
  uint32_t num_shard_;
  notify notify_;
  uint64_t time_ms_;

public:
  deadlock(fn_victim fn,
           net_service *service,
           uint32_t num_shard,
           bool deadlock_detection,
           uint64_t detect_timeout_ms,
           uint64_t lock_wait_timeout_ms
  );

  void start();

  void stop_and_join();

  void set_next_node(node_id_t node_id_t);

  void recv_dependency(const ptr<dependency_set> ds);
  void debug_deadlock(std::ostream &os);
  // invoke by CCB thread
  void tx_finish(xid_t xid);
  void async_wait_lock(fn_wait_lock wait);

private:

  void tick();
  void add_dependency(const ptr<dependency_set> ds);

  void detect();
  void async_victim(xid_t xid);
  void add_wait_set(const ptr<tx_wait> ws);

  void gut_recv_dependency(const ptr<dependency_set> ds);

  void send_dependency(uint64_t sequence);
  void gut_tx_finish(xid_t);
};