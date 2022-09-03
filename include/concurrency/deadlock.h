#pragma once

#include "common/wait_path.h"
#include "common/ctx_strand.h"
#include "common/notify.h"
#include <boost/asio.hpp>
#include "network/net_service.h"
#include "common/timer.h"

typedef std::function<void(xid_t)> fn_victim;
typedef std::function<result<void>(tx_wait_set &ds_out)> fn_wait_lock;

class deadlock : public ctx_strand, public std::enable_shared_from_this<deadlock> {
 private:
  bool detecting_;
  bool distributed_;
  fn_victim fn_victim_;
  node_id_t send_to_node_{};
  net_service *service_;
  wait_path wait_path_;

  std::recursive_mutex mutex_;
  std::vector<ptr<dependency_set>> dep_;
  std::vector<ptr<tx_wait_set>> set_;
  std::vector<xid_t> removed_;
  std::mutex timer_mutex_;
  ptr<timer> timer_;
  std::random_device rd_;
  std::mt19937 g_;
  std::uniform_int_distribution<uint64_t> rnd_;
  uint32_t num_shard_;
  notify notify_;
 public:
  deadlock(fn_victim fn, net_service *service, uint32_t num_shard);

  void tick();

  void stop_and_join();

  void set_next_node(node_id_t node_id_t);

  void recv_dependency(const ptr<dependency_set> ds);
  void debug_deadlock(std::ostream &os);
  // invoke by CCB thread
  void tx_finish(xid_t xid);
  void async_wait_lock(fn_wait_lock fn);
 private:
  void add_dependency(const ptr<dependency_set> ds);

  void detect();

  void add_wait_set(const ptr<tx_wait_set> ws);

  void gut_recv_dependency(const ptr<dependency_set> ds);

  void send_dependency(uint64_t sequence);
  void gut_tx_finish(xid_t);
};