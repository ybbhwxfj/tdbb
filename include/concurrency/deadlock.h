#pragma once

#include "common/wait_path.h"
#include <boost/asio.hpp>
#include "network/net_service.h"

typedef std::function<void(xid_t)> fn_victim;
typedef std::function<result<void>(tx_wait_set &ds_out)> fn_wait_lock;
class deadlock : public std::enable_shared_from_this<deadlock> {
private:
  bool detecting_;
  bool distributed_;
  fn_victim fn_victim_;
  node_id_t send_to_node_{};
  net_service *service_;
  wait_path wait_path_;

  std::recursive_mutex mutex_;
  std::vector<dependency_set> dep_;
  std::vector<tx_wait_set> set_;
  std::vector<xid_t> removed_;
  ptr<boost::asio::steady_timer> timer_;
public:
  deadlock(fn_victim fn, net_service *service, uint32_t num_shard);
  void tick();
  void stop();
  void add_wait_set(tx_wait_set &ws);
  void add_dependency(const dependency_set &ds);
  void detect();
  void tx_finish(xid_t xid);
  void set_next_node(node_id_t node_id_t);
  void recv_dependency(const dependency_set &ds);
  void debug_deadlock(std::ostream &os);
  void async_wait_lock(fn_wait_lock fn);
private:
  void send_dependency();
};