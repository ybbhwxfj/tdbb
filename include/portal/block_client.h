#pragma once

#include "portal/portal_client.h"
#include "common/config.h"
#include "common/panic.h"
#include "common/debug_url.h"
#include "common/json_pretty.h"
#include "common/ptr.hpp"
#include "common/set_thread_name.h"
#include "common/wait_path.h"
#include "network/client.h"
#include "network/debug_client.h"
#include "network/debug_server.h"
#include "network/message_processor.h"
#include "portal/workload.h"
#include "portal/barrier_net.h"
#include "proto/proto.h"
#include <boost/asio.hpp>
#include <boost/program_options.hpp>
#include <boost/regex.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <memory>
#include <unordered_map>
#include "common/block.h"


using boost::asio::ip::tcp;

class block_client : public block_handler, public std::enable_shared_from_this<block_client> {
private:
  bool close_server_;
  config conf_;
  ptr<boost::barrier> barrier0_;
  ptr<boost::barrier> barrier1_;
  ptr<boost::barrier> barrier2_;
  ptr<boost::barrier> barrier_warm1_;
  ptr<boost::barrier> barrier_warm2_;
  std::unordered_map<node_id_t, std::string> response_;
  ptr<debug_server> debug_;
  std::vector<node_config> conf_list_;
  std::unordered_map<node_id_t , ptr<db_client>> client_sync_;
  std::atomic_bool stopped_;
  std::mutex client_sync_mutex_;
  std::unordered_map<std::string , ptr<barrier_net>> barrier_net_;
  std::mutex barrier_net_mutex_;
  uint32_t term_id_begin_;
  uint32_t term_id_end_;
  ptr<workload> wl_;
public:
  block_client(const config &conf, bool close_server);

  virtual ~block_client();

  void run_command(const std::string &command);

  void close();

  template <typename T>
  result<void> handle_message(const ptr<connection> c, message_type t,
                              const ptr<T> m) {
    return client_handle_message(c, t, m);
  }
private:
  void send_close_request();
  void output_result_thread(ptr<workload> &wl);
  void load_data();
  void run_benchmark();
  void load_data_thread(node_id_t node_id, const ptr<workload> &wl);
  void run_benchmark_thread(shard_id_t sd_id, uint32_t terminal_id,
                            ptr<workload> &wl, bool log);

  void client_handle_debug(const std::string &path, std::ostream &os);
  void debug_deadlock(std::ostream &os);
  void run_sync_thread();
  void connect_sync();
  void bench_stop();
  void barrier_wait(std::string name, std::string payload = "");

  void barrier_done(std::string name, std::string payload, node_id_t node_id);

  template <typename T>
  result<void> client_handle_message(const ptr<connection>, message_type,
                                     const ptr<T>) {
    BOOST_ASSERT(false);
    return outcome::success();
  }

  result<void> client_handle_message(const ptr<connection>, message_type, const ptr<client_sync>);
  result<void> client_handle_message(const ptr<connection>, message_type, const ptr<client_bench_stop>);
  void stop_sync();

  void setup_terminal_id();
  uint32_t client_num_terminal();

  ptr<barrier_net> get_barrier_net(const std::string &name);
};

