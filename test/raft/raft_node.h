#pragma once

#include <boost/test/unit_test.hpp>
#include "common/ptr.hpp"
#include "network/net_service.h"
#include "network/sock_server.h"
#include "network/message_handler.h"
#include "raft/state_machine.h"
#include "common/gen_config.h"
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <iostream>
#include <ostream>
#include <memory>
#include "replog/log_service.h"
using namespace boost;
using namespace std;

class raft_test_context;

class raft_node : public std::enable_shared_from_this<raft_node>, public log_service {
 public:
  std::recursive_mutex mutex_;
  raft_node(const config &conf, const std::string &case_name);
  ~raft_node() {}
  void start(raft_test_context *ctx);

  virtual void commit_log(const std::vector<ptr<log_entry>> &, const log_write_option &);
  virtual void write_log(const std::vector<ptr<log_entry>> &log, const log_write_option &);

  virtual void write_state(const ptr<log_state> &state, const log_write_option &);

  virtual void retrieve_state(fn_state);

  virtual void retrieve_log(fn_tx_log);

  result<ptr<log_entry>> get_log_entry(uint64_t index);
  void append(ptr<log_entry> &log) {
    auto r = state_machine_->append_entry(log);
    if (!r) {
      BOOST_LOG_TRIVIAL(error) << "append entries error, " << r.error().message();
    }
  }

  ptr<state_machine> &get_state_machine() {
    return state_machine_;
  }

  void stop() {
    server_->stop();
  }

  void join() {
    server_->join();
  }
  config conf_;
  node_id_t node_id_;
  std::string name_;
  node_config this_peer_;
  std::map<uint64_t, ptr<log_entry>> log_;
  std::map<uint64_t, ptr<log_entry>> committed_log_;
  log_state state_;
  ptr<net_service> service_;
  ptr<sock_server> server_;
  ptr<state_machine> state_machine_;
};