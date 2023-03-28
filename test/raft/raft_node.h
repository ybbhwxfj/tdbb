#pragma once

#include "common/gen_config.h"
#include "common/ptr.hpp"
#include "network/message_handler.h"
#include "network/net_service.h"
#include "network/sock_server.h"
#include "raft/state_machine.h"
#include "replog/log_service.h"
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/test/unit_test.hpp>
#include <iostream>
#include <memory>
#include <ostream>
using namespace boost;
using namespace std;

class raft_test_context;

class raft_node : public std::enable_shared_from_this<raft_node>,
                  public log_service {
public:
  std::recursive_mutex mutex_;
  raft_node(const config &conf, const std::string &case_name);
  ~raft_node() {}
  void start(raft_test_context *ctx);

  virtual void commit_log(const std::vector<ptr<raft_log_entry>> &,
                          const log_write_option &);
  virtual void write_log(const std::vector<ptr<raft_log_entry>> &log,
                         const log_write_option &);

  virtual void write_state(const ptr<raft_log_state> &state,
                           const log_write_option &);

  virtual void retrieve_state(fn_state);

  virtual void retrieve_log(fn_tx_log);

  result<ptr<raft_log_entry>> get_log_entry(uint64_t index);
  void append(ptr<raft_log_entry> &log) {
    auto r = state_machine_->leader_append_entry(log);
    if (!r) {
      LOG(error) << "append entries error, " << r.error().message();
    }
  }

  ptr<state_machine> &get_state_machine() { return state_machine_; }

  void stop() { server_->stop(); }

  void join() { server_->join(); }
  config conf_;
  node_id_t node_id_;
  std::string name_;
  node_config this_peer_;
  std::map<uint64_t, ptr<raft_log_entry>> log_;
  std::map<uint64_t, ptr<raft_log_entry>> committed_log_;
  raft_log_state state_;
  ptr<net_service> service_;
  ptr<sock_server> server_;
  ptr<state_machine> state_machine_;
};