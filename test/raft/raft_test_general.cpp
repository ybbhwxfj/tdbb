#include "raft_test_general.h"
#include "proto/raft_test.pb.h"
#include "raft_node.h"
#include "raft_test.h"
#include "raft_test_context.h"
#include "common/json_pretty.h"
#include <boost/log/sources/global_logger_storage.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#ifdef NUM_RG
#undef NUM_RG
#endif
#define NUM_RG 1

#define NUM_APPEND_LOGS 5000

void raft_test_general() {
  //boost::log::add_file_log("raft_test.log");
  boost::log::core::get()->set_filter(boost::log::trivial::severity >=
      boost::log::trivial::debug);

  ptr<raft_test_context> ctx = raft_test_startup("");

  uint64_t xid = 0;

  node_id_t leader_node = 0;
  uint64_t max_xid = 0;

  for (; xid < NUM_APPEND_LOGS;) {
    for (const auto &n: ctx->nodes_) {
      ptr<state_machine> sm = n.second->get_state_machine();
      sm_status s = sm->status();
      if (s.state == RAFT_STATE_LEADER) {
        leader_node = s.lead_node;
        xid++;
        ptr<log_entry> log(new log_entry());

        tx_log *op = log->add_xlog();
        op->set_xid(xid);
        op->set_log_type(TX_CMD_RM_COMMIT);
        auto r = sm->append_entry(log);
        if (r) {
          if (max_xid < xid) {
            max_xid = xid;
          }
        }
      }
    }
  }

  bool transfer_success = false;
  auto az_ids = ctx->az_id_set_;
  az_ids.erase(TO_AZ_ID(leader_node));
  az_id_t leader_az = *az_ids.rbegin();
  while (!transfer_success) {
    for (const auto &n: ctx->nodes_) {
      node_id_t node_id = n.first;
      ptr<state_machine> sm = n.second->get_state_machine();
      sm_status s = sm->status();
      if (TO_AZ_ID(node_id) == leader_az) {
        if (s.state == RAFT_STATE_LEADER) {
          transfer_success = true;
          break;
        } else {
          sm->node_transfer_leader(node_id);
        }
      }
    }
    sleep(1);
  }

  for (; xid < NUM_APPEND_LOGS * 2;) {
    for (const auto &n: ctx->nodes_) {
      ptr<state_machine> sm = n.second->get_state_machine();
      sm_status s = sm->status();
      if (s.state == RAFT_STATE_LEADER) {
        xid++;
        ptr<log_entry> log(new log_entry());

        tx_log *op = log->add_xlog();
        op->set_xid(xid);
        op->set_log_type(TX_CMD_RM_COMMIT);
        op->set_log_type(TX_CMD_RM_COMMIT);
        auto r = sm->append_entry(log);
        if (r) {
          if (max_xid < xid) {
            max_xid = xid;
          }
        }
      }
    }
  }

  ctx->wait_tx_commit(max_xid);

  ctx->stop_and_join();
}