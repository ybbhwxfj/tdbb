#include <memory>
#include <boost/test/unit_test.hpp>
#include "raft_node.h"
#include "raft_test_context.h"

void test_become_leader(uint64_t node_id, raft_test_context *ctx, uint64_t term) {
  bool at_most_one_leader = ctx->check_at_most_one_leader_per_term(node_id, term, true);
  BOOST_CHECK(at_most_one_leader);
  bool max_committed_logs = ctx->check_max_committed_logs(node_id);
  BOOST_CHECK(max_committed_logs);
}

void test_become_follower(uint64_t node_id, raft_test_context *ctx, uint64_t term) {
  bool at_most_one_leader = ctx->check_at_most_one_leader_per_term(node_id, term, false);
  BOOST_CHECK(at_most_one_leader);
}

void test_commit_log_entries(uint64_t node_id,
                             raft_test_context *ctx,
                             bool lead,
                             const std::vector<ptr<log_entry>> &entries) {
  bool check_commit_entries = ctx->check_commit_entries(node_id, lead, entries);
  std::stringstream ssm;
  if (not entries.empty()) {
    ssm << "check log commit [" << entries[0]->index() << ":" << entries[entries.size() - 1]->index() << "]";
  }
  BOOST_CHECK_MESSAGE(check_commit_entries, ssm.str());
}

raft_node::raft_node(const config &conf, const std::string &case_name) :
    conf_(conf),
    node_id_(conf.node_id()),
    name_(id_2_name(conf.node_id())) {
  if (case_name != "") {
    boost::filesystem::path p(__FILE__);
    std::string cnf_json_file = case_name + "_" + std::to_string(conf.az_id()) + ".json";
    p = p.parent_path().append("raft_test_log").append(cnf_json_file);
    std::ifstream fsm(p);
    std::stringstream ssm;
    ssm << fsm.rdbuf();
    raft_test_log log;
    bool ok_json_to_pb = json_to_pb(ssm.str(), log);
    if (not ok_json_to_pb) {
      BOOST_CHECK(ok_json_to_pb);
    } else {
      state_ = log.state();
      for (const auto &l : log.entries()) {
        BOOST_ASSERT(l.index() > 0);
        log_.insert(
            std::make_pair(l.index(), std::make_shared<log_entry>(l)));
      }
    }
  }
}

void raft_node::start(raft_test_context *ctx) {
  service_ = std::make_shared<net_service>(conf_);
  server_ = std::make_shared<sock_server>(conf_, service_);
  ctx->commit_xid_[node_id_] = 0;
  fn_on_become_leader fn_leader = [this, ctx](uint64_t term) {
    test_become_leader(node_id_, ctx, term);
  };

  fn_on_become_follower fn_follower = [this, ctx](uint64_t term) {
    test_become_follower(node_id_, ctx, term);
  };
  ptr<log_service> l = shared_from_this();

  fn_commit_entries fn_commit = [this, ctx](EC ec, bool lead, const std::vector<ptr<log_entry>> &entries) {
    if (ec == EC::EC_OK) {
      test_commit_log_entries(node_id_, ctx, lead, entries);
    }
  };

  state_machine_ = std::make_shared<state_machine>(
      conf_,
      service_,
      fn_leader,
      fn_follower,
      fn_commit,
      l);

  message_handler raft_message_handler = [this](
      ptr<connection>,
      message_type id,
      byte_buffer &buffer,
      msg_hdr *
  ) -> result<void> {
    ptr<byte_buffer> buf(new byte_buffer(buffer));
    boost::asio::post(
        state_machine_->get_strand(),
        [this, id, buf] {
          state_machine_->on_recv_message(id, *buf);
        });

    return outcome::success();
  };

  service_->register_handler(raft_message_handler);
  server_->start();
  state_machine_->on_start();
}

void raft_node::commit_log(const std::vector<ptr<log_entry>> &log, const log_write_option &) {
  for (auto const &l : log) {
    committed_log_[l->index()] = l;
  }
}

void raft_node::write_log(const std::vector<ptr<log_entry>> &log, const log_write_option &) {
  std::scoped_lock lock(mutex_);
  for (const auto &l : log) {
    // BOOST_LOG_TRIVIAL(trace) << id_2_name(node_id_) << " write log index: " << l->index();
    auto i = log_.find(l->index());
    if (i == log_.end()) {
      log_[l->index()] = l;
    } else {
      i->second = l;
    }

  }
}

void raft_node::write_state(const ptr<log_state> &state, const log_write_option &) {
  std::scoped_lock lock(mutex_);
  state_ = *state;
}

void raft_node::retrieve_state(fn_state fn) {
  std::scoped_lock lock(mutex_);
  std::string s = state_.SerializeAsString();
  ::slice v(s);
  fn(v);
}

void raft_node::retrieve_log(fn_tx_log fn) {
  std::scoped_lock lock(mutex_);
  for (const auto &kv : log_) {
    std::string s = kv.second->SerializeAsString();
    ::slice v(s);
    log_index_t k(kv.second->index());
    fn(k, v);
  }
}

result<ptr<log_entry>> raft_node::get_log_entry(uint64_t index) {
  std::scoped_lock lock(mutex_);
  auto i = log_.find(index);
  if (i == log_.end()) {
    return outcome::failure(EC::EC_NOT_FOUND_ERROR);
  } else {
    return outcome::success(i->second);
  }
}