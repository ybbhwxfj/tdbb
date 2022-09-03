#include "raft/state_machine.h"
#include "common/debug_url.h"
#include "common/variable.h"
#include "network/net_service.h"
#include <memory>
#include <boost/log/trivial.hpp>
#include <random>
#include <utility>

template<>
enum_strings<raft_state>::e2s_t enum_strings<raft_state>::enum2str = {
    {RAFT_STATE_LEADER, "RAFT_STATE_LEADER"},
    {RAFT_STATE_FOLLOWER, "RAFT_STATE_FOLLOWER"},
    {RAFT_STATE_CANDIDATE, "RAFT_STATE_CANDIDATE"},
};

state_machine::state_machine(
    const config &conf, ptr<net_service> sender,
    fn_on_become_leader fn_on_become_leader,
    fn_on_become_follower fn_on_become_follower,
    fn_commit_entries fn_commit,
    ptr<log_service> log_service)
    : ctx_strand(boost::asio::io_context::strand(sender->get_service(SERVICE_ASYNC_CONTEXT))),
      conf_(conf),
      node_id_(conf.this_node_config().node_id()),
      node_name_(id_2_name(conf.node_id())),
      tick_sequence_(0),
      tick_count_(0),
      commit_index_(0),
      current_term_(0),
      state_(RAFT_STATE_FOLLOWER),
      has_voted_for_(false), voted_for_(0), leader_id_(0),
      priority_replica_node_(0),
      consistent_log_index_(0),
      sender_(sender),
      rnd_(rnd_dev_()),
      rnd_dist_(0, conf_.get_tpcc_config().raft_leader_tick_ms()),
      az_rtt_ms_(conf_.get_tpcc_config().az_rtt_ms()),
      flow_control_rtt_num_(conf_.get_tpcc_config().flow_control_rtt_count()),
      tick_miliseconds_(conf_.get_tpcc_config().raft_leader_tick_ms()),
      follower_tick_max_(conf_.get_tpcc_config().raft_follow_tick_num()),
      rg_id_(conf.rg_id()),
      fn_on_become_leader_(std::move(fn_on_become_leader)),
      fn_on_become_follower_(std::move(fn_on_become_follower)),
      fn_on_commit_entries_(std::move(fn_commit)),
      log_service_(std::move(log_service)),
      stopped_(false) {
  BOOST_ASSERT(node_id_ != 0);
  az_rtt_ms_ = az_rtt_ms_ == 0 ? 100 : az_rtt_ms_;
  start_ = std::chrono::steady_clock::now();
}

void state_machine::on_start() {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  pre_start();
  BOOST_LOG_TRIVIAL(info) << "state machine start, term :" << current_term_;
  tick();
}

void state_machine::on_stop() {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  BOOST_LOG_TRIVIAL(info) << "state machine cancel_and_join";
  stopped_.store(true);
}

uint64_t state_machine::last_log_index() {
  if (log_.empty()) {
    return consistent_log_index_;
  } else {
    return offset_to_log_index(log_.size() - 1);
  }
}

uint64_t state_machine::last_log_term() {
  if (log_.empty()) {
    return 0;
  } else {
    return log_.back()->term();
  }
}

void state_machine::tick() {
  if (stopped_.load()) {
    BOOST_LOG_TRIVIAL(info) << "state machine stopped";
    return;
  }
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  uint32_t ms;
  if (state_ == RAFT_STATE_LEADER) {
    ms = tick_miliseconds_;
  } else {
    ms = follower_tick_max_ * tick_miliseconds_ + rnd_dist_(rnd_);
    if (priority_replica_node_ != 0 && priority_replica_node_ == node_id_) {
      // I want to be leader...
      node_transfer_leader(priority_replica_node_);
    }
  }
  timer_tick_.reset(new boost::asio::steady_timer(
      sender_->get_service(SERVICE_ASYNC_CONTEXT),
      boost::asio::chrono::milliseconds(ms)));
  auto fn_timeout = [this](const boost::system::error_code &error) {
    if (not error.failed()) {
      on_tick_timeout();
    } else {
      BOOST_LOG_TRIVIAL(error) << " async wait error " << error.message();
    }
  };
  timer_tick_->async_wait(
      boost::asio::bind_executor(
          get_strand(),
          fn_timeout));
}

void state_machine::pre_start() {
  std::vector<node_id_t> rep_node_id = conf_.get_rg_block_nodes(conf_.rg_id(), BLOCK_TYPE_ID_RLB);

  for (node_id_t node_id : rep_node_id) {
    nodes_ids_.push_back(node_id);
    BOOST_ASSERT(TO_RG_ID(node_id) == TO_RG_ID(node_id_));
    BOOST_ASSERT(is_rlb_block(node_id));
    auto c = conf_.get_node_conf(node_id);
    priority_.insert(std::make_pair(c.priority(), node_id));
    boost::asio::io_context::strand s(sender_->get_service(SERVICE_ASYNC_CONTEXT));
    ptr<client> cli(new client(s, c));
    clients_.insert(std::make_pair(node_id, cli));
  }
  if (priority_.size() > 1) {
    priority_replica_node_ = priority_.rbegin()->second;
  } else {
    priority_replica_node_ = 0;
  }
  for (std::pair<uint32_t, ptr<client>> pair : clients_) {
    sender_->async_client_connect(pair.second);
  }
  tick_count_ = 0;

  auto fn_state = [this](const slice &slice) {
    ptr<log_state> state(new log_state());
    bool ok = state->ParseFromArray(slice.data(), slice.size());
    if (not ok) {
      BOOST_ASSERT(false);
      BOOST_LOG_TRIVIAL(error) << " ParseFromArray error ";
      return;
    }
    log_state_ = state;
  };
  if (log_service_) {
    log_service_->retrieve_state(fn_state);
  }

  std::map<uint64_t, ptr<log_entry>> entries;
  auto fn_log = [&entries](const tx_log_index &k, const slice &slice) {
    uint64_t index = k.index();
    auto i = entries.find(index);
    ptr<log_entry> entry;
    bool no = i == entries.end();
    if (no) {
      entry = std::make_shared<log_entry>();
      entry->set_index(index);
    } else {
      entry = i->second;
    }
    entries.insert(std::make_pair(index, entry));
    if (k.type() == RAFT_LOG) {
      bool ok = entry->ParseFromArray(slice.data(), slice.size());
      if (not ok) {
        BOOST_ASSERT(false);
        BOOST_LOG_TRIVIAL(error) << " ParseFromArray log_entry error ";
      }
    } else {
      tx_log *log = entry->add_xlog();
      bool ok = log->ParseFromArray(slice.data(), slice.size());
      if (not ok) {
        BOOST_ASSERT(false);
        BOOST_LOG_TRIVIAL(error) << " ParseFromArray tx_log error ";
      }
      if (no) {
        entry->set_index(log->index());
        entry->set_term(log->term());
      } else {
        BOOST_ASSERT(entry->term() == log->term());
        BOOST_ASSERT(entry->index() == log->index());
        BOOST_ASSERT(false);
        BOOST_LOG_TRIVIAL(error) << " Raft index error ";
      }
    }
  };
  if (log_service_) {
    log_service_->retrieve_log(fn_log);
  }

  if (not log_state_) {
    log_state_.reset(new log_state());
    log_state_->set_term(current_term_);
    log_state_->set_commit_index(commit_index_);
    log_state_->set_consistency_index(consistent_log_index_);
    log_state_->set_vote(voted_for_);
    if (log_service_) {
      log_service_->write_state(log_state_, log_write_option());
    }
  } else {
    consistent_log_index_ = log_state_->consistency_index();
    current_term_ = log_state_->term();
    commit_index_ = log_state_->commit_index();
    voted_for_ = log_state_->vote();
    has_voted_for_ = voted_for_ != 0;
  }

  for (auto &entrie : entries) {
    if (entrie.first > consistent_log_index_) {
      log_.push_back(entrie.second);
      check_log_index();
    }
  }

  uint32_t next_index = offset_to_log_index(log_.size());
  progress_[node_id_].match_index_ = next_index - 1;
  progress_[node_id_].next_index_ = next_index;
}
void state_machine::on_tick_timeout() {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  tick_count_++;
  if (state_ == RAFT_STATE_LEADER) {
    auto _ = send_append_log();
    if (tick_count_ > 10) {
      leader_advance_consistency_index();
      tick_count_ = 0;
    }
    heart_beat();
  } else {
    if (tick_count_ >= follower_tick_max_) {
      tick_count_ = 0;
      timeout_request_vote();
    }
  }
  tick();
}

void state_machine::heart_beat() {
  //BOOST_LOG_TRIVIAL(trace) << "node " << node_name_ << " send heart beat " << current_term_ << "...";
  auto r = append_entry(null_log_entry_);
  if (!r) {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " send heart beat error " << current_term_ << "...";
  }
}

void state_machine::timeout_request_vote() {
  BOOST_LOG_TRIVIAL(info) << node_name_ << " time out request vote";
  if (state_ == RAFT_STATE_LEADER) {
    return;
  }
  tick_count_ = 0;
  state_ = RAFT_STATE_CANDIDATE;
  current_term_ = current_term_ + 1;
  has_voted_for_ = false;
  voted_for_ = 0;
  leader_id_ = 0;
  votes_responded_.clear();
  votes_granted_.clear();
  voter_log_.clear();
  log_state_->set_term(current_term_);
  log_state_->set_vote(voted_for_);
  log_state_->set_consistency_index(consistent_log_index_);
  log_state_->set_commit_index(commit_index_);
  if (log_service_) {
    log_service_->write_state(log_state_, log_write_option());
  }

  request_vote();
}

void state_machine::request_vote() {
  BOOST_LOG_TRIVIAL(info) << "node " << node_name_ << " request vote term "
                          << current_term_;

  if (state_ != RAFT_STATE_CANDIDATE) {
    return;
  }
  for (auto id : nodes_ids_) {
    if (votes_responded_.find(id) == votes_responded_.end()) { // not response
      auto req = std::make_shared<request_vote_request>();
      req->set_dest(id);
      req->set_source(node_id_);
      req->set_term(current_term_);
      req->set_last_log_index(last_log_index());
      req->set_last_log_term(last_log_term());

      result<void> send_result = async_send(id, RAFT_REQ_VOTE_REQ, req);
      if (!send_result) {
        BOOST_LOG_TRIVIAL(error) << "send message to " <<
                                 id_2_name(id) << " raft request vote error, " << send_result.error().message();
      }
    }
  }
}

result<void> state_machine::ccb_append_log(const ccb_append_log_request &msg) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  auto &mutable_msg = const_cast<ccb_append_log_request &>(msg);
  auto now = std::chrono::steady_clock::now();

  uint64_t ms = to_microseconds(now - start_);

  for (tx_log &log : *mutable_msg.mutable_logs()) {
    tx_logs_.emplace_back();
    tx_logs_.rbegin()->Swap(&log);
    tx_logs_.rbegin()->set_repl_latency(ms);
    BOOST_LOG_TRIVIAL(trace) << node_name_ << "state machine ccb_append_log tx_rm log " << log.xid();
  }
  //return send_append_log();

  if (tx_logs_.size() >= conf_.get_block_config().append_log_entries_batch_min()) {
    return send_append_log();
  } else {
    return outcome::success();
  }
}

result<void> state_machine::send_append_log() {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  if (tx_logs_.empty()) {
    return outcome::success();
  }

  size_t n = 0;
  ptr<log_entry> entry(new log_entry());
  for (; not tx_logs_.empty();) {
    tx_log *log = entry->add_xlog();
    log->Swap(&tx_logs_.front());
    tx_logs_.pop_front();
    n++;
    if (n > APPEND_LOG_ENTRIES_BATCH_MAX) {
      BOOST_LOG_TRIVIAL(info) << "raft append log size: " << tx_logs_.size();
      break;
    }
  }
  auto r = append_entry(entry);
  if (not r) {
    BOOST_LOG_TRIVIAL(error) << "send_append_log append entry error " << r.error().message();
    if (fn_on_commit_entries_) {
      // leader cancel this log entry ...
      std::vector<ptr<log_entry>> vec;
      vec.push_back(entry);
      fn_on_commit_entries_(r.error().code(), state_ == RAFT_STATE_LEADER, vec);
    }
  }
  return r;
}

result<void> state_machine::append_entry(ptr<log_entry> entries) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  if (state_ != RAFT_STATE_LEADER) {
    BOOST_LOG_TRIVIAL(error) << node_name_ << " only leader can append";
    return outcome::failure(EC::EC_NOT_LEADER);
  }

  bool append_log_entries = entries.get() != nullptr;
  if (append_log_entries) {
    uint64_t offset = log_.size();
    uint64_t index = offset_to_log_index(offset);
    entries->set_index(index);
    entries->set_term(current_term_);
    offset++;
    if (entries->xlog().empty()) {
      BOOST_ASSERT(false);
    }
    log_.push_back(entries);
    check_log_index();
    std::vector<ptr<log_entry>> vec;
    vec.push_back(entries);
    if (log_service_) {
      log_service_->write_log(vec, log_write_option());
    }

    BOOST_ASSERT(log_.size() < 2 || log_[log_.size() - 2]->index() + 1 == log_[log_.size() - 1]->index());

    log_state_->set_term(current_term_);
    log_state_->set_vote(voted_for_);
    log_state_->set_consistency_index(consistent_log_index_);
    log_state_->set_commit_index(commit_index_);

    auto &p = progress_[node_id_];
    p.match_index_ = index;
    p.next_index_ = index + 1;
  }

  // BOOST_LOG_TRIVIAL(debug) << "node " << node_name_ << " append entry";

  for (auto &progress : progress_) {
    if (progress.first != node_id_) {
      append_entries(progress.second);
    }
  }
  return outcome::success();
}

void state_machine::append_entries(progress &tracer) {
  uint32_t id = tracer.node_id_;
  uint64_t next_index = tracer.next_index_;

  uint64_t prev_log_term = 0;
  uint64_t prev_log_index = 0;
  uint64_t start_offset = 0;
  if (next_index != 0) {
    prev_log_index = tracer.next_index_ - 1;
    start_offset = log_index_to_offset(next_index);
  }
  if (prev_log_index > consistent_log_index_) {
    uint64_t offset = log_index_to_offset(prev_log_index);
    if (offset < log_.size()) {
      prev_log_term = log_[offset]->term();
    } else {
      BOOST_ASSERT(false);
    }
  }

  auto now = std::chrono::steady_clock::now();
  uint64_t ms = to_milliseconds(now - start_);
  auto ae = std::make_shared<append_entries_request>();
  ae->set_source(node_id_);
  ae->set_dest(id);
  ae->set_term(current_term_);
  ae->set_prev_log_index(prev_log_index);
  ae->set_prev_log_term(prev_log_term);
  ae->set_commit_index(commit_index_);
  ae->set_consistency_index(consistent_log_index_);
  ae->set_tick_sequence(ms);
  uint64_t size = log_.size();
  BOOST_ASSERT(size <= log_.size());
  for (uint64_t i = 0, off = start_offset;
       off < size && i < tracer.append_log_num_; off++, i++) {
    log_entry *p = ae->add_entries();
    *p = *log_[off];
    BOOST_ASSERT(log_[off]->index() == prev_log_index + 1 + i);
  }
  if (ae->entries_size() > 0) {
    // auto now = std::chrono::steady_clock::now();
    // uint64_t ms = to_milliseconds(now - start_);
    //BOOST_LOG_TRIVIAL(info) << node_name_ << " " << ms << "ms send append to " << id_2_name(id) << " next index: "
    //                         << next_index <<
    //                         " [" << start_offset << ":" << start_offset + ae->entries_size() << "]";
    ae->set_heart_beat(ae->entries_size() == 0);
  }

  auto r = async_send(id, RAFT_APPEND_ENTRIES_REQ, ae);
  if (!r) {
    if (r.error().code() == EC_MESSAGE_SIZE_TOO_LARGE) {
      if (tracer.append_log_num_ > 2) {
        tracer.append_log_num_ >>= 1;
      }
    }
    BOOST_LOG_TRIVIAL(error) << "send message raft append entries error " << r.error().message();
  } else {

  }
}

void state_machine::become_leader() {
  if (state_ != RAFT_STATE_CANDIDATE) {
    return;
  }
  if (votes_granted_.size() * 2 < nodes_ids_.size()) {
    return;
  }
  BOOST_LOG_TRIVIAL(info) << "node " << node_name_ << " become leader at term "
                          << current_term_;

  state_ = RAFT_STATE_LEADER;
  leader_id_ = node_id_;
  for (auto id : nodes_ids_) {
    progress *p = nullptr;
    auto i = progress_.find(id);
    if (i == progress_.end()) {
      auto pair = progress_.insert(std::make_pair(id, progress()));
      p = &pair.first->second;
    } else {
      p = &i->second;
    }
    p->node_id_ = id;
    if (id == node_id_) {
      p->next_index_ = last_log_index() + 1;
      p->match_index_ = last_log_index();
    } else {
      p->next_index_ = consistent_log_index_ + 1;
      p->match_index_ = consistent_log_index_;
    }

    p->append_log_num_ = MAX_RAFT_LOG_ENTRIES;
    BOOST_ASSERT(progress_[id].next_index_ != 0);
  }

  if (fn_on_become_leader_) {
    fn_on_become_leader_(current_term_);
  }

  //
  //
  heart_beat();
  timer_tick_->expiry();
}

void state_machine::become_follower() {
  if (fn_on_become_follower_) {
    BOOST_LOG_TRIVIAL(info) << "node " << node_name_
                            << " from become follower at term " << current_term_;
    fn_on_become_follower_(current_term_);
  }

  state_ = RAFT_STATE_FOLLOWER;
  progress_.clear();
  BOOST_LOG_TRIVIAL(info) << "node " << node_name_
                          << " become follower at term " << current_term_;
}

void state_machine::leader_advance_commit_index() {
  if (state_ != RAFT_STATE_LEADER) {
    return;
  }

  uint64_t new_commit_index = 0;
  for (int i = log_.size() - 1; i >= 0; i--) {
    log_index_t log_index = offset_to_log_index(i);
    size_t n = 0;
    for (auto id : nodes_ids_) {
      if (progress_[id].match_index_ >= log_index) {
        n++;
      }
    }
    if (n * 2 > nodes_ids_.size()) {
      new_commit_index = log_index;
      break;
    }
  }
  assert(new_commit_index <= last_log_index());
  if (new_commit_index > 0 &&
      log_[log_index_to_offset(new_commit_index)]->term() <= current_term_) {

    update_commit_index(new_commit_index);
  }
}

void state_machine::leader_advance_consistency_index() {
  if (state_ != RAFT_STATE_LEADER) {
    return;
  }
  uint64_t new_consistency_index = 0;
  for (int i = log_.size() - 1; i >= 0; i--) {
    log_index_t log_index = offset_to_log_index(i);
    size_t n = 0;
    for (auto id : nodes_ids_) {
      if (progress_[id].match_index_ >= log_index) {
        n++;
      }
    }
    if (n == nodes_ids_.size()) {
      if (log_.size() > 1 &&
          commit_index_ >= log_index &&
          offset_to_log_index(log_.size() - 1) >= log_index) {
        new_consistency_index = log_index;
      }
      break;
    }
  }
  if (new_consistency_index > 0 &&
      log_[log_index_to_offset(new_consistency_index)]->term() == current_term_) {
    update_consistency_index(new_consistency_index);
  }
}

void state_machine::handle_request_vote_request(
    const request_vote_request &request) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  update_term(request.term());

  uint32_t src_node_id = request.source();
  uint64_t last_term = last_log_term();
  uint64_t last_index = last_log_index();
  bool log_ok = request.last_log_term() > last_term ||
      (request.last_log_term() == last_term &&
          request.last_log_index() >= last_index);
  bool granted =
      request.term() == current_term_ && log_ok &&
          ((has_voted_for_ && voted_for_ == src_node_id) || !has_voted_for_);

  BOOST_LOG_TRIVIAL(trace) << "node " << node_id_
                           << " handle request vote request "
                           << "current term " << current_term_
                           << " request term " << request.term() << " granted "
                           << granted;

  auto response = std::make_shared<request_vote_response>();
  if (granted) {
    has_voted_for_ = true;
    voted_for_ = src_node_id;
  }

  response->set_source(node_id_);
  response->set_dest(src_node_id);
  response->set_term(current_term_);
  response->set_vote_granted(granted);

  result<void> send_result =
      async_send(src_node_id, RAFT_REQ_VOTE_RESP, response);
  if (not send_result) {
    BOOST_LOG_TRIVIAL(error)
      << "send message raft request_vote_response error";
  }
}

void state_machine::handle_request_vote_response(
    const request_vote_response &response) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  update_term(response.term());
  if (response.term() != current_term_) {
    return;
  }

  uint32_t src_node_id = response.source();
  votes_responded_.insert(src_node_id);
  if (response.vote_granted()) {
    BOOST_LOG_TRIVIAL(trace)
      << node_name_ << " receive request_vote_response grant from "
      << id_2_name(src_node_id) << " current term " << current_term_
      << " response term " << response.term();
    votes_granted_.insert(src_node_id);
  }

  if (state_ == RAFT_STATE_CANDIDATE &&
      votes_granted_.size() * 2 > nodes_ids_.size()) {
    become_leader();
  }
}

void state_machine::resonse_append_entries_response(uint32_t to_node_id,
                                                    uint64_t tick_sequence,
                                                    bool success,
                                                    uint64_t match_index,
                                                    bool heart_beat) {
  BOOST_ASSERT(match_index == 0 || log_index_to_offset(match_index) < log_.size());
  auto response = std::make_shared<append_entries_response>();
  response->set_source(node_id_);
  response->set_dest(to_node_id);
  response->set_term(current_term_);
  response->set_success(success);
  response->set_match_index(match_index);
  response->set_heart_beat(heart_beat);
  response->set_lead(leader_id_);
  response->set_tick_sequence(tick_sequence);

  if (not heart_beat) {
    BOOST_LOG_TRIVIAL(trace)
      << "response append entry, node " << id_2_name(node_id_) << " match_index "
      << response->match_index() << "  to " << id_2_name(to_node_id);
  }

  result<void> r = async_send(to_node_id, RAFT_APPEND_ENTRIES_RESP, response);
  if (not r) {
    BOOST_LOG_TRIVIAL(error) << "send message RAFT_REQ_VOTE_RESP error, " << r.error().message();
  }
}

void state_machine::handle_append_entries_request(
    const append_entries_request &request) {
#ifdef MULTI_THREAD_EXECUTOR
  std::scoped_lock l(mutex_);
#endif
  bool heart_beat = request.heart_beat();

  if (not heart_beat) {
    if (request.entries_size() > 0) {
      BOOST_LOG_TRIVIAL(trace) << "node: " << node_name_ << " handle append log , index：["
                               << request.entries(0).index() << ":" <<
                               request.entries(request.entries_size() - 1).index() << "]";
    }
  }
  if (request.term() < current_term_) {
    BOOST_LOG_TRIVIAL(info) << "node: " << node_name_ << " reject request term: " << request.term()
                            << " current term " << current_term_;
    resonse_append_entries_response(request.source(),
                                    request.tick_sequence(),
                                    false, 0, heart_beat);
    return; // reject request
  } else if (request.term() > current_term_) {
    tick_count_ = 0;
    current_term_ = request.term();
    become_follower();
    return;
  } else {
    leader_id_ = request.source();
    tick_count_ = 0;
  }
  if (state_ == RAFT_STATE_LEADER) {
    return;
  } else if (state_ == RAFT_STATE_CANDIDATE) {
    become_follower();
    return;
  }

  bool log_ok;
  log_index_t next_index;
  uint64_t next_offset;
  uint64_t entries_append;
  if (request.prev_log_index() <= consistent_log_index_) {
    log_ok = true;
    next_index = consistent_log_index_ + 1;
    next_offset = log_index_to_offset(next_index);
    entries_append = consistent_log_index_ - request.prev_log_index();
  } else {
    entries_append = 0;
    uint64_t prev_log_offset = log_index_to_offset(request.prev_log_index());
    next_index = request.prev_log_index() + 1;
    next_offset = prev_log_offset + 1;
    BOOST_ASSERT(next_index > 0);
    log_ok =
        (request.prev_log_index() == 0) ||
            (request.prev_log_index() > 0 &&
                request.prev_log_index() <= last_log_index() &&
                (log_.size() == 0 ||
                    (log_.size() > prev_log_offset
                        && request.prev_log_term() == log_[prev_log_offset]->term()))
            );
  }

  if (!log_ok) {
    BOOST_LOG_TRIVIAL(info) << "node: " << node_name_ << " reject request, "
                            << " request prev log index:" << request.prev_log_index()
                            << " current last index: " << last_log_index()
                            << " consistent index:" << consistent_log_index_
                            << " request prev log term:" << request.prev_log_term();

    resonse_append_entries_response(request.source(), request.tick_sequence(),
                                    false, 0, heart_beat);
    return; // reject request
  }

  if (request.entries().empty()) {
    resonse_append_entries_response(
        request.source(), request.tick_sequence(), true, next_index - 1, heart_beat);
  } else {
    if (log_.size() > next_offset) {
      bool conflict = false;
      size_t log_i = next_offset;
      size_t req_i = entries_append;
      size_t entries_size = (size_t) request.entries_size();
      for (; log_i < log_.size() && req_i < entries_size; log_i++, req_i++) {
        if (request.entries(req_i).term() != log_[log_i]->term()) {
          // conflict, remove 1 entry, not necessarily send a response message
          BOOST_LOG_TRIVIAL(trace) << "log conflict";
          log_.resize(next_offset);
          conflict = true;
          break;
        } else {
          BOOST_ASSERT(request.entries(req_i).index() == log_[log_i]->index());
        }
      }
      if (not conflict) {
        size_t entry_size = (size_t) request.entries_size();
        if (req_i < entry_size) {
          // have additional entries, append to the state machine
          uint32_t write_begin_off = log_i;
          for (; req_i < entry_size; log_i++, req_i++) {
            log_.push_back(std::make_shared<log_entry>(request.entries(req_i)));
            check_log_index();
          }
          std::vector<ptr<log_entry>> vec(log_.begin() + write_begin_off, log_.end());
          if (log_service_) {
            log_service_->write_log(vec, log_write_option());
          }
        }
        BOOST_LOG_TRIVIAL(trace) << "log size > offset; index [" <<
                                 log_[next_offset]->index() << ":" <<
                                 log_[log_i - 1]->index() << "]";
        resonse_append_entries_response(
            request.source(), request.tick_sequence(), true, request.prev_log_index() + req_i, heart_beat);
      }
    } else if (log_.size() == next_offset) {
      // append log entries,
      auto num_entries = (size_t) request.entries().size();
      std::vector<ptr<log_entry>> vec;
      size_t num = 0;
      for (size_t i = entries_append; i < num_entries; i++, num++) {
        const log_entry &e = request.entries().at(i);
        ptr<log_entry> entry(new log_entry(e));
        vec.push_back(entry);
        log_.push_back(entry);
        check_log_index();
      }
      if (log_service_) {
        log_service_->write_log(vec, log_write_option());
      }
      BOOST_LOG_TRIVIAL(trace) << "handle append entry, node " << node_id_
                               << " receive " << num_entries << " log entries ";
      resonse_append_entries_response(
          request.source(), request.tick_sequence(), true,
          request.prev_log_index() + entries_append + num, heart_beat);
    }
  }
  update_commit_index(request.commit_index());
  update_consistency_index(request.consistency_index());
}

void state_machine::handle_append_entries_response(
    const append_entries_response &response) {
  std::scoped_lock l(mutex_);
  update_term(response.term());
  uint32_t src_node_id = response.source();
  progress &p = progress_[src_node_id];
  {
    auto now = std::chrono::steady_clock::now();
    uint64_t ms = to_milliseconds(now - start_) - response.tick_sequence();
    //BOOST_LOG_TRIVIAL(trace) << "RTT " << ms << "ms";
    if (ms > az_rtt_ms_ * flow_control_rtt_num_) {
      if (p.append_log_num_ > 2) {
        p.append_log_num_ >>= 1;
      }
    } else {
      if (p.append_log_num_ < MAX_RAFT_LOG_ENTRIES) {
        p.append_log_num_ <<= 1;
      }
    }
  }
  if (response.term() != current_term_) {
    return;
  }

  bool heart_beat = response.heart_beat();
  if (heart_beat) {
    //BOOST_LOG_TRIVIAL(trace) << "node " << node_name_ << " receive heart beat " << current_term_ << "...";
  }

  if (response.success()) {
    if (p.match_index_ < response.match_index()) {
      p.match_index_ = response.match_index();
    }
    if (p.next_index_ < response.match_index() + 1) {
      p.next_index_ = response.match_index() + 1;
      BOOST_ASSERT(p.next_index_ != 0);
    }
    if (not heart_beat) {
      auto now = std::chrono::steady_clock::now();
      uint64_t ms = to_milliseconds(now - start_);
      BOOST_LOG_TRIVIAL(trace)
        << node_name_ << " " << ms << "ms, match index： "
        << response.match_index() << " receive from : " << id_2_name(src_node_id);

      leader_advance_commit_index();
    }
  } else {
    auto i = progress_.find(src_node_id);
    if (i != progress_.end()) {
      if (i->second.next_index_ > 1) {
        i->second.next_index_--;
        BOOST_ASSERT(i->second.next_index_ > consistent_log_index_);
      }
    }
  }
}

void state_machine::update_term(uint64_t term) {
  if (term <= current_term_) {
    return;
  }
  BOOST_ASSERT(current_term_ <= term);
  // advance its term
  current_term_ = term;
  has_voted_for_ = false;
  voted_for_ = 0;
  become_follower();
}

void state_machine::handle_debug(const std::string &path, std::ostream &os) {
  std::scoped_lock l(mutex_);
  if (not boost::regex_match(path, url_json_prefix)) {
    os << "raft state : " << enum2str(state_) << std::endl;
  }

  if (boost::regex_match(path, url_log)) {
    size_t size = log_.size();
    for (size_t i = 0; i < size; i++) {
      auto log = log_[i];
      os << "term :" << log->term();
      os << " index : " << log->index();
      os << std::endl;
      for (auto op : log->xlog()) {
        os << "    op: " << op.log_type() << " tx_rm : " << op.xid() << std::endl;
      }
    }
  } else if (boost::regex_match(path, url_log_offset)) {
    size_t size = log_.size();
    if (size > 1) {
      auto log = log_[size - 1];
      os << "term :" << log->term();
      os << " index : " << log->index();
      os << std::endl;
      for (auto op : log->xlog()) {
        os << "    op: " << op.log_type() << " tx_rm : " << op.xid() << std::endl;
      }
    }
  }
}

void state_machine::on_recv_message(message_type id, byte_buffer &msg_body) {
  switch (id) {
    case message_type::RAFT_APPEND_ENTRIES_REQ: {
      append_entries_request request;
      result<void> res = buf_to_pb(msg_body, request);
      if (res) {
        handle_append_entries_request(request);
      }

    }
      break;
    case message_type::RAFT_APPEND_ENTRIES_RESP: {
      append_entries_response response;
      result<void> res = buf_to_pb(msg_body, response);
      if (res) {
        handle_append_entries_response(response);
      }
      break;
    }
    case message_type::RAFT_REQ_VOTE_REQ: {
      request_vote_request request;
      result<void> res = buf_to_pb(msg_body, request);
      if (res) {
        handle_request_vote_request(request);
      }
      break;
    }
    case message_type::RAFT_REQ_VOTE_RESP: {
      request_vote_response response;
      result<void> res = buf_to_pb(msg_body, response);
      if (res) {
        handle_request_vote_response(response);
      }
      break;
    }
    case message_type::RAFT_TRANSFER_LEADER: {
      //to_pb_msg(msg_body, );
      auto msg = std::make_shared<transfer_leader>();
      to_pb(msg_body, *msg);
      handle_transfer_leader(msg);
      break;
    }
    case message_type::RAFT_TRANSFER_NOTIFY: {
      transfer_notify msg;
      to_pb(msg_body, msg);
      handle_transfer_notify(msg);
      break;
    }
    default:BOOST_ASSERT_MSG(false, "unknown message");
      break;
  }
}
sm_status state_machine::status() {
  std::scoped_lock l(mutex_);
  sm_status s;
  s.state = state_;
  s.term = current_term_;

  if (state_ == RAFT_STATE_LEADER || state_ == RAFT_STATE_FOLLOWER) {
    s.lead_node = voted_for_;
    BOOST_ASSERT(s.lead_node == 0 ||
        TO_RG_ID(s.lead_node) == TO_RG_ID(node_id_));
  } else {
    s.lead_node = 0;
  }
  return s;
}

log_index_t state_machine::offset_to_log_index(uint64_t offset) {
  BOOST_ASSERT(offset <= log_.size());
  return consistent_log_index_ + offset + 1;
}

uint64_t state_machine::log_index_to_offset(log_index_t index) {
  BOOST_ASSERT(index >= consistent_log_index_ + 1);
  return index - consistent_log_index_ - 1;
}

void state_machine::update_consistency_index(uint64_t index) {
  BOOST_ASSERT(log_.size() == 0 || (log_.size() > 0 && offset_to_log_index(log_.size() - 1) >= index));

  if (index < consistent_log_index_ + 10) { // leave 10 entries
    return;
  }
  index -= 10;
  uint64_t removed = index - consistent_log_index_;
  //uint64_t prev_size = log_.size();
  log_.erase(log_.begin(), log_.begin() + removed);
  //BOOST_ASSERT(log_.size() + removed == prev_size);
  //BOOST_ASSERT(log_.size() > 0);
  consistent_log_index_ = index;
  log_state_->set_consistency_index(index);
  log_write_option opt;
  opt.set_force(false);
  if (log_service_) {
    log_service_->write_state(log_state_, opt);
  }
}

void state_machine::update_commit_index(uint64_t index) {
  if (index <= commit_index_) {
    return;
  }

  if (log_.empty()) {
    return;
  }
  uint64_t commit_index = std::min(offset_to_log_index(log_.size() - 1), index);
  uint64_t prev_commit_index = commit_index_;
  uint64_t off_begin = 0;
  uint64_t off_end = 0;
  if (prev_commit_index == commit_index) {
    return;
  }
  if (prev_commit_index != 0) {
    off_begin = log_index_to_offset(prev_commit_index) + 1;
  }
  if (commit_index != 0) {
    off_end = log_index_to_offset(commit_index) + 1;
  }
  if (off_begin >= off_end) {
    return;
  }

  std::vector<ptr<log_entry>> vec(
      log_.begin() + off_begin, log_.begin() + off_end);
  BOOST_ASSERT(!vec.empty());

  log_state_->set_commit_index(commit_index);
  log_write_option opt;
  opt.set_force(false);
  if (log_service_) {
    log_service_->write_state(log_state_, opt);
  }

  BOOST_LOG_TRIVIAL(trace) << node_name_ << "commit " << off_begin << " : " << off_end;

  if (fn_on_commit_entries_) {
    if (tx_logs_.size() > APPEND_LOG_ENTRIES_BATCH_MAX) {
      // penalty for a fast CC Block beyond RL Block's processing capability
      // delay some milliseconds
#ifdef DELAY_PENALTY
      uint64_t delay_ms = tx_logs_.size() * 2;
      ptr<boost::asio::steady_timer> timer(
          new boost::asio::steady_timer(
              sender_->get_service(SERVICE_ASYNC_CONTEXT),
              boost::asio::chrono::milliseconds(delay_ms)));
      timer->async_wait([timer, vec, this](const boost::system::error_code &error) {
        timer.get();
        if (not error.failed()) {
          this->fn_on_commit_entries_(EC::EC_OK, this->state_==RAFT_STATE_LEADER, vec);
        }
      });
#else
      BOOST_LOG_TRIVIAL(info) << node_name_ << " log pile up " << tx_logs_.size();

      fn_on_commit_entries_(EC::EC_OK, state_ == RAFT_STATE_LEADER, vec);
#endif //
    } else {
      fn_on_commit_entries_(EC::EC_OK, state_ == RAFT_STATE_LEADER, vec);
    }
  }
  commit_index_ = commit_index;
}

void state_machine::handle_transfer_leader(const ptr<transfer_leader> msg) {

  node_id_t id = msg->leader_transferee();
  if (state_ == RAFT_STATE_FOLLOWER) {
    if (leader_id_ != 0) {
      auto r = async_send(leader_id_, RAFT_TRANSFER_LEADER, msg);
      if (not r) {
        BOOST_LOG_TRIVIAL(error) << "send raft transfer leader error " << r.error().message();
      }
    }
  } else {
    if (id == node_id_) {
      return;
    }
    auto t = progress_.find(id);
    if (t == progress_.end()) {
      return;
    }
    if (t->second.match_index_ == last_log_index()) {
      auto m = std::make_shared<transfer_notify>();
      m->set_leader_transferee(id);
      auto r = async_send(id, RAFT_TRANSFER_NOTIFY, m);
      if (not r) {
        BOOST_LOG_TRIVIAL(error) << "send raft transfer leader notify error " << r.error().message();
      }
    } else {
      append_entries(t->second);
    }
  }
}

void state_machine::handle_transfer_notify(const transfer_notify &) {
  std::scoped_lock l(mutex_);
  timeout_request_vote();
}

void state_machine::node_transfer_leader(node_id_t node_id) {
  std::scoped_lock l(mutex_);
  auto msg = std::make_shared<transfer_leader>();
  msg->set_leader_transferee(node_id);
  handle_transfer_leader(msg);
}

void state_machine::check_log_index() {
  BOOST_ASSERT(log_.size() < 2 || log_[log_.size() - 2]->index() + 1 == log_[log_.size() - 1]->index());
}