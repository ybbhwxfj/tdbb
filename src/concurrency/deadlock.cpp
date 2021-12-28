#include "concurrency/deadlock.h"
#include <utility>

deadlock::deadlock(fn_victim fn,
                   net_service *service,
                   uint32_t num_shard)
    : detecting_(false),
      distributed_(false),
      fn_victim_(std::move(fn)),
      service_(service),
      wait_path_(num_shard * 2) {
#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {
    distributed_ = true;
  }
#endif
}

void deadlock::tick() {
  std::scoped_lock l(mutex_);
  ptr<boost::asio::steady_timer> timer(new boost::asio::steady_timer(
      service_->get_service(SERVICE_DEADLOCK),
      boost::asio::chrono::milliseconds(500)));
  timer_ = timer;
  auto s = shared_from_this();
  auto fn_timeout = [timer, s](const boost::system::error_code &error) {
    if (not error.failed()) {
      s->tick();
      timer.get();
    }
  };

  detect();
  send_dependency();
  timer->async_wait(fn_timeout);
}

void deadlock::stop() {
  timer_->cancel();
}

void deadlock::add_wait_set(tx_wait_set &ws) {
  std::scoped_lock l(mutex_);
  if (detecting_) {
    set_.push_back(ws);
  } else {
    wait_path_.merge_dependency_set(ws);
  }
}

void deadlock::add_dependency(const dependency_set &ds) {
  std::scoped_lock l(mutex_);
  if (detecting_) {
    dep_.push_back(ds);
  } else {
    wait_path_.add_dependency_set(ds);
  }
}

void deadlock::tx_finish(xid_t xid) {
  std::scoped_lock l(mutex_);
  if (detecting_) {
    removed_.push_back(xid);
  } else {
    wait_path_.tx_finish(xid);
  }
}
void deadlock::set_next_node(node_id_t node_id) {
  std::scoped_lock l(mutex_);
  send_to_node_ = node_id;
}

void deadlock::send_dependency() {
  if (not distributed_) {
    return;
  }
  dependency_set ds;
  wait_path_.to_dependency_set(ds);
  if (ds.IsInitialized()) {
    if (send_to_node_ == 0) {
      return;
    }
    auto r = service_->async_send(send_to_node_, DL_DEPENDENCY, ds);
    if (not r) {
      BOOST_LOG_TRIVIAL(error) << " async send DL_DEPENDENCY error " << r.error().message();
    }
  }
}

void deadlock::recv_dependency(const dependency_set &ds) {
  std::scoped_lock l(mutex_);
  BOOST_ASSERT(distributed_);
  add_dependency(ds);
}

void deadlock::detect() {
  {
    std::scoped_lock l(mutex_);
    for (dependency_set &ds: dep_) {
      wait_path_.add_dependency_set(ds);
    }
    for (tx_wait_set &w: set_) {
      wait_path_.merge_dependency_set(w);
    }

    for (xid_t xid: removed_) {
      wait_path_.tx_finish(xid);
    }
    dep_.clear();
    removed_.clear();
    set_.clear();
    detecting_ = true;
  }
  std::set<xid_t> xids;
  auto fn = [&xids, this](const std::vector<xid_t> &circle) {
    if (!circle.empty()) {
      auto sorted = circle;
      std::sort(sorted.begin(), sorted.end());
      xid_t xid = sorted.front();
      xids.insert(xid);
    }
  };
  wait_path_.detect_circle(fn);

  std::unordered_map<xid_t, uint32_t> victim;
  {
    std::scoped_lock l(mutex_);
    detecting_ = false;
    for (xid_t xid: xids) {
      wait_path_.tx_victim(xid);
    }
    victim = wait_path_.victim();
  }
  for (const auto &v: victim) {
    fn_victim_(v.first);
  }
}

void deadlock::debug_deadlock(std::ostream &os) {
  wait_path_.handle_debug(os);
}

void deadlock::async_wait_lock(fn_wait_lock fn) {
  ptr<boost::asio::steady_timer> timer(new boost::asio::steady_timer(
      service_->get_service(SERVICE_DEADLOCK),
      boost::asio::chrono::milliseconds(500)));
  auto dl = shared_from_this();
  auto fn_timeout = [dl, timer, fn](const boost::system::error_code &error) {
    if (not error.failed()) {
      tx_wait_set ws;
      auto r = fn(ws);
      if (r) {
        dl->add_wait_set(ws);
        dl->async_wait_lock(fn);
      }
      timer.get();
    }
  };
  timer->async_wait(fn_timeout);
}