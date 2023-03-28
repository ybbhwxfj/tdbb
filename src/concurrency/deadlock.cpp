#include "concurrency/deadlock.h"
#include "common/scoped_time.h"
#include <sstream>
#include <utility>

deadlock::deadlock(
    fn_victim fn,
    net_service *service,
    uint32_t num_shard,
    bool deadlock_detection,
    uint64_t detect_timeout_ms,
    uint64_t lock_wait_timeout_ms
)
    : ctx_strand(service->get_service(SERVICE_CC)),
      wait_die_(!deadlock_detection), detecting_(false),
      distributed_(false), fn_victim_(std::move(fn)), service_(service),
      wait_path_(num_shard * 2), rnd_(0, 99), deadlock_detect_ms_(detect_timeout_ms),
      lock_wait_timeout_ms_(lock_wait_timeout_ms),
      num_shard_(num_shard), time_ms_(0) {
#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {
    distributed_ = true;
  }
#endif
}

void deadlock::start() {
  if (wait_die_) {
    return;
  }
  auto s = shared_from_this();
  auto fn_timeout = [s]() {
    s->tick();
  };
  ptr<timer> t(new timer(
      get_strand(), boost::asio::chrono::milliseconds(deadlock_detect_ms_),
      fn_timeout));
  timer_ = t;
  timer_->async_tick();
}

void deadlock::tick() {
  detect();
}

void deadlock::stop_and_join() {
  if (timer_) {
    timer_->cancel_and_join();
  }
}

void deadlock::add_wait_set(ptr<tx_wait> ws) {
  if (wait_die_) {
    xid_t xid = ws->xid();
    for (xid_t i : ws->in_set()) {
      if (xid >= i) {
        // abort xid
        async_victim(xid);
      }
    }
  } else {
    if (detecting_) {
      set_.push_back(ws);
    } else {
      wait_path_.add(ws);
    }
  }
}

void deadlock::add_dependency(const ptr<dependency_set> ds) {
  std::scoped_lock l(mutex_);
  if (detecting_) {
    dep_.push_back(ds);
  } else {
    wait_path_.add_dependency_set(*ds);
  }
}

void deadlock::gut_tx_finish(xid_t) {
  /*
  if (detecting_) {
    removed_.push_back(xid);
  } else {
    wait_path_.tx_finish(xid);
  }
   */
}

void deadlock::recv_dependency(const ptr<dependency_set> ds) {

  auto dl = shared_from_this();
  boost::asio::post(get_strand(), [dl, ds]() {
    scoped_time _t("gut_recv_dependency");
    dl->gut_recv_dependency(ds);
  });
}

void deadlock::tx_finish(xid_t xid) {
  auto dl = shared_from_this();
  boost::asio::post(get_strand(), [dl, xid]() {
    scoped_time _t("tx_finish");
    dl->gut_tx_finish(xid);
  });
}

void deadlock::set_next_node(node_id_t node_id) {
  std::scoped_lock l(mutex_);
  send_to_node_ = node_id;
}

void deadlock::send_dependency(uint64_t sequence) {
  POSSIBLE_UNUSED(sequence);
#ifdef DIST_DEADLOCK
  if (not distributed_) {
    wait_path_.clear();
    return;
  }
  if (send_to_node_ == 0) {
    wait_path_.clear();
    return;
  }
  uint64_t ms = steady_clock_ms_since_epoch();
  if (ms > time_ms_ && ms - time_ms_ < deadlock_detect_ms_ && sequence == 0) {
    return;
  }
  time_ms_ = ms;
  // LOG(trace) << " async send DL_DEPENDENCY " << sequence << " to " <<
  // id_2_name(send_to_node_);

  auto ds = std::make_shared<dependency_set>();
  wait_path_.to_dependency_set(*ds);
  ds->set_sequence(sequence);
  if (ds->IsInitialized()) {
    auto r = service_->async_send(send_to_node_, DL_DEPENDENCY, ds);
    if (not r) {
      LOG(error) << " async send DL_DEPENDENCY error " << r.error().message();
    }
  }
#endif
}

void deadlock::gut_recv_dependency(const ptr<dependency_set> ds) {
  if (ds->sequence() >= num_shard_ + 1) {
    wait_path_.clear();
    return;
  }
  BOOST_ASSERT(distributed_);
  add_dependency(ds);
  send_dependency(ds->sequence() + 1);
}

void deadlock::detect() {
  {
    std::scoped_lock l(mutex_);
    for (auto &ds : dep_) {
      wait_path_.add_dependency_set(*ds);
    }
    for (auto &w : set_) {
      wait_path_.add(w);
    }

    // for (xid_t xid : removed_) {
    //   wait_path_.tx_finish(xid);
    // }
    dep_.clear();
    // removed_.clear();
    set_.clear();
    detecting_ = true;
  }
  std::set<xid_t> xids;
  auto fn = [&xids, this](const std::vector<xid_t> &circle) {
    if (!circle.empty()) {
      uint64_t n = rnd_(rd_);
      uint64_t index = circle.size() * n / 100;
      if (index >= circle.size()) {
        index = circle.size() - 1;
      }
      xid_t xid = circle[index];
      xids.insert(xid);
      /*
      std::stringstream ssm;
      ssm << "detect circle: ";
      for (auto id : circle) {
        ssm << " ->" << id << std::endl;
      }
      ssm << "victim xid:" << xid << std::endl;
      LOG(info) << ssm.str();
       */
    }
  };
  std::unordered_set<xid_t> victim;

  wait_path_.detect_circle(fn);
  {
    std::scoped_lock l(mutex_);
    detecting_ = false;
    for (xid_t xid : xids) {
      wait_path_.tx_victim(xid);
    }
    victim = wait_path_.victim();
  }
  for (auto x : victim) {
    // LOG(info) << "deadlock victim:" << v.first;
    async_victim(x);
  }

  send_dependency(0);
}

void deadlock::async_victim(xid_t x) {

  // LOG(info) << "deadlock victim:" << v.first;
  auto f = fn_victim_;
  boost::asio::post(get_strand(), [f, x] {
    scoped_time _t("deadlock invoke victim");
    f(x);
  });

}

void deadlock::debug_deadlock(std::ostream &os) { wait_path_.handle_debug(os); }

void deadlock::async_wait_lock(fn_wait_lock fn_wait) {
  // issue another wait to avoid lost this message
  ptr<boost::asio::steady_timer> timer(new boost::asio::steady_timer(
      get_strand().context(),
      boost::asio::chrono::milliseconds(lock_wait_timeout_ms_)));
  auto dl = shared_from_this();
  auto fn_timeout = boost::asio::bind_executor(
      get_strand(),
      [dl, timer, fn_wait](const boost::system::error_code &error) {
        if (not error.failed()) {
          fn_handle_wait_set fn = [dl](ptr<tx_wait> wait) {
            boost::asio::post(dl->get_strand(), [dl, wait] {
              scoped_time _t("deadlock::add_wait_set");
              dl->add_wait_set(wait);
            });
          };
          auto r = fn_wait(fn);
          if (r) {
            // existing such tx_rm
            dl->async_wait_lock(fn_wait);
            timer.get();
          } else { // no such tx_rm, maybe this tx_rm have removed, we need not
            // wait it any longer
            return;
          }
        } else {
          LOG(error) << "wait lock timeout error";
        }
      });
  timer->async_wait(fn_timeout);
}