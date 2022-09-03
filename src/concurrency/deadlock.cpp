#include "concurrency/deadlock.h"
#include <sstream>
#include <utility>

deadlock::deadlock(fn_victim fn,
                   net_service *service,
                   uint32_t num_shard)
    : ctx_strand(service->get_service(SERVICE_ASYNC_CONTEXT)),
      detecting_(false),
      distributed_(false),
      fn_victim_(std::move(fn)),
      service_(service),
      wait_path_(num_shard * 2),
      g_(rd_()),
      rnd_(0, 99),
      num_shard_(num_shard) {
#ifdef DB_TYPE_SHARE_NOTHING
  if (is_shared_nothing()) {
    distributed_ = true;
  }
#endif
}

void deadlock::tick() {
  auto s = shared_from_this();
  auto fn_timeout = [s]() {
    s->detect();
    s->send_dependency(0);
  };
  ptr<timer> t(new timer(
      service_->get_service(SERVICE_ASYNC_CONTEXT),
      boost::asio::chrono::milliseconds(100),
      fn_timeout));
  timer_ = t;
}

void deadlock::stop_and_join() {
  timer_->cancel_and_join();
}

void deadlock::add_wait_set(ptr<tx_wait_set> ws) {
  if (detecting_) {
    set_.push_back(ws);
  } else {
    wait_path_.merge_dependency_set(*ws);
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
  boost::asio::post(get_strand(),
                    [dl, ds]() { dl->gut_recv_dependency(ds); });
}

void deadlock::tx_finish(xid_t xid) {
  auto dl = shared_from_this();
  boost::asio::post(get_strand(),
                    [dl, xid]() { dl->gut_tx_finish(xid); });
}

void deadlock::set_next_node(node_id_t node_id) {
  std::scoped_lock l(mutex_);
  send_to_node_ = node_id;
}

void deadlock::send_dependency(uint64_t sequence) {
  if (not distributed_) {
    return;
  }
  if (send_to_node_ == 0) {
    return;
  }

  //BOOST_LOG_TRIVIAL(trace) << " async send DL_DEPENDENCY " << sequence << " to " << id_2_name(send_to_node_);

  auto ds = std::make_shared<dependency_set>();
  wait_path_.to_dependency_set(*ds);
  ds->set_sequence(sequence);
  if (ds->IsInitialized()) {
    auto r = service_->async_send(send_to_node_, DL_DEPENDENCY, ds);
    if (not r) {
      BOOST_LOG_TRIVIAL(error) << " async send DL_DEPENDENCY error " << r.error().message();
    }
  }

  // clear after sending
  wait_path_.clear();
}

void deadlock::gut_recv_dependency(const ptr<dependency_set> ds) {
  if (ds->sequence() >= num_shard_ + 1) {
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
    for (ptr<tx_wait_set> &w : set_) {
      wait_path_.merge_dependency_set(*w);
    }

    //for (xid_t xid : removed_) {
    //  wait_path_.tx_finish(xid);
    //}
    dep_.clear();
    //removed_.clear();
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
      BOOST_LOG_TRIVIAL(info) << ssm.str();
       */
    }
  };
  wait_path_.detect_circle(fn);

  std::unordered_set<xid_t> victim;
  {
    std::scoped_lock l(mutex_);
    detecting_ = false;
    for (xid_t xid : xids) {
      wait_path_.tx_victim(xid);
    }
    victim = wait_path_.victim();
  }
  for (auto x : victim) {
    //BOOST_LOG_TRIVIAL(info) << "deadlock victim:" << v.first;
    auto f = fn_victim_;
    boost::asio::post(
        get_strand(),
        [f, x] { f(x); });
  }
}

void deadlock::debug_deadlock(std::ostream &os) {
  wait_path_.handle_debug(os);
}

void deadlock::async_wait_lock(fn_wait_lock fn) {
  ptr<tx_wait_set> ws(new tx_wait_set());
  auto r = fn(*ws);
  if (r) {
    // existing such tx_rm
    auto d = shared_from_this();
    boost::asio::post(get_strand(), [d, ws] {
      d->add_wait_set(ws);
    });
  } else { // no such tx_rm, maybe this tx_rm have removed, we need not wait it any longer
    return;
  }
  // issue another wait to avoid lost this message
  ptr<boost::asio::steady_timer> timer(new boost::asio::steady_timer(
      service_->get_service(SERVICE_ASYNC_CONTEXT),
      boost::asio::chrono::milliseconds(500)));
  auto dl = shared_from_this();
  auto fn_timeout =
      boost::asio::bind_executor(
          get_strand(),
          [dl, timer, fn](const boost::system::error_code &error) {
            if (not error.failed()) {
              dl->async_wait_lock(fn);
              timer.get();
            } else {
              BOOST_LOG_TRIVIAL(error) << "wait lock timeout error";
            }
          });
  timer->async_wait(fn_timeout);
}