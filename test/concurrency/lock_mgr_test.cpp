#define BOOST_TEST_MODULE LOCK_MGR_TEST
#include "common/lock_mode.h"
#include "concurrency/lock_mgr.h"
#include "concurrency/tx.h"
#include <boost/test/unit_test.hpp>
#include <random>
#include <string>

#define TUPLE_ID_MIN 1
#define TUPLE_ID_MAX 10
#define NUM_READ_KEY 10
#define NUM_WRITE_KEY 4
#define NUM_PREDICATE 1
#define NUM_TX 100

#include <boost/asio.hpp>

class tx_ctx_mock : public tx_rm {
public:
  explicit tx_ctx_mock(xid_t xid, boost::asio::io_context &ctx)
      : tx_rm(boost::asio::io_context::strand(ctx), xid) {}

  void async_lock_acquire(EC, oid_t) override {}
};

struct tx_lock_t {
  xid_t xid_{};
  oid_t oid_{};
  lock_mode mode_;
  predicate pred_;
  ptr<tx_rm> ctx_;
};

class random_value {
  std::default_random_engine generator_;
  std::uniform_int_distribution<uint64_t> distribution_;

public:
  random_value(uint64_t min, uint64_t max)
      : generator_((uint64_t) this), distribution_(min, max) {

  };
  uint64_t value() { return distribution_(generator_); }
};

enum range_type {
  RANGE_CLOSE = 0,
  RANGE_LEFT_OPEN = 1,
  RANGE_RIGHT_OPEN = 2,
  RANGE_OPEN = 3,
};

std::vector<tx_lock_t> gen_test_case(boost::asio::io_context &c) {
  std::vector<tx_lock_t> pred_list;

  random_value rnd(TUPLE_ID_MIN, TUPLE_ID_MAX);
  random_value rnd_rt(RANGE_CLOSE, RANGE_OPEN);
  for (uint64_t xid = 1; xid <= NUM_TX; xid++) {
    std::set<tuple_id_t> key_set;
    boost::icl::interval_set<tuple_id_t> interval_set;
    oid_t oid = 1;
    ptr<tx_rm> ctx(new tx_ctx_mock(xid, c));
    for (uint32_t i = 0; i < NUM_READ_KEY; i++) {
      uint64_t key = rnd.value();
      if (key_set.contains(key)) {
        continue;
      }
      key_set.insert(key);
      tx_lock_t l;
      l.xid_ = xid;
      l.ctx_ = ctx;
      l.oid_ = oid++;
      l.mode_ = lock_mode::LOCK_READ_ROW;
      l.pred_.key_ = key;
      pred_list.push_back(l);
    }

    for (uint32_t i = 0; i < NUM_WRITE_KEY; i++) {
      uint64_t key = rnd.value();
      if (key_set.contains(key)) {
        continue;
      }
      key_set.insert(key);
      tx_lock_t l;
      l.xid_ = xid;
      l.ctx_ = ctx;
      l.oid_ = oid++;
      l.mode_ = LOCK_WRITE_ROW;
      l.pred_.key_ = key;
      pred_list.push_back(l);
    }

    for (uint32_t i = 0; i < NUM_PREDICATE; i++) {
      uint64_t upper = rnd.value();
      uint64_t lower = rnd.value();
      if (upper != lower) {
        if (upper < lower) {
          std::swap(upper, lower);
        }
        tx_lock_t l;
        l.xid_ = xid;
        l.ctx_ = ctx;
        l.mode_ = LOCK_READ_PREDICATE;
        l.oid_ = oid++;
        auto rt = range_type(rnd_rt.value());
        if (rt == RANGE_CLOSE) {
          l.pred_.interval_ =
              boost::icl::interval<uint64_t>::closed(lower, upper);
        } else if (rt == RANGE_LEFT_OPEN) {
          l.pred_.interval_ =
              boost::icl::interval<uint64_t>::left_open(lower, upper);
        } else if (rt == RANGE_RIGHT_OPEN) {
          l.pred_.interval_ =
              boost::icl::interval<uint64_t>::right_open(lower, upper);
        } else if (rt == RANGE_OPEN) {
          l.pred_.interval_ =
              boost::icl::interval<uint64_t>::open(lower, upper);
        } else {
          continue;
        }
        bool overlapped;
        for (tuple_id_t t : key_set) {
          if (boost::icl::contains(l.pred_.interval_, t)) {
            overlapped = true;
          }
        }
        auto iis = interval_set.find(l.pred_.interval_);
        if (iis != interval_set.end()) {
          overlapped = true;
        }
        if (not overlapped && not boost::icl::is_empty(l.pred_.interval_)) {
          pred_list.push_back(l);
        }
      }
    }
    std::random_device rd;
    std::default_random_engine rng{rd()};
    std::shuffle(pred_list.begin(), pred_list.end(), rng);
  }
  return pred_list;
}

BOOST_AUTO_TEST_CASE(lock_mgr_test) {
  // this io_context have no effect, only work as test stub
  boost::asio::io_context ctx(1);
  lock_mgr mgr(1, 1, ctx, nullptr, nullptr, nullptr);
  std::vector<tx_lock_t> v = gen_test_case(ctx);
  for (auto &l : v) {
    mgr.lock(l.xid_, l.oid_, l.mode_, l.pred_, l.ctx_);
  }
  for (auto &l : v) {
    mgr.unlock(l.xid_, l.mode_, l.pred_);
  }
}