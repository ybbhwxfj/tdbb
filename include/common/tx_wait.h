#pragma once
#include "common/ptr.hpp"
#include "common/id.h"
#include "proto/proto.h"

enum color {
  COLOR_WHITE,
  COLOR_BLOCK,
  COLOR_GRAY,
};

struct tx_wait {
  tx_wait(xid_t xid) : xid_(xid), flag_(COLOR_WHITE) {}

  xid_t xid() const { return xid_; }
  const std::set<xid_t> &out() const { return out_; }
  std::set<xid_t> &out() { return out_; }

  xid_t xid_;
  enum color flag_;
  std::set<xid_t> out_;
};

struct tx_wait_set {
  uint64_t limit_;
  std::unordered_map<xid_t, ptr<tx_wait>> tx_wait_;
  std::unordered_map<xid_t, uint32_t> victim_;
  std::unordered_map<xid_t, uint32_t> finish_;

  template<typename CONTAINER>
  void add(const CONTAINER &in, xid_t out) {
    for (auto v: in) { // loop in set
      auto iter = tx_wait_.find(v);
      if (iter != tx_wait_.end()) { // existing such xid
        ptr<tx_wait> &d = iter->second; // add to out
        d->out().insert(out);
        BOOST_ASSERT(out != v);
      } else {
        ptr<tx_wait> w(new tx_wait(v));
        w->out().insert(out);
        tx_wait_.insert(std::make_pair(v, w));
      }
    }
  }

  void add(xid_t in, xid_t out) {
    auto iter = tx_wait_.find(in);
    if (iter != tx_wait_.end()) { // existing such xid
      ptr<tx_wait> &d = iter->second; // add to out
      d->out().insert(out);
      BOOST_ASSERT(out != in);
    } else {
      ptr<tx_wait> w(new tx_wait(in));
      w->out().insert(out);
      tx_wait_.insert(std::make_pair(in, w));
    }
  }

  template<typename CONTAINER>
  void add(xid_t xid, const CONTAINER &out) {
    auto iter = tx_wait_.find(xid);
    if (iter != tx_wait_.end()) { // existing xid
      ptr<tx_wait> d = iter->second; // add its out
      d->out().insert(out.begin(), out.end());
    } else { // no such xid
      ptr<tx_wait> w(new tx_wait(xid));
      w->out().template insert(out.begin(), out.end());
      tx_wait_.template insert(std::make_pair(xid, w));
    }
  }

  void add(ptr<tx_wait> wait) {
    auto i = tx_wait_.find(wait->xid_);
    if (i != tx_wait_.end()) {
      i->second->out().merge(wait->out());
    } else {
      tx_wait_.insert(std::make_pair(wait->xid(), wait));
    }
  }

  void merge_dependency_set(tx_wait_set &s) {
    for (auto kv: s.tx_wait_) {
      add(kv.second);
    }
    victim_.merge(s.victim_);
    finish_.merge(s.finish_);
  }

  void add_dependency_set(const dependency_set &ds) {
    for (auto d: ds.dep()) {
      xid_t in = xid_t(d.in());
      add(in, d.out());
    }
    for (const auto &x: ds.victim()) {
      auto i = victim_.find(x.xid());
      if (i == victim_.end()) {
        victim_.insert(std::make_pair(x.xid(), x.seq() + 1));
      } else {
        if (i->second < x.seq() + 1) {
          i->second = x.seq() + 1;
          if (i->second > limit_) {
            //xid_t xid = i->first;
            //tx_wait_.erase(xid);
            //victim_.erase(i);
          }
        }
      }
    }
    for (auto x: ds.finish()) {
      auto i = finish_.find(x.xid());
      if (i == finish_.end()) {
        finish_.insert(std::make_pair(x.xid(), x.seq() + 1));
      } else {
        if (i->second < x.seq() + 1) {
          i->second = x.seq() + 1;
          if (i->second > limit_) {
            xid_t xid = i->first;
            tx_wait_.erase(xid);
            //victim_.erase(xid);
            finish_.erase(i);
          }
        }
      }
    }
  }

  void to_dependency_set(dependency_set &ds) {
    for (std::pair<xid_t, ptr<tx_wait>> p: tx_wait_) {
      ptr<tx_wait> w = p.second;
      dependency *d = ds.add_dep();
      d->set_in(w->xid());
      for (xid_t x: w->out()) {
        d->add_out(x);
      }
    }
    for (const auto &i: victim_) {
      auto v = ds.mutable_victim()->Add();
      v->set_xid(i.first);
      v->set_seq(i.second);
    }
    for (const auto &i: finish_) {
      auto f = ds.mutable_finish()->Add();
      f->set_xid(i.first);
      f->set_seq(i.second);
    }
  }
};