#pragma once

#include "common/id.h"
#include "common/logger.hpp"
#include "common/ptr.hpp"
#include "proto/proto.h"

enum color {
  COLOR_WHITE,
  COLOR_BLOCK,
  COLOR_GRAY,
};

struct tx_wait {
  tx_wait(xid_t xid)
      : xid_(xid), flag_(COLOR_WHITE), index_(0), low_link_(0),
        on_stack_(false) {}
  ~tx_wait() {}

  void init() {
    index_ = low_link_ = 0;
    on_stack_ = false;
  }

  xid_t xid() const { return xid_; }

  const std::unordered_set<xid_t> &in_set() const { return in_set_; }

  std::unordered_set<xid_t> &in_set() { return in_set_; }

  xid_t xid_;
  enum color flag_;
  uint64_t index_;
  uint64_t low_link_;
  bool on_stack_;
  std::unordered_set<xid_t> in_set_;
};

struct tx_wait_set {
  uint64_t limit_;
  std::unordered_map<xid_t, ptr<tx_wait>> tx_wait_;
  std::unordered_set<xid_t> victim_;
  std::unordered_set<xid_t> finish_;

  template<typename CONTAINER> void add(const CONTAINER &in, xid_t out) {
    auto iter = tx_wait_.find(out);
    if (iter!=tx_wait_.end()) {    // existing xid
      ptr<tx_wait> d = iter->second; // add its in
      d->in_set().insert(in.begin(), in.end());
    } else { // no such xid
      ptr<tx_wait> w(new tx_wait(out));
      w->in_set().template insert(in.begin(), in.end());
      tx_wait_.template insert(std::make_pair(out, w));
    }
  }

  void add(xid_t in, xid_t out) {
    auto iter = tx_wait_.find(out);
    if (iter!=tx_wait_.end()) {     // existing such xid
      ptr<tx_wait> &d = iter->second; // add to out
      d->in_set().insert(in);
      BOOST_ASSERT(in!=out);
    } else {
      ptr<tx_wait> w(new tx_wait(out));
      w->in_set().insert(in);
      tx_wait_.insert(std::make_pair(out, w));
    }
  }

  template<typename CONTAINER> void add(xid_t in, const CONTAINER &out) {
    for (auto v : out) { // loop in set
      auto iter = tx_wait_.find(v);
      if (iter!=tx_wait_.end()) {     // existing such xid
        ptr<tx_wait> &d = iter->second; // add to out
        d->in_set().insert(in);
        BOOST_ASSERT(in!=v);
      } else {
        ptr<tx_wait> w(new tx_wait(v));
        w->in_set().insert(in);
        tx_wait_.insert(std::make_pair(v, w));
      }
    }
  }

  void add(ptr<tx_wait> wait) {
    auto i = tx_wait_.find(wait->xid_);
    if (i!=tx_wait_.end()) {
      i->second->in_set().insert(wait->in_set().begin(), wait->in_set().end());
    } else {
      tx_wait_.insert(std::make_pair(wait->xid(), wait));
    }
  }

  void merge_dependency_set(tx_wait_set &s) {
    for (auto kv : s.tx_wait_) {
      add(kv.second);
    }
    victim_.insert(s.victim_.begin(), s.victim_.end());
    finish_.insert(s.finish_.begin(), s.victim_.end());
  }

  void add_dependency_set(const dependency_set &ds) {
    for (auto d : ds.dep()) {
      xid_t out = xid_t(d.out());
      add(d.in(), out);
    }
    for (auto xid : ds.victim()) {
      victim_.insert(xid);
    }
    for (auto xid : ds.finish()) {
      tx_wait_.erase(xid);
      victim_.erase(xid);
      finish_.insert(xid);
    }
  }

  void to_dependency_set(dependency_set &ds) {
    for (std::pair<xid_t, ptr<tx_wait>> p : tx_wait_) {
      ptr<tx_wait> w = p.second;
      dependency *d = ds.add_dep();
      d->set_out(w->xid());
      for (xid_t x : w->in_set()) {
        d->add_in(x);
      }
    }
    for (auto xid : victim_) {
      ds.add_victim(xid);
    }
    for (auto xid : finish_) {
      ds.add_finish(xid);
    }
  }

  void clear() {
    tx_wait_.clear();
    victim_.clear();
    finish_.clear();
  }
};