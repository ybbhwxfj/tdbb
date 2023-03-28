#include "common/wait_path.h"
#include "common/json_pretty.h"
#include <sstream>

void wait_path::clear() { ws_.clear(); }

void wait_path::tx_finish(xid_t xid) {
  ws_.finish_.insert(xid);
  ws_.tx_wait_.erase(xid);
  ws_.victim_.erase(xid);
}

void wait_path::tx_victim(xid_t xid) { ws_.victim_.insert(xid); }

const std::unordered_set<xid_t> &wait_path::victim() const {
  return ws_.victim_;
}

void wait_path::add_dependency_set(const dependency_set &ds) {
  ws_.add_dependency_set(ds);
}

void wait_path::merge_dependency_set(tx_wait_set &wait) {
  ws_.merge_dependency_set(wait);
}

void wait_path::add(ptr<tx_wait> wait) {
  ws_.add(wait);
}

bool wait_path::detect_circle(fn_circle_found fn) {
  std::vector<xid_t> path;
#if false
  std::set<std::vector<xid_t>> scc;
  for (auto kv : ws_.tx_wait_) {
    kv.second->flag_ = COLOR_WHITE;
  }
  for (auto kv : ws_.tx_wait_) {
    BOOST_ASSERT(kv.first != 0);
    ptr<tx_wait> p = kv.second;
    if (p->flag_ == COLOR_WHITE) {
      detect_circle(p, path, scc);
    }
  }
#else
  std::vector<std::vector<xid_t>> scc;
  find_scc(scc);
#endif
  if (fn) {
    for (auto i = scc.begin(); i != scc.end(); ++i) {
      fn(*i);
    }
  }
  return !scc.empty();
}

void wait_path::detect_circle(ptr<tx_wait> w, std::vector<xid_t> &path,
                              std::set<std::vector<xid_t>> &circle) {
  if (w->flag_ != COLOR_WHITE) {
    return;
  }

  w->flag_ = COLOR_GRAY;
  path.push_back(w->xid_);
  for (xid_t x : w->in_set_) {
    BOOST_ASSERT(x != 0);
    auto iw = ws_.tx_wait_.find(x);
    if (iw == ws_.tx_wait_.end()) {
      continue;
    }
    ptr<tx_wait> p = iw->second;
    color flag = p->flag_;
    if (flag == COLOR_WHITE) {
      detect_circle(p, path, circle);
    } else if (flag == COLOR_GRAY) {
      auto rend = std::find(path.rbegin(), path.rend(), x);
      if (rend != path.rend()) {
        auto vec = std::vector(path.rbegin(), rend + 1);
        circle.insert(vec);
      } else {
        BOOST_ASSERT(false);
      }

    } else {
      p->in_set_.erase(x);
    }
  }
  w->flag_ = COLOR_BLOCK;
  path.pop_back();
}

void wait_path::to_dependency_set(dependency_set &ds) {
  ws_.to_dependency_set(ds);
}

void wait_path::handle_debug(std::ostream &os) {
  dependency_set ds;
  to_dependency_set(ds);
  std::stringstream ssm;
  ssm << pb_to_json(ds);
  json_pretty(ssm, os);
}

void wait_path::find_scc(std::vector<std::vector<xid_t>> &scc) {
  std::set<xid_t> keys;
  for (auto kv : ws_.tx_wait_) {
    kv.second->init();
    keys.insert(kv.first);
  }
  for (auto k : keys) {
    auto iter = ws_.tx_wait_.find(k);
    if (iter != ws_.tx_wait_.end()) {
      for (xid_t xid : iter->second->in_set_) {
        if (!ws_.tx_wait_.contains(xid)) {
          ws_.tx_wait_.insert(std::make_pair(xid, cs_new<tx_wait>(xid)));
        }
      }
    }
  }

  uint64_t index = 1;
  std::stack<xid_t> stack;
  for (auto iter = ws_.tx_wait_.begin(); iter != ws_.tx_wait_.end(); iter++) {
    ptr<tx_wait> v = iter->second;
    if (v->index_ == 0) {
      strong_connect(&index, v, stack, scc);
    }
  }
}

void wait_path::strong_connect(uint64_t *index, ptr<tx_wait> v,
                               std::stack<xid_t> &stack,
                               std::vector<std::vector<xid_t>> &scc) {
  v->index_ = *index;
  v->low_link_ = *index;
  *index += 1;
  stack.push(v->xid_);
  v->on_stack_ = true;
  for (auto xid : v->in_set_) {
    auto iter = this->ws_.tx_wait_.find(xid);
    if (iter != this->ws_.tx_wait_.end()) {
      ptr<tx_wait> w = iter->second;
      if (w->index_ == 0) {
        strong_connect(index, w, stack, scc);
        v->low_link_ = std::min(v->low_link_, w->low_link_);
      } else if (w->on_stack_) {
        v->low_link_ = std::min(v->low_link_, w->index_);
      }
    }
  }

  // If v is a root node, pop the stack and generate an SCC
  if (v->low_link_ == v->index_) {
    // current strongly connected component
    std::vector<xid_t> current_scc;
    // start a new strongly connected component
    while (!stack.empty()) {
      auto xid = stack.top();
      auto iter = this->ws_.tx_wait_.find(xid);
      if (iter != this->ws_.tx_wait_.end()) {
        ptr<tx_wait> w = iter->second;
        w->on_stack_ = false;
        stack.pop();
        current_scc.push_back(xid);
      } else {
        break;
      }

      if (xid == v->xid_) {
        break;
      }
    }

    if (current_scc.size() > 1) {
      scc.push_back(current_scc);
    }
  }
}