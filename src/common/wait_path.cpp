#include "common/wait_path.h"
#include "common/json_pretty.h"
#include <sstream>

void wait_path::clear() {
  ws_.clear();
}

void wait_path::tx_finish(xid_t xid) {
  ws_.finish_.insert(xid);
  ws_.tx_wait_.erase(xid);
  ws_.victim_.erase(xid);
}

void wait_path::tx_victim(xid_t xid) {
  ws_.victim_.insert(xid);
}

const std::unordered_set<xid_t> &wait_path::victim() const {
  return ws_.victim_;
}

void wait_path::add_dependency_set(const dependency_set &ds) {
  ws_.add_dependency_set(ds);
}

void wait_path::merge_dependency_set(tx_wait_set &wait) {
  ws_.merge_dependency_set(wait);
}

bool wait_path::detect_circle(fn_circle_found fn) {
  std::vector<xid_t> path;
  std::set<std::vector<xid_t>> circle;
  for (auto kv : ws_.tx_wait_) {
    kv.second->flag_ = COLOR_WHITE;
  }
  for (auto kv : ws_.tx_wait_) {
    BOOST_ASSERT(kv.first != 0);
    ptr<tx_wait> p = kv.second;
    if (p->flag_ == COLOR_WHITE) {
      detect_circle(p, path, circle);
    }
  }
  if (fn) {
    for (auto i = circle.begin(); i != circle.end(); ++i) {
      fn(*i);
    }
  }
  return !circle.empty();
}

void wait_path::detect_circle(
    ptr<tx_wait> w,
    std::vector<xid_t> &path,
    std::set<std::vector<xid_t>> &circle
) {
  if (w->flag_ != COLOR_WHITE) {
    return;
  }

  w->flag_ = COLOR_GRAY;
  path.push_back(w->xid_);
  for (xid_t x : w->out_) {
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
      p->out_.erase(x);
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