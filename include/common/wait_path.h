#pragma once

#include <set>
#include <unordered_map>
#include <unordered_set>
#include "common/tx_wait.h"

typedef std::function<void(const std::vector<xid_t> &)> fn_circle_found;

class wait_path {
private:
  friend tx_wait_set;
  tx_wait_set ws_;
public:
  wait_path() {
    ws_.limit_ = 0;
  }

  wait_path(uint32_t limit) {
    ws_.limit_ = limit;
  }
  void tx_finish(xid_t);
  void tx_victim(xid_t);
  const std::unordered_map<xid_t, uint32_t> &victim() const;
  void add_dependency_set(const dependency_set &ds);
  void to_dependency_set(dependency_set &ds);
  bool detect_circle(fn_circle_found fn);
  void merge_dependency_set(tx_wait_set &wait);
  void handle_debug(std::ostream &os);
private:
  void detect_circle(
      ptr<tx_wait> w,
      std::vector<xid_t> &path,
      std::set<std::vector<xid_t>> &circle
  );
};
