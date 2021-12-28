#pragma once

#include <vector>
#include "common/callback.h"

class history {
private:
  std::mutex mutex_;
  std::vector<tx_op> history_;
public:
  history() {}
  void add_op(const tx_op &o);
  bool is_serializable();
};