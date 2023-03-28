#pragma once

#include <stdint.h>

class tx_rsc_mgr {
private:
  uint64_t tx_id_;

public:
  tx_rsc_mgr(uint64_t id) : tx_id_(id) {}
};
