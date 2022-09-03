#pragma once

#include <cstdint>

struct violate {
  violate() : read_v_(0), write_v_(0) {

  }

  uint32_t read_v_;
  uint32_t write_v_;
};