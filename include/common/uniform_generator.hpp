#pragma once

#include <cstdint>
#include <random>

template<class INT_TYPE=uint32_t>
class uniform_generator {
 private:
  std::default_random_engine rand_;
  std::uniform_int_distribution<INT_TYPE> gen_;
 public:
  uniform_generator() :
      rand_(std::random_device{}()),
      gen_(0, 0) {

  }

  uniform_generator(INT_TYPE lower_bound, INT_TYPE upper_bound) :
      rand_(std::random_device{}()),
      gen_(lower_bound, upper_bound) {

  }

  INT_TYPE generate() {
    return gen_(rand_);
  }
};
