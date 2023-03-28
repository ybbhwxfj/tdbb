#include <random>

// [0..n)
static inline uint64_t random_n(uint64_t n) {
  static thread_local std::mt19937 generator;
  std::uniform_int_distribution<int> distribution(0, n - 1);
  return distribution(generator);
}