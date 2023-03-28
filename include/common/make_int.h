#pragma once

#include <stdint.h>

inline uint64_t make_uint64(uint32_t a, uint32_t b) {
  return uint64_t(a) << 32 | uint64_t(b);
}

inline uint32_t higher_uint32(uint64_t i) { return uint32_t(i >> 32); }

inline uint32_t lower_uint32(uint64_t i) { return uint32_t(i & 0xffff0000); }
