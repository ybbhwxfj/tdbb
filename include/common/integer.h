#pragma once

#include<string>

inline std::string int32_to_binary(int32_t v) {
  return std::string((const char *) &v, sizeof(v));
}

inline std::string int64_to_binary(int64_t v) {
  return std::string((const char *) &v, sizeof(v));
}

inline int32_t binary_to_int32(const std::string &b) {
  int32_t v;
  v = *(const int32_t *) b.c_str();
  return v;
}

inline int64_t binary_to_int64(const std::string &b) {
  int64_t v;
  v = *(const int64_t *) b.c_str();
  return v;
}
