#pragma once

#include "proto/tuple.pb.h"
#include "common/buffer.h"
#include "common/endian.h"

#include "common/id.h"
#include <vector>
#include <string>

typedef tuple tuple_pb;
typedef std::string key_binary;

inline tuple_id_t uint64_to_key(uint64_t i) {
  return i;
}

inline key_binary tupleid2binary(uint64_t i) {
  return std::string((const char *) &i, sizeof(i));
}

inline uint64_t binary2tupleid(std::string s) {
  uint64_t i;
  BOOST_ASSERT(sizeof(i) == s.size());
  memcpy(((void *) &i), s.c_str(), s.size());
  return i;
}

inline key_binary pbtuple_to_binary(const tuple_pb &pb) {
  return pb.SerializeAsString();
}

inline bool binary_to_pbtuple(const std::string binary, tuple_pb &pb) {
  return pb.ParseFromString(binary);
}

inline bool is_tuple_nil(const tuple_pb &pb) {
  return pb.ByteSizeLong() == 0;
}