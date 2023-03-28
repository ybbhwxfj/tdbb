#pragma once

#include "common/byte_buffer.h"
#include "common/endian.h"
#include "proto/tuple.pb.h"

#include "common/id.h"
#include <string>
#include <vector>

typedef tuple tuple_proto;
typedef std::string tuple_pb;
typedef std::string key_binary;
typedef std::string tuple_binary;

inline tuple_id_t uint64_to_key(uint64_t i) { return i; }

inline key_binary tupleid2binary(uint64_t i) {
  return std::string((const char *) &i, sizeof(i));
}

inline uint64_t binary2tupleid(std::string s) {
  uint64_t i;
  BOOST_ASSERT(sizeof(i)==s.size());
  memcpy(((void *) &i), s.c_str(), s.size());
  return i;
}

inline tuple_binary pb_tuple_to_binary(const tuple_proto &pb) {
  return pb.SerializeAsString();
}

inline bool binary_to_pb_tuple(const tuple_binary &binary, tuple_proto &pb) {
  return pb.ParseFromString(binary);
}

inline bool pb_tuple_to_binary(const tuple_proto &pb, tuple_binary &binary) {
  return pb.SerializePartialToString(&binary);
}

inline bool is_tuple_nil(const tuple_binary &binary) { return binary.empty(); }

inline bool is_tuple_nil(const tuple_proto &pb) {
  return pb.ByteSizeLong()==0;
}