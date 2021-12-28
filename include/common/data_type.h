#pragma once

#include <map>
#include <boost/assert.hpp>
#include "proto/data_type.pb.h"

enum dt {
  DT_INT32 = data_type::INT32,
  DT_INT64 = data_type::INT64,
  DT_UINT32 = data_type::UINT32,
  DT_UINT64 = data_type::UINT64,
  DT_FLOAT32 = data_type::FLOAT32,
  DT_FLOAT64 = data_type::FLOAT64,
  DT_STRING = data_type::STRING,
};

static const std::map<std::string, dt> _str2dt_ = {
    {"INT32", DT_INT32},
    {"INT64", DT_INT64},
    {"UINT32", DT_UINT32},
    {"UINT64", DT_UINT64},
    {"FLOAT32", DT_FLOAT32},
    {"FLOAT64", DT_FLOAT64},
    {"STRING", DT_STRING}
};

inline dt str_to_dt(const std::string &name) {
  BOOST_ASSERT(_str2dt_.contains(name));
  auto iter = _str2dt_.find(name);
  if (iter != _str2dt_.end()) {
    return iter->second;
  } else {
    BOOST_ASSERT(false);
    return dt::DT_INT32;
  }

}