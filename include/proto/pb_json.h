#pragma once
#include <boost/assert.hpp>
#include <google/protobuf/util/json_util.h>

template<typename M>
std::string pb_to_json(const M m) {
  std::string str;
  google::protobuf::util::Status status = google::protobuf::util::MessageToJsonString(m, &str);
  if (!status.ok()) {
    BOOST_ASSERT(false);
    return "";
  }
  return str;
}

template<typename M>
bool json_to_pb(const std::string &str, M &m) {
  if (!str.empty()) {
    google::protobuf::stringpiece_internal::StringPiece s(str.c_str());
    google::protobuf::util::Status status = google::protobuf::util::JsonStringToMessage(s, &m);
    if (!status.ok()) {
      BOOST_ASSERT(false);
      return false;
    }
  }
  return true;
}