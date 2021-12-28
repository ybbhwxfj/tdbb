#include "common/test_config.h"

boost::json::object test_config::to_json() const {
  boost::json::object obj;
  obj["wan_latency_ms"] = wan_latency_ms_;
  return obj;
}

void test_config::from_json(boost::json::object &obj) {
  wan_latency_ms_ = boost::json::value_to<int64_t>(obj["wan_latency_ms"]);
}