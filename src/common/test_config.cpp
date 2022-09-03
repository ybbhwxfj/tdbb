#include "common/test_config.h"

boost::json::object test_config::to_json() const {
  boost::json::object obj;
  obj["wan_latency_delay_wait_ms"] = wan_latency_ms_;
  obj["cached_tuple_percentage"] = cached_tuple_percentage_;
  return obj;
}

void test_config::from_json(boost::json::object &obj) {
  wan_latency_ms_ = boost::json::value_to<int64_t>(obj["wan_latency_delay_wait_ms"]);
  cached_tuple_percentage_ = boost::json::value_to<float_t>(obj["cached_tuple_percentage"]);
}