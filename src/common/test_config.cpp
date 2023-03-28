#include "common/test_config.h"
#include "common/variable.h"

test_config::test_config()
    : wan_latency_ms_(0), cached_tuple_percentage_(0.0),
      deadlock_detection_ms_(DEADLOCK_DETECTION_TIMEOUT_MILLIS),
      deadlock_detection_(DEADLOCK_DETECTION),
      lock_timeout_ms_(LOCK_WAIT_TIMEOUT_MILLIS) {}

boost::json::object test_config::to_json() const {
  boost::json::object obj;
  obj["wan_latency_delay_wait_ms"] = wan_latency_ms_;
  obj["percent_cached_tuple"] = cached_tuple_percentage_;
  obj["deadlock_detection_ms"] = deadlock_detection_ms_;
  obj["deadlock_detection"] = deadlock_detection_;
  obj["lock_timeout_ms"] = lock_timeout_ms_;
  return obj;
}

void test_config::from_json(boost::json::object &obj) {
  wan_latency_ms_ =
      boost::json::value_to<int32_t>(obj["wan_latency_delay_wait_ms"]);
  cached_tuple_percentage_ =
      boost::json::value_to<float_t>(obj["percent_cached_tuple"]);
  deadlock_detection_ms_ =
      boost::json::value_to<int32_t>(obj["deadlock_detection_ms"]);
  deadlock_detection_ = boost::json::value_to<bool>(obj["deadlock_detection"]);
  lock_timeout_ms_ = boost::json::value_to<int64_t>(obj["lock_timeout_ms"]);
}