#pragma once

#include <boost/json.hpp>

class test_config {
private:
  uint32_t wan_latency_ms_;
  float_t cached_tuple_percentage_;
  uint32_t deadlock_detection_ms_;
  bool deadlock_detection_;
  uint64_t lock_timeout_ms_;
  std::string label_;

public:
  test_config();

  [[nodiscard]] uint32_t debug_add_wan_latency_ms() const {
    return wan_latency_ms_;
  }

  float_t percent_cached_tuple() const { return cached_tuple_percentage_; }

  const std::string &label() const { return label_; }

  void set_wan_latency_ms(uint32_t ms) { wan_latency_ms_ = ms; }

  void set_deadlock_detection_ms(uint32_t ms) { deadlock_detection_ms_ = ms; }

  uint32_t deadlock_detection_ms() const { return deadlock_detection_ms_; }

  void set_deadlock_detection(bool dl) { deadlock_detection_ = dl; }

  bool deadlock_detection() const { return deadlock_detection_; }

  void set_cached_tuple_percentage(float_t percentage) {
    cached_tuple_percentage_ = percentage;
  }

  void set_label(const std::string &label) { label_ = label; }

  void set_lock_timeout_ms(uint64_t ms) { lock_timeout_ms_ = ms; }
  uint64_t lock_timeout_ms() const { return lock_timeout_ms_; }

  [[nodiscard]] boost::json::object to_json() const;

  void from_json(boost::json::object &obj);
};