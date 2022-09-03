#pragma once

#include <boost/json.hpp>

class test_config {
 private:
  uint32_t wan_latency_ms_;
  float_t cached_tuple_percentage_;
  std::string label_;
 public:
  test_config() : wan_latency_ms_(0), cached_tuple_percentage_(0.0) {}

  [[nodiscard]] uint32_t debug_add_wan_latency_ms() const { return wan_latency_ms_; }

  float_t cached_tuple_percentage() const { return cached_tuple_percentage_; }

  const std::string &label() const { return label_; }

  void set_wan_latency_ms(uint32_t ms) {
    wan_latency_ms_ = ms;
  }

  void set_cached_tuple_percentage(float_t percentage) {
    cached_tuple_percentage_ = percentage;
  }

  void set_label(const std::string &label) {
    label_ = label;
  }

  [[nodiscard]] boost::json::object to_json() const;

  void from_json(boost::json::object &obj);
};