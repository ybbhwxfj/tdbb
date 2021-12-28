#pragma once

#include "common/id.h"
#include "common/variable.h"
#include <boost/json.hpp>
#include <cstdint>

class tpcc_config {
private:
  uint32_t num_warehouse_;
  uint32_t num_item_;
  uint32_t num_order_item_;
  uint32_t num_order_initialize_;
  uint32_t num_district_;
  uint32_t num_max_order_line_;
  uint32_t num_terminal_;
  uint32_t num_customer_;
  uint32_t num_new_order_;
  float percent_non_exist_item_;
  float percent_distributed_;

  uint32_t raft_leader_tick_ms_;
  uint32_t raft_follow_tick_num_;
  uint32_t calvin_epoch_ms_;
  uint32_t num_output_result_;
  uint32_t az_rtt_ms_;
  uint32_t flow_control_rtt_count_;
public:
  tpcc_config() :
      num_warehouse_(0),
      num_item_(0),
      num_order_item_(0),
      num_order_initialize_(0),
      num_district_(0),
      num_max_order_line_(0),
      num_terminal_(0),
      num_customer_(0),
      num_new_order_(0),
      percent_non_exist_item_(0.0),
      percent_distributed_(0.0),
      raft_leader_tick_ms_(RAFT_TICK_MILISECONDS),
      raft_follow_tick_num_(RAFT_FOLLOW_TICK_NUM),
      calvin_epoch_ms_(CALVIN_EPOCH_MILLISECOND),
      num_output_result_(TPM_CAL_NUM),
      az_rtt_ms_(100),
      flow_control_rtt_count_(10) {

  }

  [[nodiscard]] uint32_t num_warehouse() const { return num_warehouse_; }
  [[nodiscard]] uint32_t num_item() const { return num_item_; }
  [[nodiscard]] uint32_t num_order_item() const { return num_order_item_; }
  [[nodiscard]] uint32_t num_order_initalize() const { return num_order_initialize_; }
  [[nodiscard]] uint32_t num_district() const { return num_district_; }
  [[nodiscard]] uint32_t num_max_order_line() const { return num_max_order_line_; }
  [[nodiscard]] uint32_t num_terminal() const { return num_terminal_; }
  [[nodiscard]] uint32_t num_customer() const { return num_customer_; }
  [[nodiscard]] uint32_t num_new_order() const { return num_new_order_; }

  [[nodiscard]] uint32_t raft_leader_tick_ms() const { return raft_leader_tick_ms_; }
  [[nodiscard]] uint32_t raft_follow_tick_num() const { return raft_follow_tick_num_; }
  [[nodiscard]] uint32_t calvin_epoch_ms() const { return calvin_epoch_ms_; }
  [[nodiscard]] uint32_t num_output_result() const { return num_output_result_; }
  [[nodiscard]] uint32_t az_rtt_ms() const { return az_rtt_ms_; }
  [[nodiscard]] uint32_t flow_control_rtt_count() const { return flow_control_rtt_count_; }
  float_t percent_non_exist_item() const { return percent_non_exist_item_; }
  float_t percent_distributed() const {
    return percent_distributed_;
  }
  void set_num_warehouse(uint32_t v) { num_warehouse_ = v; }
  void set_num_item(uint32_t v) { num_item_ = v; }
  void set_num_order_item(uint32_t v) { num_order_item_ = v; }
  void set_num_order_initalize(uint32_t v) { num_order_initialize_ = v; }
  void set_num_district(uint32_t v) { num_district_ = v; }
  void set_num_max_order_line(uint32_t v) { num_max_order_line_ = v; }
  void set_num_terminal(uint32_t v) { num_terminal_ = v; }
  void set_num_customer(uint32_t v) { num_customer_ = v; }
  void set_num_new_order(uint32_t v) { num_new_order_ = v; }
  void set_percent_non_exist_item(float_t v) { percent_non_exist_item_ = v; }
  void set_percent_distributed(float_t v) {
    percent_distributed_ = v;
  }
  void set_num_output_result(uint32_t v) { num_output_result_ = v; }
  void set_az_rtt_ms(uint32_t ms) { az_rtt_ms_ = ms; }
  void set_flow_control_rtt_count(uint32_t count) { flow_control_rtt_count_ = count; }
  boost::json::object to_json() const {
    boost::json::object j;
    j["num_warehouse"] = num_warehouse_;
    j["num_item"] = num_item_;
    j["num_order_item"] = num_order_item_;
    j["num_order_initialize"] = num_order_initialize_;
    j["num_district"] = num_district_;
    j["num_max_order_line"] = num_max_order_line_;
    j["num_terminal"] = num_terminal_;
    j["num_customer"] = num_customer_;
    j["num_new_order"] = num_new_order_;
    j["percent_non_exist_item"] = percent_non_exist_item_;
    j["percent_distributed"] = percent_distributed_;
    j["raft_follow_tick_num"] = raft_follow_tick_num_;
    j["raft_leader_tick_ms"] = raft_leader_tick_ms_;
    j["calvin_epoch_ms"] = calvin_epoch_ms_;
    j["num_output_result"] = num_output_result_;
    j["az_rtt_ms"] = az_rtt_ms_;
    j["flow_control_rtt_count"] = flow_control_rtt_count_;
    return j;
  }

  void from_json(boost::json::object &j) {
    num_warehouse_ = (uint32_t)boost::json::value_to<int64_t>(j["num_warehouse"]);
    num_item_ = (uint32_t)boost::json::value_to<int64_t>(j["num_item"]);
    num_order_item_ = (uint32_t)boost::json::value_to<int64_t>(j["num_order_item"]);
    num_order_initialize_ = (uint32_t)boost::json::value_to<int64_t>(j["num_order_initialize"]);
    num_district_ = (uint32_t)boost::json::value_to<int64_t>(j["num_district"]);
    num_max_order_line_ = (uint32_t)boost::json::value_to<int64_t>(j["num_max_order_line"]);
    num_terminal_ = (uint32_t)boost::json::value_to<int64_t>(j["num_terminal"]);
    num_customer_ = (uint32_t)boost::json::value_to<int64_t>(j["num_customer"]);
    num_new_order_ = (uint32_t)boost::json::value_to<int64_t>(j["num_new_order"]);
    percent_non_exist_item_ = (float_t)boost::json::value_to<double_t>(j["percent_non_exist_item"]);
    percent_distributed_ = (float_t)boost::json::value_to<double_t>(j["percent_distributed"]);
    raft_follow_tick_num_ = (uint32_t)boost::json::value_to<int64_t>(j["raft_follow_tick_num"]);
    raft_leader_tick_ms_ = (uint32_t)boost::json::value_to<int64_t>(j["raft_leader_tick_ms"]);
    calvin_epoch_ms_ = (uint32_t)boost::json::value_to<int64_t>(j["calvin_epoch_ms"]);
    num_output_result_ = (uint32_t)boost::json::value_to<int64_t>(j["num_output_result"]);
    az_rtt_ms_ = (uint32_t)boost::json::value_to<int64_t>(j["az_rtt_ms"]);
    flow_control_rtt_count_ = (uint32_t)boost::json::value_to<int64_t>(j["flow_control_rtt_count"]);
  }
};