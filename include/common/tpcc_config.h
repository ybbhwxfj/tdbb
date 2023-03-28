#pragma once

#include "common/id.h"
#include "common/variable.h"
#include <boost/json.hpp>
#include <cstdint>

class tpcc_config {
private:
  uint64_t num_warehouse_;
  uint64_t num_item_;
  uint64_t num_order_initialize_per_district_;
  uint64_t num_district_per_warehouse_;
  uint64_t num_max_order_line_;
  uint64_t num_terminal_;
  uint64_t num_customer_per_district_;
  uint64_t num_new_order_;
  double_t percent_non_exist_item_;
  double_t percent_remote_warehouse_;
  double_t percent_hot_row_;
  double_t percent_read_only_;
  uint32_t read_only_rows_;
  bool additional_read_only_terminal_;
  uint64_t hot_item_num_;
  uint64_t raft_leader_tick_ms_;
  uint64_t raft_follow_tick_num_;
  uint64_t calvin_epoch_ms_;
  uint64_t num_output_result_;
  uint64_t az_rtt_ms_;
  uint64_t flow_control_rtt_count_;
  bool control_percent_dist_tx_;

public:
  tpcc_config()
      : num_warehouse_(0), num_item_(0), num_order_initialize_per_district_(0),
        num_district_per_warehouse_(0), num_max_order_line_(0),
        num_terminal_(0), num_customer_per_district_(0), num_new_order_(0),
        percent_non_exist_item_(0.0), percent_remote_warehouse_(0.0),
        percent_hot_row_(0.0), percent_read_only_(PERCENTAGE_READ_ONLY),
        read_only_rows_(READ_ONLY_ROWS),
        additional_read_only_terminal_(ADDITIONAL_READ_ONLY_TERMINAL),
        hot_item_num_(1),
        raft_leader_tick_ms_(RAFT_LEADER_ELECTION_TICK_MILLI_SECONDS),
        raft_follow_tick_num_(RAFT_FOLLOW_TICK_NUM),
        calvin_epoch_ms_(CALVIN_EPOCH_MILLISECOND),
        num_output_result_(TPM_CAL_NUM), az_rtt_ms_(100),
        flow_control_rtt_count_(10), control_percent_dist_tx_(DIST_TX_PERCENTAGE) {}

  [[nodiscard]] uint64_t num_warehouse() const { return num_warehouse_; }

  [[nodiscard]] uint64_t num_item() const { return num_item_; }

  [[nodiscard]] uint64_t num_order_initialize_per_district() const {
    return num_order_initialize_per_district_;
  }

  [[nodiscard]] uint64_t num_district_per_warehouse() const {
    return num_district_per_warehouse_;
  }

  [[nodiscard]] uint64_t num_max_order_line() const {
    return num_max_order_line_;
  }

  [[nodiscard]] uint64_t num_terminal() const { return num_terminal_; }

  [[nodiscard]] uint64_t num_customer_per_district() const {
    return num_customer_per_district_;
  }

  [[nodiscard]] uint64_t num_new_order() const { return num_new_order_; }

  [[nodiscard]] uint64_t raft_leader_election_tick_ms() const {
    return raft_leader_tick_ms_;
  }

  [[nodiscard]] uint64_t raft_follow_tick_num() const {
    return raft_follow_tick_num_;
  }

  [[nodiscard]] uint64_t calvin_epoch_ms() const { return calvin_epoch_ms_; }

  [[nodiscard]] uint64_t num_output_result() const {
    return num_output_result_;
  }

  [[nodiscard]] uint64_t az_rtt_ms() const { return az_rtt_ms_; }

  [[nodiscard]] uint64_t flow_control_rtt_count() const {
    return flow_control_rtt_count_;
  }

  [[nodiscard]] bool control_percent_dist_tx() const {
    return control_percent_dist_tx_;
  }

  double_t percent_non_exist_item() const { return percent_non_exist_item_; }

  double_t percent_remote() const { return percent_remote_warehouse_; }

  double_t percent_hot_row() const { return percent_hot_row_; }

  double_t percent_read_only() const { return percent_read_only_; }

  uint32_t num_read_only_rows() const { return read_only_rows_; }

  bool additional_read_only_terminal() const { return additional_read_only_terminal_; }

  [[nodiscard]] uint64_t hot_item_num() const { return hot_item_num_; }

  void set_num_warehouse(uint64_t v) { num_warehouse_ = v; }

  void set_num_item(uint64_t v) { num_item_ = v; }

  void set_num_order_initialize(uint64_t v) {
    num_order_initialize_per_district_ = v;
  }

  void set_num_district_per_warehouse(uint64_t v) {
    num_district_per_warehouse_ = v;
  }

  void set_num_max_order_line(uint64_t v) { num_max_order_line_ = v; }

  void set_num_terminal(uint64_t v) { num_terminal_ = v; }

  void set_num_customer_per_district(uint64_t v) {
    num_customer_per_district_ = v;
  }

  void set_num_new_order(uint64_t v) { num_new_order_ = v; }

  void set_percent_non_exist_item(double_t v) { percent_non_exist_item_ = v; }

  void set_percent_remote_wh(double_t v) { percent_remote_warehouse_ = v; }

  void set_percent_hot_row(double_t v) { percent_hot_row_ = v; }

  void set_hot_item_num(uint64_t v) { hot_item_num_ = v; }

  void set_control_percent_dist_tx(bool v) { control_percent_dist_tx_ = v; }

  void set_num_output_result(uint64_t v) { num_output_result_ = v; }

  void set_az_rtt_ms(uint64_t ms) { az_rtt_ms_ = ms; }

  void set_flow_control_rtt_count(uint64_t count) {
    flow_control_rtt_count_ = count;
  }

  boost::json::object to_json() const {
    boost::json::object j;
    j["num_warehouse"] = num_warehouse_;
    j["num_item"] = num_item_;
    j["num_max_order_line"] = num_max_order_line_;
    j["num_order_initialize_per_district"] = num_order_initialize_per_district_;
    j["num_district_per_warehouse"] = num_district_per_warehouse_;
    j["num_max_order_line"] = num_max_order_line_;
    j["num_terminal"] = num_terminal_;
    j["num_customer_per_district"] = num_customer_per_district_;
    j["num_new_order"] = num_new_order_;
    j["percent_non_exist_item"] = percent_non_exist_item_;
    j["percent_remote"] = percent_remote_warehouse_;
    j["percent_hot_item"] = percent_hot_row_;
    j["percent_read_only"] = percent_read_only_;
    j["num_read_only_rows"] = read_only_rows_;
    j["additional_read_only_terminal"] = additional_read_only_terminal_;
    j["hot_item_num"] = hot_item_num_;
    j["raft_follow_tick_num"] = raft_follow_tick_num_;
    j["raft_leader_election_tick_ms"] = raft_leader_tick_ms_;
    j["calvin_epoch_ms"] = calvin_epoch_ms_;
    j["num_output_result"] = num_output_result_;
    j["az_rtt_ms"] = az_rtt_ms_;
    j["flow_control_rtt_count"] = flow_control_rtt_count_;
    j["control_percent_dist_tx"] = control_percent_dist_tx_;
    return j;
  }

  void from_json(boost::json::object &j) {
    num_warehouse_ =
        (uint64_t) boost::json::value_to<int64_t>(j["num_warehouse"]);
    num_item_ = (uint64_t) boost::json::value_to<int64_t>(j["num_item"]);
    num_max_order_line_ =
        (uint64_t) boost::json::value_to<int64_t>(j["num_max_order_line"]);
    num_order_initialize_per_district_ =
        (uint64_t) boost::json::value_to<int64_t>(
            j["num_order_initialize_per_district"]);
    num_district_per_warehouse_ = (uint64_t) boost::json::value_to<int64_t>(
        j["num_district_per_warehouse"]);
    num_max_order_line_ =
        (uint64_t) boost::json::value_to<int64_t>(j["num_max_order_line"]);
    num_terminal_ = (uint64_t) boost::json::value_to<int64_t>(j["num_terminal"]);
    num_customer_per_district_ = (uint64_t) boost::json::value_to<int64_t>(
        j["num_customer_per_district"]);
    num_new_order_ =
        (uint64_t) boost::json::value_to<int64_t>(j["num_new_order"]);
    percent_non_exist_item_ =
        (double_t) boost::json::value_to<double_t>(j["percent_non_exist_item"]);
    percent_remote_warehouse_ =
        (double_t) boost::json::value_to<double_t>(j["percent_remote"]);
    percent_hot_row_ =
        (double_t) boost::json::value_to<double_t>(j["percent_hot_item"]);
    percent_read_only_ =
        (double_t) boost::json::value_to<double_t>(j["percent_read_only"]);
    read_only_rows_ =
        (uint32_t) boost::json::value_to<uint32_t>(j["num_read_only_rows"]);
    additional_read_only_terminal_ =
        boost::json::value_to<bool>(j["additional_read_only_terminal"]);
    hot_item_num_ =
        (uint64_t) boost::json::value_to<uint64_t>(j["hot_item_num"]);
    raft_follow_tick_num_ =
        (uint64_t) boost::json::value_to<int64_t>(j["raft_follow_tick_num"]);
    raft_leader_tick_ms_ = (uint64_t) boost::json::value_to<int64_t>(
        j["raft_leader_election_tick_ms"]);
    calvin_epoch_ms_ =
        (uint64_t) boost::json::value_to<int64_t>(j["calvin_epoch_ms"]);
    num_output_result_ =
        (uint64_t) boost::json::value_to<int64_t>(j["num_output_result"]);
    az_rtt_ms_ = (uint64_t) boost::json::value_to<int64_t>(j["az_rtt_ms"]);
    flow_control_rtt_count_ =
        (uint64_t) boost::json::value_to<int64_t>(j["flow_control_rtt_count"]);
    control_percent_dist_tx_ =
        boost::json::value_to<bool>(j["control_percent_dist_tx"]);
  }
};