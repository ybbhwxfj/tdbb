#pragma once

#include "common/block_type.h"
#include "common/variable.h"
#include <boost/json.hpp>
#include <set>
#include <string>

class block_config {
private:
  std::string node_name_;
  std::string db_path_;
  std::string schema_path_;
  std::string register_node_name_;

  uint64_t threads_cc_;

  // 0 for default
  uint64_t threads_io_;
  // 0 for default

  uint64_t threads_replication_;

  uint64_t threads_async_context_;
  // 0 for default
  uint64_t connections_per_peer_;

  uint64_t append_log_entries_batch_min_;
  uint64_t append_log_entries_batch_max_;

public:
  block_config();

  void set_node_name(const std::string &name) { node_name_ = name; }

  void set_db_path(const std::string &path) { db_path_ = path; }

  void set_schema_path(const std::string &path) { schema_path_ = path; }

  void set_register_node(const std::string &name) {
    register_node_name_ = name;
  }

  void set_threads_io(uint64_t n) { threads_io_ = n; }

  void set_threads_async_context(uint64_t n) { threads_async_context_ = n; }

  void set_connections_per_peer(uint64_t n) { connections_per_peer_ = n; }

  void set_append_log_entries_batch_max(uint32_t size) {
    append_log_entries_batch_max_ = size;
  }

  uint64_t append_log_entries_batch_min() const {
    return append_log_entries_batch_min_;
  }

  const std::string &node_name() const { return node_name_; }

  const std::string &db_path() const { return db_path_; }

  const std::string &schema_path() const { return schema_path_; }

  boost::json::object to_json() const;

  const std::string &register_node() const { return register_node_name_; }

  uint64_t threads_cc() { return threads_cc_; }

  uint64_t threads_io() { return threads_io_; }

  uint64_t threads_replication() { return threads_replication_; }

  uint64_t threads_async_context() { return threads_async_context_; }

  uint64_t connections_per_peer() {
    if (connections_per_peer_==0) {
      connections_per_peer_ = CONNECTIONS_PER_PEER;
    }
    return connections_per_peer_;
  }
  [[nodiscard]] uint32_t append_log_entries_batch_max() const {
    return append_log_entries_batch_max_;
  }

  void from_json(boost::json::object &obj);
};