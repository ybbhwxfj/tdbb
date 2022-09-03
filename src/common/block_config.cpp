#include "common/block_config.h"
#include "common/variable.h"

block_config::block_config() :
    threads_io_(0),
    threads_async_context_(0),
    connections_per_peer_(0),
    append_log_entries_batch_min_(APPEND_LOG_ENTRIES_BATCH_MIN) {
};

boost::json::object block_config::to_json() const {
  boost::json::object obj;
  obj["node_name"] = node_name_;
  obj["db_path"] = db_path_;
  obj["schema_path"] = schema_path_;
  obj["binding_node_name"] = register_node_name_;
  obj["threads_async_context"] = threads_async_context_;
  obj["threads_io"] = threads_io_;
  obj["connections_per_peer"] = connections_per_peer_;
  obj["append_log_entries_batch_min"] = append_log_entries_batch_min_;
  return obj;
}

void block_config::from_json(boost::json::object &obj) {
  node_name_ = boost::json::value_to<std::string>(obj["node_name"]).c_str();
  db_path_ = boost::json::value_to<std::string>(obj["db_path"]).c_str();
  schema_path_ = boost::json::value_to<std::string>(obj["schema_path"]).c_str();
  register_node_name_ = boost::json::value_to<std::string>(obj["binding_node_name"]).c_str();
  threads_async_context_ = boost::json::value_to<uint64_t>(obj["threads_async_context"]);
  threads_io_ = boost::json::value_to<uint64_t>(obj["threads_io"]);
  connections_per_peer_ = boost::json::value_to<uint64_t>(obj["connections_per_peer"]);
  append_log_entries_batch_min_ = boost::json::value_to<uint64_t>(obj["append_log_entries_batch_min"]);
}