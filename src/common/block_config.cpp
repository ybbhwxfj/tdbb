#include "common/block_config.h"

boost::json::object block_config::to_json() const {
  boost::json::object obj;
  obj["node_name"] = node_name_;
  obj["db_path"] = db_path_;
  obj["schema_path"] = schema_path_;
  obj["binding_node_name"] = register_node_name_;
  return obj;
}

void block_config::from_json(boost::json::object &obj) {
  node_name_ = boost::json::value_to<std::string>(obj["node_name"]).c_str();
  db_path_ = boost::json::value_to<std::string>(obj["db_path"]).c_str();
  schema_path_ = boost::json::value_to<std::string>(obj["schema_path"]).c_str();
  register_node_name_ = boost::json::value_to<std::string>(obj["binding_node_name"]).c_str();
}