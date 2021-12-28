#pragma once
#include <string>
#include <set>
#include <boost/json.hpp>
#include "common/block_type.h"

class block_config {
private:
  std::string node_name_;
  std::string db_path_;
  std::string schema_path_;
  std::string register_node_name_;

public:
  block_config() {};

  void set_node_name(const std::string &name) { node_name_ = name; }
  void set_db_path(const std::string &path) { db_path_ = path; }
  void set_schema_path(const std::string &path) { schema_path_ = path; }
  void set_register_node(const std::string &name) { register_node_name_ = name; }
  const std::string &node_name() const { return node_name_; }
  const std::string &db_path() const { return db_path_; }
  const std::string &schema_path() const { return schema_path_; }
  boost::json::object to_json() const;
  const std::string &register_node() const { return register_node_name_; }

  void from_json(boost::json::object &obj);
}; 