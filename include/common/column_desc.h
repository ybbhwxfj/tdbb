#pragma once

#include "common/data_type.h"
#include <boost/json.hpp>
#include "common/id.h"

class column_desc {
 private:
  dt type_;
  uint32_t length_;
  std::string name_;
  std::string default_value_;
  column_id_t id_;
 public:
  column_desc() : type_(dt::DT_INT32), length_(0), id_(0) {}

  void from_json(boost::json::object &json);

  dt column_type() const { return type_; }

  column_id_t column_id() const { return id_; }

  void set_column_id(column_id_t id) { id_ = id; }

  data_type column_pb_type() const { return data_type(type_); }

  const std::string &column_name() const { return name_; }

  const std::string &default_value() const { return default_value_; }
};
