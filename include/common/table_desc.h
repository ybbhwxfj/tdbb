#pragma once

#include <vector>
#include <string>
#include "common/table_id.h"
#include "common/column_desc.h"
#include <boost/json.hpp>

class table_desc {
private:
  std::string table_name_;
  std::vector<column_desc> column_desc_;
  table_id_t table_id_;
public:
  table_desc() : table_id_(0) {}
  void from_json(boost::json::object &obj);
  void set_table_id(table_id_t id) { table_id_ = id; }
  table_id_t table_id() const { return table_id_; }
  const std::string &table_name() const { return table_name_; }
  const std::vector<column_desc> &column_desc_list() const { return column_desc_; }
};
