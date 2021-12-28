#include "common/table_desc.h"

void table_desc::from_json(boost::json::object &obj) {
  table_name_ = boost::json::value_to<std::string>(obj["TableName"]).c_str();
  boost::json::array &a = obj["Column"].as_array();
  column_id_t id = 0;
  for (auto i = a.begin(); i != a.end(); i++, id++) {
    column_desc_.emplace_back(column_desc());
    column_desc_.rbegin()->from_json(i->as_object());
    column_desc_.rbegin()->set_column_id(id + 1);
  }
}
