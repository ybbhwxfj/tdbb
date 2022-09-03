#include "common/schema_mgr.h"

void schema_mgr::from_json(boost::json::object &obj) {
  boost::json::array arr = obj["Table"].as_array();
  for (auto &i : arr) {
    table_desc td;
    td.from_json(i.as_object());
    table_[td.table_name()] = td;
  }

  for (auto &i : table_) {
    table_id_t id = str_2_table_id(i.second.table_name());
    i.second.set_table_id(id);
    id2table_[i.second.table_id()] = i.second;
  }
}

result<void> schema_mgr::from_json_string(const std::string &str) {
  try {
    boost::system::error_code ec;
    boost::json::value jv = boost::json::parse(str, ec);

    if (ec) {
      std::cout << "Parsing failed: " << ec.message() << "\n";
      return outcome::failure(EC::EC_CONFIG_ERROR);
    }

    from_json(jv.as_object());
    return outcome::success();
  } catch (std::exception const &e) {
    std::cout << "Parsing failed: " << e.what() << "\n";
    return outcome::failure(EC::EC_CONFIG_ERROR);
  }
}
