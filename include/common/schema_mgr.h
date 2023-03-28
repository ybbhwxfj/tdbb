#pragma once

#include "common/result.hpp"
#include "common/table_desc.h"
#include "common/table_id.h"
#include <unordered_map>

class schema_mgr {
private:
  std::unordered_map<std::string, table_desc> table_;
  std::unordered_map<table_id_t, table_desc> id2table_;

public:
  schema_mgr() = default;

  void from_json(boost::json::object &obj);

  result<void> from_json_string(const std::string &str);

  const std::unordered_map<table_id_t, table_desc> &id2table() const {
    return id2table_;
  }
};
