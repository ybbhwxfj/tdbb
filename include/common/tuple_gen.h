#pragma once
#include "common/id.h"
#include "common/table_desc.h"
#include "common/tuple.h"
#include <unordered_map>

class tuple_gen {
  std::unordered_map<table_id_t, std::string> default_tuple_;

public:
  tuple_gen(const std::unordered_map<table_id_t, table_desc> &desc) {
    for (std::pair<table_id_t, table_desc> p : desc) {
      tuple_proto pb;
      for (const column_desc &c : p.second.column_desc_list()) {
        datum *d = pb.add_item();
        d->set_type(c.column_pb_type());
        d->set_binary(c.default_value());
      }
      std::string t = pb.SerializeAsString();
      default_tuple_[p.first] = t;
    }
  }

  std::string gen_tuple(table_id_t table_id) {
    return default_tuple_[table_id];
  }

private:
};