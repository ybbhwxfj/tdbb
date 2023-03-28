#pragma once

#include "common/column_desc.h"
#include <vector>

class row_desc {
private:
  uint32_t fix_length_columns_;
  uint32_t var_length_columns_;
  uint32_t tuple_header_size_;
  std::vector<column_desc> column_;

public:
  const std::vector<column_desc> &columns() const { return column_; }

  void add_column_desc(column_desc &cd) { column_.push_back(cd); }

  void reorder() {
    std::sort(column_.begin(), column_.end(),
              [](column_desc &x, column_desc &y) {
                return x.get_data_type() < y.get_data_type();
              });
    size_t offset = 0;
    uint32_t fix_length = 0;
    uint32_t var_length = 0;
    for (uint32_t id = 0; id < column_.size(); id++) {
      column_desc &desc = column_[id];
      desc.set_column_id(id);
      if (desc.is_fixed_length()) {
        desc.set_offset(offset);
        offset += desc.get_data_size();
        fix_length++;
      } else {
        desc.set_offset(var_length);
        var_length++;
      }
    }
    fix_length_columns_ = fix_length;
    var_length_columns_ = var_length;
    tuple_header_size_ = var_length*4 + 4;
  }
};