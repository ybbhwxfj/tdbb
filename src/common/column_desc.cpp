#include "common/column_desc.h"
#include "common/integer.h"
#include "common/data_type.h"
#include <boost/assert.hpp>

void column_desc::from_json(boost::json::object &json) {
  name_ = boost::json::value_to<std::string>(json["ColumnName"]).c_str();
  std::string dt_name = boost::json::value_to<std::string>(json["DataType"]).c_str();
  type_ = str_to_dt(dt_name);
  length_ = (uint32_t) boost::json::value_to<int64_t>(json["Length"]);
  switch (type_) {
    case dt::DT_INT32:
    case dt::DT_INT64: {
      int64_t v = boost::json::value_to<int64_t>(json["Value"]);
      if (type_ == DT_INT32) {
        int32_t v32 = int32_t(v);
        default_value_ = int32_to_binary(v32);
      } else if (type_ == DT_INT64) {
        int64_t v64 = int64_t(v);
        default_value_ = int64_to_binary(v64);
      }
      break;
    }
    case dt::DT_STRING: {
      default_value_ = boost::json::value_to<std::string>(json["Value"]).c_str();
      break;
    }
    case dt::DT_FLOAT64: {
      double v = boost::json::value_to<double_t>(json["Value"]);
      default_value_.append((const char *) &v, sizeof(v));
      break;
    }
    case dt::DT_FLOAT32: {
      float v = boost::json::value_to<double_t>(json["Value"]);
      default_value_.append((const char *) &v, sizeof(v));
      break;
    }
    default:BOOST_ASSERT(false);
      break;
  }
}