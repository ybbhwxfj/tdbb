#pragma once

#include "common/id.h"
#include <boost/icl/interval_map.hpp>

typedef boost::icl::interval<tuple_id_t>::type interval;

struct predicate {
  predicate() : key_(0) {}

  explicit predicate(tuple_id_t key) : key_(key) {}

  explicit predicate(interval intrvl) : interval_(intrvl) {}

  interval interval_;
  tuple_id_t key_;
};