#pragma once

#include "common/id.h"

inline uint64_t make_customer_key(uint64_t wid, uint64_t did, uint64_t cid,
                                  uint64_t num_warehouse,
                                  uint64_t num_district_per_warehouse) {
  return wid + did*(num_warehouse + 1) +
      cid*(num_warehouse + 1)*(num_district_per_warehouse + 1);
}

inline uint64_t make_stock_key(uint64_t wid, uint64_t iid,
                               uint64_t num_warehouse) {
  return wid + iid*(num_warehouse + 1);
}

inline uint64_t make_order_key(uint64_t wid, uint64_t did, uint64_t oid,
                               uint64_t num_warehouse,
                               uint64_t num_district_per_warehouse) {
  return wid + did*(num_warehouse + 1) +
      oid*(num_warehouse + 1)*(num_district_per_warehouse + 1);
}

inline uint64_t make_district_key(uint64_t wid, uint64_t did,
                                  uint64_t num_warehouse) {
  return wid + did*(num_warehouse + 1);
}

inline uint64_t make_order_line_key(uint64_t wid, uint64_t did, uint64_t oid,
                                    uint64_t olid, uint64_t num_warehouse,
                                    uint64_t num_district_per_warehouse,
                                    uint64_t num_order) {

  return wid + did*(num_warehouse + 1) +
      oid*(num_warehouse + 1)*(num_district_per_warehouse + 1) +
      (num_order + 1)*(num_warehouse + 1)*
          (num_district_per_warehouse + 1)*olid;
}
