#pragma once

#include "common/id.h"

inline uint32_t make_customer_key(uint32_t wid,
                                  uint32_t did,
                                  uint32_t cid,
                                  uint32_t num_warehouse,
                                  uint32_t num_district) {
  return wid + did * (num_warehouse + 1) +
      cid * (num_warehouse + 1) * (num_district + 1);
}

inline uint32_t make_stock_key(uint32_t wid, uint32_t iid, uint32_t num_warehouse) {
  return wid + iid * (num_warehouse + 1);
}

inline uint32_t make_order_key(uint32_t wid, uint32_t oid, uint32_t num_warehouse) {
  return wid + oid * (num_warehouse + 1);
}

inline uint32_t make_district_key(uint32_t wid, uint32_t did, uint32_t num_warehouse) {
  return wid + did * (num_warehouse + 1);
}

inline uint32_t make_order_line_key(uint32_t wid, uint32_t did,
                                    uint32_t olid, uint32_t num_warehouse, uint32_t num_max_order_line) {
  return (wid + did * (num_warehouse + 1)) * num_max_order_line + olid;
}

