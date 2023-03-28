#pragma once

#include "common/block_exception.h"
#include "common/id.h"
#include <boost/assert.hpp>

const table_id_t TPCC_CUSTOMER = 1;
const table_id_t TPCC_STOCK = 2;
const table_id_t TPCC_WAREHOUSE = 3;
const table_id_t TPCC_ITEM = 4;
const table_id_t TPCC_DISTRICT = 5;
const table_id_t TPCC_ORDER = 6;
const table_id_t TPCC_NEW_ORDER = 7;
const table_id_t TPCC_ORDER_LINE = 8;
const table_id_t TPCC_CUSTOMER_LAST_NAME_INDEX = 9;
const table_id_t TPCC_HISTORY = 10;
const table_id_t TPCC_CUST_LAST_INDEX = 11;
const table_id_t YCSB_MAIN = 12;
const table_id_t MAX_TABLES = 13;
static const std::map<std::string, table_id_t> __name2id__ = {
    {"CUSTOMER", TPCC_CUSTOMER},
    {"STOCK", TPCC_STOCK},
    {"WAREHOUSE", TPCC_WAREHOUSE},
    {"ITEM", TPCC_ITEM},
    {"DISTRICT", TPCC_DISTRICT},
    {"ORDER", TPCC_ORDER},
    {"NEW_ORDER", TPCC_NEW_ORDER},
    {"ORDER_LINE", TPCC_ORDER_LINE},
    {"CUSTOMER_LAST_NAME_INDEX", TPCC_CUSTOMER_LAST_NAME_INDEX},
    {"HISTORY", TPCC_HISTORY},
    {"CUST_LAST_INDEX", TPCC_CUST_LAST_INDEX},
    {"MAIN", YCSB_MAIN},
};

inline table_id_t str_2_table_id(const std::string &name) {
  auto iter = __name2id__.find(name);
  if (iter!=__name2id__.end()) {
    return iter->second;
  } else {
    BOOST_ASSERT(false);
    throw block_exception(EC::EC_NOT_FOUND_ERROR);
  }
}