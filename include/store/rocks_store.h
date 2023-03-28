#pragma once

#include "common/db_type.h"

#ifdef DB_TYPE_ROCKS

#include "common/config.h"
#include "common/key128.h"
#include "common/tuple.h"
#include "proto/proto.h"
#include "rocksdb/db.h"
#include "store/store.h"
#include <boost/asio.hpp>

class rocks_store : public store {
private:
  config conf_;
  std::string path_;
  node_id_t node_id_;
  std::string node_name_;
  rocksdb::DB *db_;
  key128_comparator cmp_;

public:
  rocks_store(const config &conf);

  ~rocks_store();

  result<void> replay(ptr<std::vector<ptr<tx_operation>>> ops);

  result<void> put(table_id_t table_id, tuple_id_t tuple_id, tuple_pb &&tuple);

  result<ptr<tuple_pb>> get(table_id_t table_id, tuple_id_t tuple_id);

  void close();

  result<void> sync();

private:
};

#endif // DB_TYPE_ROCKS