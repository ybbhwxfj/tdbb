#pragma once

#include "common/db_type.h"

#ifdef DB_TYPE_TK

#include "common/config.h"
#include "store/store.h"
#include "tkrzw_dbm_hash.h"
#include <boost/asio.hpp>

class tkrzw_store : public store {
private:
  config conf_;
  std::string path_;
  node_id_t node_id_;
  std::string node_name_;
  tkrzw::HashDBM *dbm_[MAX_TABLES];

public:
  tkrzw_store(const config &conf);

  ~tkrzw_store();

  virtual result<void> replay(ptr<std::vector<ptr<tx_operation>>> ops);

  result<void> put(table_id_t table_id, tuple_id_t tuple_id, tuple_pb &&tuple);

  result<ptr<tuple_pb>> get(table_id_t table_id, tuple_id_t tuple_id);

  result<void> sync();

  void close();
};

#endif // DB_TYPE_TK