#pragma once

#include "common/callback.h"
#include "common/table_id.h"
#include "common/tuple.h"
#include "proto/proto.h"

class store {
public:
  virtual result<void> replay(ptr<std::vector<ptr<tx_operation>>> ops) = 0;

  virtual result<void> put(table_id_t table_id, tuple_id_t tuple_id,
                           tuple_pb &&tuple) = 0;

  virtual result<ptr<tuple_pb>> get(table_id_t table_id,
                                    tuple_id_t tuple_id) = 0;

  virtual result<void> sync() = 0;

  virtual void close() = 0;

  virtual ~store() {}
};