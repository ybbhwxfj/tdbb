#pragma once

#include "common/callback.h"
#include "common/callback.h"
#include "common/table_id.h"
#include "common/tuple.h"
#include "proto/proto.h"

class store {
public:
  virtual result<void> replay(const replay_to_dsb_request msg) = 0;

  virtual result<void> put(table_id_t table_id, tuple_id_t tuple_id,
                           const tuple_pb &tuple) = 0;

  virtual result<ptr<tuple_pb>> get(table_id_t table_id, tuple_id_t tuple_id) = 0;

  virtual result<void> sync() = 0;

  virtual void close() = 0;

  virtual ~store() {}
};