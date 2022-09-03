#pragma once

#include "proto/proto.h"

inline bool is_write_operation(tx_op_type op) {
  return op == TX_OP_UPDATE || op == TX_OP_INSERT || op == TX_OP_DELETE;
}

