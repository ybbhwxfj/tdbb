#pragma once

#include "proto/proto.h"

enum lock_mode : unsigned int {
  LOCK_INVALID,
  LOCK_READ_PREDICATE,
  LOCK_READ_ROW,
  LOCK_WRITE_ROW,
};

inline lock_mode op_type_to_lock_mode(tx_op_type op) {
  switch (op) {
    case tx_op_type::TX_OP_UPDATE:
    case tx_op_type::TX_OP_DELETE:
    case tx_op_type::TX_OP_INSERT:
    case tx_op_type::TX_OP_READ_FOR_WRITE:return lock_mode::LOCK_WRITE_ROW;
    case tx_op_type::TX_OP_READ:return lock_mode::LOCK_READ_ROW;
    default:BOOST_ASSERT(false);
      return lock_mode::LOCK_INVALID;
  }
}