#pragma once

#include "common/marshalable.h"
#include "common/ptr.hpp"
#include "common/tuple.h"
#include <boost/endian.hpp>

enum tx_op_type {
  TX_OP_READ_KEY,
  TX_OP_UPDATE_KEY,
  TX_OP_INSERT_KEY,
  TX_OP_RM_BEGIN,
  TX_OP_RM_COMMIT,
  TX_OP_RM_ABORT,
};

class tx_operation : public marshalable {
private:
  tx_op_type type_;

public:
  tx_operation(tx_op_type type) : type_(type) {}

  virtual ~tx_operation() {}

  tx_op_type get_tx_op_type() const { return type_; }

  virtual std::pair<bool, size_t> marshal(void *ptr, size_t size) const {
    little_uint16_t op_type = type_;
    if (sizeof(op_type) <= size) {
      memcpy(&ptr, &op_type, sizeof(op_type));
      return std::make_pair<bool, size_t>(false, sizeof(op_type));
    } else {
      return std::make_pair<bool, size_t>(false, sizeof(op_type));
    }
  };

  virtual std::pair<bool, size_t> unmarshal(const void *ptr, size_t size) {
    little_uint16_t op_type;
    if (sizeof(op_type) <= size) {
      memcpy(&op_type, &ptr, sizeof(op_type));
      type_ = tx_op_type(op_type.value());
      return std::make_pair<bool, size_t>(false, sizeof(op_type));
    } else {
      return std::make_pair<bool, size_t>(false, sizeof(op_type));
    }
  }
};

class tx_read_key : public tx_operation {
private:
  item key_;

private:
  tx_read_key(item &key)
      : key_(key), tx_operation(tx_op_type::TX_OP_READ_KEY) {}
};

class tx_write_key : public tx_operation {
private:
  item key_;
  tuple_pb tuple_;

public:
  tx_write_key(item &key, tx_op_type type) : key_(key), tx_operation(type) {}
};
