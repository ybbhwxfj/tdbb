#pragma once

#include "common/endian.h"
#include <string>

class hello {
private:
  uint32_buf_t id_;

public:
  hello() {}

  hello(int id) : id_(id) {}

  int id() const { return id_.value(); }

  result<void> save(byte_buffer &buffer) const {
    return buffer.write((const uint8_t *) this, sizeof(this));
  }

  result<void> load(byte_buffer &buffer) {
    return buffer.read((uint8_t *) this, sizeof(this));
  }
};
