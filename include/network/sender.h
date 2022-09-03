#pragma once

#include "common/buffer.h"
#include "common/result.hpp"
#include "network/connection.h"

class client;

class sender {
 public:
  virtual void async_send(uint32_t id, const byte_buffer &buffer) = 0;

  virtual result<ptr<client>> get_connection(uint32_t id) = 0;

  virtual ~sender() = default;
};
