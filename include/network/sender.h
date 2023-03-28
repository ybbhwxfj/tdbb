#pragma once

#include "common/byte_buffer.h"
#include "common/result.hpp"
#include "network/connection.h"

class client;

class sender {
public:
  virtual result<ptr<client>> get_connection(uint32_t id) = 0;

  virtual ~sender() = default;
};
