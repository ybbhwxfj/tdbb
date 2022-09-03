#pragma once

#include "common/message.h"
#include "common/buffer.h"
#include "common/ptr.hpp"
#include <memory>
#include <iostream>

class block {
 public:
  virtual void handle_debug(const std::string &path, std::ostream &os) = 0;

  virtual void on_start() = 0;

  virtual void on_stop() = 0;

  virtual ~block() = default;
};
