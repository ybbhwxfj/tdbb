#pragma once

#include "common/error_code.h"
#include <exception>

class block_exception : public std::exception {
private:
  EC ec_;

public:
  explicit block_exception(EC ec) : ec_(ec) {}

  ~block_exception() override = default;

  EC error_code() const { return ec_; }
};
