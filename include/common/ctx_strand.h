
#pragma once

#include <boost/asio.hpp>

class ctx_strand {
 private:
  boost::asio::io_context::strand strand_;
 public:
  explicit ctx_strand(boost::asio::io_context &ctx) : strand_(ctx) {}
  explicit ctx_strand(boost::asio::io_context::strand s) : strand_(s) {}

  const boost::asio::io_context::strand &get_strand() const {
    return strand_;
  }
};