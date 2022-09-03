#pragma once

#include "common/error_code.h"
#include <system_error>
#include <boost/assert.hpp>
#include <boost/outcome.hpp>

inline std::string ec2string(EC ec) {
  auto i = enum_strings<EC>::enum2str.find(ec);
  if (i == enum_strings<EC>::enum2str.end()) {
    return "";
  }
  return i->second;
}

namespace std {
// Tell the C++ 11 STL metaprogramming that enum ec
// is registered with the standard error code system
template<>
struct is_error_code_enum<EC> : true_type {
};

namespace detail {
// Define a custom error code category derived from std::error_category
class ec_category : public std::error_category {
 public:
  // Return a short descriptive name for the category
  virtual const char *name() const noexcept override final {
    return "ConversionError";
  }

  // Return what each enum means in text
  virtual std::string message(int c) const override final {
    return ec2string(EC(c));
  }

  // OPTIONAL: Allow generic error conditions to be compared to me
  virtual std::error_condition
  default_error_condition(int c) const noexcept override final {
    switch (static_cast<EC>(c)) {
      case EC::EC_NETWORK_ERROR:return make_error_condition(std::errc::invalid_argument);
      default:
        // I have no mapping for this code
        return std::error_condition(c, *this);
    }
  }
};
} // namespace detail


} // namespace std




// Define the linkage for this function to be used by external code.
// This would be the usual __declspec(dllexport) or __declspec(dllimport)
// if we were in a Windows DLL etc. But for this example use a global
// instance but with inline linkage so multiple definitions do not collide.
#define THIS_MODULE_API_DECL extern inline

// Declare a global function returning a static instance of the custom category
THIS_MODULE_API_DECL const std::detail::ec_category &ec_category() {
  static std::detail::ec_category c;
  return c;
}

// Overload the global make_error_code() free function with our
// custom enum. It will be found via ADL by the compiler if needed.
inline std::error_code make_error_code(EC e) {
  return {static_cast<int>(e), ec_category()};
}

class berror : public std::error_code {
 public:
  explicit berror() : std::error_code() {}

  explicit berror(EC ec)
      : std::error_code(ec, ec_category()) {

  };

  explicit berror(boost::system::error_code bec) :
      std::error_code(bec.value(), bec.category()) {

  }

  explicit berror(std::error_code sec)
      : std::error_code(sec) {

  };

  bool operator==(const berror &err) { return this->value() == err.value(); }

  bool operator==(EC ec) const { return this->code() == ec; }

  EC code() const { return EC(value()); }

  std::string message() const {
    std::string msg = ec2string(EC(value()));
    if (msg.empty()) {
      return error_code::message();
    } else {
      return msg;
    }
  }

  bool failed() const { return value() != 0; }
};
