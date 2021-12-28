#define BOOST_TEST_MODULE ENUM2STRING_TEST
#include "common/enum_str.h"
#include <boost/test/unit_test.hpp>
#include <string>

enum enum_x {
  ENUM_VALUE1,
};

template<>
enum_strings<enum_x>::e2s_t enum_strings<enum_x>::enum2str;

template<>
enum_strings<enum_x>::e2s_t enum_strings<enum_x>::enum2str = {
    {ENUM_VALUE1, "ENUM STRING"},
};

BOOST_AUTO_TEST_CASE(conf_test) {
  std::stringstream ssm;
  ssm << enum2holder(ENUM_VALUE1);
  std::string s;
  s = ssm.str();
  auto h = string2holder<enum_x>(s);
  BOOST_CHECK_EQUAL(h.enum_value, ENUM_VALUE1);
}