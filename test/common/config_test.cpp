#define BOOST_TEST_MODULE ENUM2STRING_TEST
#include "common/config.h"
#include "common/gen_config.h"
#include <boost/test/unit_test.hpp>
#include <string>

BOOST_AUTO_TEST_CASE(test_json) {
  config_option opt;
  opt.num_az = NUM_AZ;
  opt.set_config_share_nothing(5);

  std::pair<std::vector<std::string>, std::vector<std::string>> conf_list = generate_config_json_file(opt);

  for (const auto &c : conf_list.first) {
    std::ifstream fsm(c);
    std::stringstream ssm;
    ssm << fsm.rdbuf();
    config conf;
    BOOST_CHECK(conf.from_json_string(ssm.str()));
  }
  for (const auto &c : conf_list.second) {
    std::ifstream fsm(c);
    std::stringstream ssm;
    ssm << fsm.rdbuf();
    config conf;
    BOOST_CHECK(conf.from_json_string(ssm.str()));
  }
}
