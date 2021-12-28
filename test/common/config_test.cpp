#define BOOST_TEST_MODULE ENUM2STRING_TEST
#include <boost/test/unit_test.hpp>
#include <string>
#include "common/config.h"
#include "common/gen_config.h"

BOOST_AUTO_TEST_CASE(test_json) {
  config_option opt;
  opt.num_az = NUM_AZ;
  opt.num_shard = 3;
  opt.loose_bind = true;
  std::vector<std::string> conf_list = generate_config_json_file(opt);

  for (const auto &c: conf_list) {
    std::ifstream fsm(c);
    std::stringstream ssm;
    ssm << fsm.rdbuf();
    config conf;
    BOOST_CHECK(conf.from_json_string(ssm.str()));
  }
}
