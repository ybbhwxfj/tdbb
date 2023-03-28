#include "common/logger.hpp"
#include "portal/portal_client.h"
#include <boost/log/expressions.hpp>

int main(int ac, const char *av[]) {
  boost::log::core::get()->set_filter(boost::log::trivial::severity >=
      boost::log::trivial::info);
  return portal_client(ac, av);
}
