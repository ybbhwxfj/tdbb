#define BOOST_TEST_MODULE WAIT_GRAPH_TEST
#include "common/json_pretty.h"
#include "common/logger.hpp"
#include "common/wait_path.h"
#include "proto/proto.h"
#include <boost/filesystem.hpp>
#include <boost/test/unit_test.hpp>
#include <fstream>
#include <string>

BOOST_AUTO_TEST_CASE(wait_graph_test_read_json) {
  boost::filesystem::path p(__FILE__);
  p = p.parent_path().append("dep_set.json");
  std::ifstream fsm(p.string());
  std::stringstream ssm;
  ssm << fsm.rdbuf();
  dependency_set ds;
  bool json_to_pb_ok = json_to_pb(ssm.str(), ds);
  BOOST_CHECK(json_to_pb_ok);
  wait_path wp;
  wp.add_dependency_set(ds);
  std::stringstream pretty;
  json_pretty(ssm, pretty);
  LOG(info) << pretty.str();

  std::vector<std::vector<xid_t>> circles;
  wp.detect_circle(
      [&circles](const std::vector<xid_t> &c) { circles.push_back(c); });
  for (const auto &c : circles) {
    LOG(info) << "circle->";
    for (auto x : c) {
      LOG(info) << "    ->" << x;
    }
  }
  BOOST_CHECK(circles.size() == 1);
}

BOOST_AUTO_TEST_CASE(wait_graph_test_read_json_array) {
  boost::filesystem::path p(__FILE__);
  p = p.parent_path().append("dep_set_array.json");
  std::ifstream fsm(p.string());
  std::stringstream ssm;
  ssm << fsm.rdbuf();
  dependency_set_array dsa;
  bool json_to_pb_ok = json_to_pb(ssm.str(), dsa);
  BOOST_CHECK(json_to_pb_ok);
  wait_path wp;
  for (const auto &ds : dsa.array()) {
    wp.add_dependency_set(ds);
  }

  std::stringstream pretty;
  json_pretty(ssm, pretty);
  LOG(info) << pretty.str();

  std::vector<std::vector<xid_t>> circles;
  wp.detect_circle(
      [&circles](const std::vector<xid_t> &c) { circles.push_back(c); });
  for (const auto &c : circles) {
    LOG(info) << "circle->";
    for (auto x : c) {
      LOG(info) << "    ->" << x;
    }
  }
  BOOST_CHECK(circles.size() == 1);
}