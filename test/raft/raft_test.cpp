#define BOOST_TEST_MODULE RAFT_TEST
#include "raft_test_general.h"
#include "raft_test_restart.h"
#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(raft_test_case_restart) { raft_test_restart(); }

BOOST_AUTO_TEST_CASE(raft_test) { raft_test_general(); }
