#include "raft_test_restart.h"
#include "raft_test.h"

void raft_test_restart() {
  ptr<raft_test_context> c = raft_test_startup("restart", 10);
  c->wait_finish();
  c->stop_and_join();
}