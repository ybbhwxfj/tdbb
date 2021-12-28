#include "raft_test_restart.h"
#include "raft_test.h"

void raft_test_restart() {
  ptr<raft_test_context> c = raft_test_startup("restart");
  c->wait_tx_commit(10002);
  c->stop_and_join();
}