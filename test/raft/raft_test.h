#pragma once
#include "raft_test_context.h"

ptr<raft_test_context> raft_test_startup(const std::string &test_case_name,
                                         uint64_t max_log_entries);
