[CCB -> DSB(access)] 2
ccb_read_request

    calvin_context.cpp
    tx_context.cpp

[CCB -> RLB] 4

tx_log_binary

    calvin_scheduler.cpp
    tx_context.cpp
    tx_coordinator.cpp

rlb_commit_entries

    cc_block.cpp

[RLB -> DSB(access)] 1

replay_to_dsb_request rlb

    rl_block.cpp