set(src_files
        access_mgr.cpp
        tx_context.cpp
        tx_coordinator.cpp
        cc_block.cpp
        lock_mgr.cpp
        data_mgr.cpp
        lock.cpp
        lock_slot.cpp
        write_ahead_log.cpp
        calvin_sequencer.cpp
        calvin_scheduler.cpp
        calvin_context.cpp
        calvin_collector.cpp
        deadlock.cpp ../../include/portal/block_client.h)

set(cc_lib concurrency)
add_library(${cc_lib} ${src_files})
add_dependencies(concurrency proto)

