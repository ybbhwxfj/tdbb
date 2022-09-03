add_library(raft
        state_machine.cpp
        )
add_dependencies(raft proto)