add_library(replog
        rl_block.cpp
        log_service_impl.cpp
        )
add_dependencies(replog proto)