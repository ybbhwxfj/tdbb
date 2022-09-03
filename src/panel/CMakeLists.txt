set(src_files
        pn_block.cpp
        )

set(panel_lib panel)
add_library(${panel_lib} ${src_files})
add_dependencies(${panel_lib} proto)