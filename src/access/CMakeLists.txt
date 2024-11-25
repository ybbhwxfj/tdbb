set(src_files
        access_mgr.cpp
        data_mgr.cpp
)

set(access_lib access)
add_library(${access_lib} ${src_files})
add_dependencies(access proto)

