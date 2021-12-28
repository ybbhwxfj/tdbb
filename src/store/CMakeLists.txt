
set(src_files
        ds_block.cpp
        tkrzw_store.cpp
        rocks_store.cpp
        )

set(store_lib store)
add_library(${store_lib} ${src_files})


