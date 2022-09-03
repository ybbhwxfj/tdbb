
add_library(
        common
        global.cpp
        db_type.cpp
        config.cpp
        column_desc.cpp
        table_desc.cpp
        schema_mgr.cpp
        block_config.cpp
        message.cpp
        error_code.cpp
        wait_path.cpp
        test_config.cpp
)

add_dependencies(common proto)

