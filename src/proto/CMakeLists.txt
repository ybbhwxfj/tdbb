include(FindProtobuf)
find_package(Protobuf REQUIRED)

file(GLOB proto_files "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
protobuf_generate_cpp(proto_sources proto_headers ${proto_files})

add_library(proto ${proto_sources} ${proto_headers})
