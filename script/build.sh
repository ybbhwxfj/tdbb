#!/bin/bash
dir=$(dirname "$0")
proj_dir=$(cd "$dir" || exit; cd ..; pwd)
mkdir -p "${proj_dir}/build"
cd "${proj_dir}/build" || exit;
cmake -DDISABLE_TEST=true \
	-DCMAKE_BUILD_TYPE=RelWithDebInfo \
	-DProtobuf_INCLUDE_DIR=../third_party/include \
	-DProtobuf_LIBRARIES=../third_party/lib \
	..
make -j
