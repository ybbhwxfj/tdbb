#!/bin/bash

dir=$(dirname "$0")
proj_dir=$(cd "$dir" || exit; cd ..; pwd)
build_dir="$proj_dir"/build

mkdir -p "$build_dir"
cd "$build_dir" || exit

cmake .. -DCMAKE_CXX_FLAGS=--coverage -DCMAKE_C_FLAGS=--coverage
make -j || exit

PATH="$PATH:$proj_dir/bin"
test_config
test_enum2str
test_wait_graph
test_network
test_raft
test_db -- --dbtype db-s --bind false
test_db -- --dbtype db-s --bind true
test_db -- --dbtype db-sn
test_db -- --dbtype db-d
test_db -- --dbtype db-gro

