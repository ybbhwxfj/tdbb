#!/bin/bash
set -e
script_dir=$(cd `dirname $0`; pwd)
cd $script_dir
cd ../
project_path=$(pwd)
cd $project_path/bin

PATH=$project_path/bin:${PATH}



buffer_test --abort-on-failure
serialization_test --abort-on-failure
timer_test --abort-on-failure
strfmt_test --abort-on-failure
stat_mgr_test --abort-on-failure
raft_server_test --abort-on-failure
failure_test --abort-on-failure
asio_service_test --abort-on-failure
