#!/bin/bash
dir=$(dirname "$0")
proj_dir=$(cd "$dir" || exit; cd ..; pwd)
tp_dir="$proj_dir"/third_party


echo "build boost ..."
cd "${tp_dir}" || exit
if [ ! -f boost_1_77_0.tar.gz ]; then
  wget https://boostorg.jfrog.io/artifactory/main/release/1.77.0/source/boost_1_77_0.tar.gz
fi
tar -xvf boost_1_77_0.tar.gz
cd boost_1_77_0
./bootstrap.sh --prefix="${tp_dir}"
./b2
./b2 install
cd "${tp_dir}" || exit
rm -rf boost_1_77_0

echo "build rocksdb ..."
cd "${tp_dir}" || exit
if [ ! -f v6.26.1.tar.gz ]; then
  wget https://github.com/facebook/rocksdb/archive/refs/tags/v6.26.1.tar.gz
fi
tar -xvf v6.26.1.tar.gz
cd rocksdb-6.26.1
mkdir -p build
cd build
cmake -DCMAKE_INSTALL_PREFIX="${tp_dir}" -DCMAKE_BUILD_TYPE=RelWithDebInfo ../
make
make install
cd "${tp_dir}" || exit
rm -rf rocksdb-6.26.1

echo "build tkrzw ..."
cd "${tp_dir}" || exit
if [ ! -f tkrzw-1.0.20.tar.gz ]; then
  wget wget https://dbmx.net/tkrzw/pkg/tkrzw-1.0.20.tar.gz
fi
tar -xvf tkrzw-1.0.20.tar.gz
cd tkrzw-1.0.20
./configure --enable-opt-native --enable-most-features --prefix="${tp_dir}"
make
make check
make install
cd "${tp_dir}" || exit
rm -rf tkrzw-1.0.20

echo "build all protobuf ..."
cd "${tp_dir}" || exit
if [ ! -f protobuf-cpp-3.19.1.tar.gz ]; then
  wget https://github.com/protocolbuffers/protobuf/releases/download/v3.19.1/protobuf-cpp-3.19.1.tar.gz
fi
tar -xvf protobuf-cpp-3.19.1.tar.gz
cd protobuf-3.19.1
./configure --prefix="${tp_dir}"
make
make install
cd "${tp_dir}" || exit
rm -rf protobuf-3.19.1

echo "build all third party libraries..."