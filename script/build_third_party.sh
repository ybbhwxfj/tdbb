#!/bin/bash

boost_version_ul="1_79_0" # underline
boost_version_d="1.79.0"  # dot
protobuf_version="3.19.1"
rocksdb_version="7.4.3"
tkrzw_version="1.0.20"

usage()
{
   # Display Help
   echo "Help."
   echo
   echo "Syntax: build_third_party.sh [-i [PREFIX_PATH] -h]"
   echo "options:"
   echo "i     install prefix directory"
   echo "h     help description"
   echo
   exit 0
}


dir=$(dirname "$0")
proj_dir=$(cd "$dir" || exit; cd ..; pwd)
third_party_dir="$proj_dir"/third_party
install_dir=${third_party_dir}  # the default install directory

while getopts "i:h" opt
do
   case "$opt" in
      i )   install_dir="$OPTARG" ;;
      h )   usage ;;
      ? ) usage ;; # Print helpFunction in case parameter is non-existent
   esac
done

echo "install third party libraries to directory ${install_dir}"

echo "..."

echo "build boost ..."
cd "${third_party_dir}" || exit
if [ ! -f boost_${boost_version_ul}.tar.gz ]; then
  wget -c https://boostorg.jfrog.io/artifactory/main/release/${boost_version_d}/source/boost_${boost_version_ul}.tar.gz || exit
fi
tar -xvf boost_${boost_version_ul}.tar.gz
cd boost_${boost_version_ul}
./bootstrap.sh --prefix="${install_dir}"
./b2
./b2 install
cd "${third_party_dir}" || exit
rm -rf boost_${boost_version_ul}

echo "build all protobuf ..."
cd "${third_party_dir}" || exit
if [ ! -f protobuf-cpp-${protobuf_version}.tar.gz ]; then
  wget https://github.com/protocolbuffers/protobuf/releases/download/v${protobuf_version}/protobuf-cpp-${protobuf_version}.tar.gz || exit
fi
tar -xvf protobuf-cpp-${protobuf_version}.tar.gz
cd protobuf-${protobuf_version}
./configure --prefix="${install_dir}"
make
make install
cd "${third_party_dir}" || exit
rm -rf protobuf-${protobuf_version}


echo "build rocksdb ..."
cd "${third_party_dir}" || exit
if [ ! -f v${rocksdb_version}.tar.gz ]; then
  wget -cO - https://codeload.github.com/facebook/rocksdb/tar.gz/refs/tags/v${rocksdb_version} > rocksdb-${rocksdb_version}.tar.gz || exit
fi
tar -xvf rocksdb-${rocksdb_version}.tar.gz
cd rocksdb-${rocksdb_version} || exit
mkdir -p build
cd build
cmake -DCMAKE_INSTALL_PREFIX="${install_dir}" -DCMAKE_BUILD_TYPE=RelWithDebInfo ../
make
make install
cd "${third_party_dir}" || exit
rm -rf rocksdb-${rocksdb_version}

echo "build tkrzw ..."
cd "${third_party_dir}" || exit
if [ ! -f tkrzw-${tkrzw_version}.tar.gz ]; then
  wget -c https://dbmx.net/tkrzw/pkg/tkrzw-${tkrzw_version}.tar.gz || exit
fi
tar -xvf tkrzw-${tkrzw_version}.tar.gz
cd tkrzw-${tkrzw_version}
./configure --enable-opt-native --enable-most-features --prefix="${install_dir}"
make
make check
make install
cd "${third_party_dir}" || exit
rm -rf tkrzw-${tkrzw_version}

echo "install intel tbb ..."
cd "${third_party_dir}" || exit
wget https://registrationcenter-download.intel.com/akdlm/irc_nas/18728/l_tbb_oneapi_p_2021.6.0.835_offline.sh
echo "${install_dir}"
bash l_tbb_oneapi_p_2021.6.0.835_offline.sh -a -s --eula accept --install-dir "${install_dir}"
rm l_tbb_oneapi_p_2021.6.0.835_offline.sh
mkdir -p "${install_dir}/include"
mkdir -p "${install_dir}/lib"
ln -s "${install_dir}/tbb/latest/include/tbb" "${install_dir}/include/tbb"
ln -s "${install_dir}/tbb/latest/include/oneapi" "${install_dir}/include/oneapi"
find "${install_dir}" -name "libtbb.so*" | grep "intel" | xargs -i cp {} "${install_dir}/lib/"
cd "${third_party_dir}" || exit

echo "build and install all third party libraries..."