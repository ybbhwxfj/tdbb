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
   echo "Syntax: build_third_party.sh [-i [PREFIX_PATH] -p [PACKAGE ] -h]"
   echo "options:"
   echo "i     install prefix directory"
   echo "p     install package,  all/boost/protobuf/rocksdb/tkrzw/tbb"
   echo "h     help description"
   echo
   exit 0
}


dir=$(dirname "$0")
proj_dir=$(cd "$dir" || exit; cd ..; pwd)
third_party_dir="$proj_dir"/third_party
install_prefix=${third_party_dir}  # the default install directory

install_package="all"

while getopts "i:p:h" opt
do
   case "$opt" in
      i )   install_prefix="$OPTARG" ;;
      p )   install_package="$OPTARG" ;;
      h )   usage ;;
      ? ) usage ;; # Print helpFunction in case parameter is non-existent
   esac
done

echo "install third party libraries to directory ${install_prefix}"

echo "..."



if [ "$install_package" = "all" ] || [ "$install_package" = "boost" ]; then
  echo "build boost ..."
  cd "${third_party_dir}" || exit
  if [ ! -f boost_${boost_version_ul}.tar.gz ]; then
    wget -c https://boostorg.jfrog.io/artifactory/main/release/${boost_version_d}/source/boost_${boost_version_ul}.tar.gz || exit
  fi
  tar -xvf boost_${boost_version_ul}.tar.gz
  cd boost_${boost_version_ul}
  ./bootstrap.sh --prefix="${install_prefix}"
  ./b2
  ./b2 install
  cd "${third_party_dir}" || exit
  rm -rf boost_${boost_version_ul}
fi

if [ "$install_package" = "all" ] || [ "$install_package" = "protobuf" ]; then
  echo "build all protobuf ..."
  cd "${third_party_dir}" || exit
  if [ ! -f protobuf-cpp-${protobuf_version}.tar.gz ]; then
    wget https://github.com/protocolbuffers/protobuf/releases/download/v${protobuf_version}/protobuf-cpp-${protobuf_version}.tar.gz || exit
  fi
  tar -xvf protobuf-cpp-${protobuf_version}.tar.gz
  cd protobuf-${protobuf_version}
  ./configure --prefix="${install_prefix}"
  make
  make install
  cd "${third_party_dir}" || exit
  rm -rf protobuf-${protobuf_version}
fi

if [ "$install_package" = "all" ] || [ "$install_package" = "rocksdb" ]; then
  echo "build rocksdb ..."
  cd "${third_party_dir}" || exit
  if [ ! -f v${rocksdb_version}.tar.gz ]; then
    wget -cO - https://codeload.github.com/facebook/rocksdb/tar.gz/refs/tags/v${rocksdb_version} > rocksdb-${rocksdb_version}.tar.gz || exit
  fi
  tar -xvf rocksdb-${rocksdb_version}.tar.gz
  cd rocksdb-${rocksdb_version} || exit
  mkdir -p build
  cd build
  cmake -DCMAKE_INSTALL_PREFIX="${install_prefix}" -DCMAKE_BUILD_TYPE=RelWithDebInfo ../
  PORTABLE=1 make static_lib
  make install
  cd "${third_party_dir}" || exit
  rm -rf rocksdb-${rocksdb_version}
fi

if [ "$install_package" = "all" ] || [ "$install_package" = "tkrzw" ]; then
  echo "build tkrzw ..."
  cd "${third_party_dir}" || exit
  if [ ! -f tkrzw-${tkrzw_version}.tar.gz ]; then
    wget -c https://dbmx.net/tkrzw/pkg/tkrzw-${tkrzw_version}.tar.gz || exit
  fi
  tar -xvf tkrzw-${tkrzw_version}.tar.gz
  cd tkrzw-${tkrzw_version}
  ./configure --enable-opt-native --enable-most-features --prefix="${install_prefix}"
  make
  make check
  make install
  cd "${third_party_dir}" || exit
  rm -rf tkrzw-${tkrzw_version}
fi

if [ "$install_package" = "all" ] || [ "$install_package" = "tbb" ]; then

  echo "install intel tbb ..."
  cd "${third_party_dir}" || exit
  
  git clone https://github.com/oneapi-src/oneTBB.git
  cd oneTBB
  # Create binary directory for out-of-source build
  mkdir build && cd build
  # Configure: customize CMAKE_INSTALL_PREFIX and disable TBB_TEST to avoid tests build
  cmake -DCMAKE_INSTALL_PREFIX="${install_prefix}" -DTBB_TEST=OFF ..
  # Build
  cmake --build .
  # Install
  cmake --install .

  cd "${third_party_dir}" || exit
fi


echo "build and install all third party libraries..."