#!/bin/bash
dir=$(dirname "$0")
proj_dir=$(cd "$dir" || exit; cd ..; pwd)
tp_dir="$proj_dir"/third_party
echo "build boost ..."
cd "${tp_dir}" || exit
ls
if [ ! -f mysql-boost-8.0.26.tar.gz ]; then
  if [ ! -d mysql-8.0.26 ]; then
    echo "Get mysql source code from Internet"
    wget https://downloads.mysql.com/archives/get/p/23/file/mysql-boost-8.0.26.tar.gz
  fi
fi

if [ ! -d mysql-8.0.26 ]; then
  echo "tar mysql source code"
  tar -xvf mysql-boost-8.0.26.tar.gz
fi
cd mysql-8.0.26
#patch -s -p1 < ../../mysql/:mysql-8.0.26.patch
rm -rf build
mkdir -p build
cd build
cmake -DCMAKE_INSTALL_PREFIX=/usr/local -DWITH_BOOST="../boost" ../
make -j
make install
