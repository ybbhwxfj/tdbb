# block-db

building blocks for transactional storage on cloud

# dependency

## on ubuntu

    apt-get install 
        zlib1g 
        zlib1g-dev 
        libsnappy-dev
        libzstd-dev
        libbz2-dev
        liblz4-dev

## install boost

download and install boost

    https://boostorg.jfrog.io/ui/native/main/release/1.77.0/source/

### build boost with clang support

```
./bootstrap.sh --with-toolset=clang cxxflags="-std=c++20 -stdlib=libc++" linkflags="-stdlib=libc++"
./b2 clean
./b2 toolset=clang cxxflags="-std=c++20 -stdlib=libc++" linkflags="-stdlib=libc++" -DBOOST_ASIO_NO_DEPRECATED

```

## install protobuf

## build third_party

build tkrzw:

    cd tkrzw-1.0.9
    ./configure
    make

build rocksdb

    export USE_RTTI=1
    make static_lib

### memcheck

nohup valgrind --leak-check=full --show-reachable=yes --leak-resolution=high ./test_db &

# configure

## configure max file descriptor

add these lines to /etc/security/limits.conf

```
<user>    hard    nofile  50000
<user>    soft    nofile  50000

```

# build docker

systemctl start docker.service

docker build -t Dockerfile --build-arg BUILD_TYPE=[dev/ndev] .

## create docker network

    docker network create -d bridge block-db-network



    kubectl run block-db --image=block-db --image-pull-policy=Never
