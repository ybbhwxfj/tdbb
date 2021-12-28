# block-db

This repo is the source code of our research paper,
*Highly Available Transactional System Building Blocks*(To appear)
# dependency

## on ubuntu 20.04

    apt-get update && \
    apt-get -y dist-upgrade && \
    apt-get install -y \
        software-properties-common \
        wget \
        make \
        g++ \
        cmake \
        zlib1g \
        zlib1g-dev \
        libsnappy-dev \
        libzstd-dev \
        libbz2-dev \
        liblz4-dev \
        libgflags-dev \
        liburing-dev \
        git \
        vim \
        openssh-client \
        openssh-server \
        openssh-sftp-server \
        python3 \
        python3-pip \
        net-tools \
        iputils-ping \
        iproute2 \
        rsync \
        gdb


## install third party library, boost, rocksdb, tkrzw, protobuf
    ./script/build_third_party.sh

# configure

## configure max file descriptor

add these lines to /etc/security/limits.conf

```
<user>    hard    nofile  50000
<user>    soft    nofile  50000

```

## build

    ./script/build.sh