# block-db

This repo is the source code of our research paper *Building Blocks for Cloud Transactional DBMS*(To appear).
This paper aims to simplify cloud transactional DBMS development.

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
        openssh-client \
        openssh-server \
        openssh-sftp-server \
        python3 \
        python3-pip \
        net-tools \
        iputils-ping \
        iproute2 \
        rsync

## install third party library, boost, rocksdb, tkrzw, protobuf

    ./script/build_third_party.sh -i [INSTALL_PREFIX_PATH]

## build

    ./script/build.sh

# configure

## configure max file descriptor

Increase maximum opened file numbers. Add these lines to /etc/security/limits.conf

```
<user>    hard    nofile  <integer value>
<user>    soft    nofile  <integer value>

```