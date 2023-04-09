
## Install dependency on Ubuntu 22.04

    ./script/install.sh

This would install prerequisite, third party library including boost, rocksdb, tkrzw, protobuf, oneTBB

## Configure limit variable of linux

Increase maximum opened file numbers. Add these lines to /etc/security/limits.conf

```
<user>    hard    nofile  <integer value>
<user>    soft    nofile  <integer value>

```

## Build the source code

    ./script/build.sh