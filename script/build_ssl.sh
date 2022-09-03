wget https://www.openssl.org/source/openssl-1.1.1m.tar.gz
tar -xf openssl-1.1.1m.tar.gz
cd openssl-1.1.1m

./config shared zlib
make
make install