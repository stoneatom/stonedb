#!/bin/bash

mkdir -p ../build
pushd ../build
export STONEDB_BUILD_NUMBER=$(date +%s)

# currently support 4.9.2; other versions are not verified!
GCC_VERSION=4.9.2
export PATH=/usr/local/gcc-${GCC_VERSION}/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/gcc-${GCC_VERSION}/lib64:$LD_LIBRARY_PATH

# `cc` is linked to /usr/bin/gcc but cmake checks CC for compiler
# version so the following does the trick
export CC=gcc

# rpm_create will exit on failure
g++ -std=c++11 -x c++ -E -dM - </dev/null >/dev/null

cmake -DWITH_EMBEDDED_SERVER=0 \
      -DWITH_EMBEDDED_SHARED_LIBRARY=0 \
      -DWITHOUT_PERFSCHEMA_STORAGE_ENGINE=1 \
      -DINSTALL_MYSQLTESTDIR=mysql-test \
      -DCMAKE_INSTALL_PREFIX=%{install_path} \
      -DMYSQL_DATADIR=%{install_path}/data \
      -DDEFAULT_CHARSET=utf8 -DDEFAULT_COLLATION=utf8_general_ci \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DBUILD_NUMBER="$STONEDB_BUILD_NUMBER" \
      -DINSTALL_LAYOUT=STANDALONE \
      ../src/mysql-5.6.24/

export NPROC=$(getconf _NPROCESSORS_ONLN)
make VERBOSE=1 -j`expr $NPROC + 1` 2>&1 | tee build.ouput
make package

popd

