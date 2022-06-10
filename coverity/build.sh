#!/bin/bash

pushd `dirname $0` > /dev/null
SCRIPTPATH=`pwd`
popd > /dev/null

mkdir -p $SCRIPTPATH/../build
pushd $SCRIPTPATH/../build
export PATH=/usr/local/gcc-stonedb/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/gcc-stonedb/lib64:$LD_LIBRARY_PATH

# `cc` is linked to /usr/bin/gcc but cmake checks CC for compiler
# version so the following does the trick
export CC=gcc

# rpm_create will exit on failure
g++ -std=c++1z -x c++ -E -dM - </dev/null >/dev/null

cmake -DENABLE_DTRACE=0 \
      -DMYSQL_TCP_PORT=3092 \
      -DWITH_EMBEDDED_SERVER=OFF \
      -DWITH_EMBEDDED_SHARED_LIBRARY=OFF \
      -DWITHOUT_ARCHIVE_STORAGE_ENGINE=1 \
      -DWITHOUT_BLACKHOLE_STORAGE_ENGINE=1 \
      -DWITHOUT_EXAMPLE_STORAGE_ENGINE=1 \
      -DWITHOUT_FEDERATED_STORAGE_ENGINE=1 \
      -DWITHOUT_PARTITION_STORAGE_ENGINE=1 \
      -DWITH_PERFSCHEMA_STORAGE_ENGINE=1 \
      -DWITHOUT_MYISAMMRG_STORAGE_ENGINE=1 \
      -DWITH_NDBCLUSTER_STORAGE_ENGINE=OFF \
      -DWITH_MYISAMMRG_STORAGE_ENGINE=0 \
      -DWITH_PARTITION_STORAGE_ENGINE=0 \
      -DINSTALL_MYSQLTESTDIR= \
      -DINSTALL_LAYOUT=RPM \
      -DDEFAULT_CHARSET=utf8 \
      -DDEFAULT_COLLATION=utf8_general_ci \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DBUILD_NUMBER="coverity" \
      -DRPM_RELEASE=$RPM_PACKAGE_RELEASE \
      -DWITH_SSL=/usr/local/openssl \
      -DENABLE_GPROF=OFF \
      -DENABLE_GCOV=OFF \
      ../src/mysql-5.6.24/

export NPROC=$(getconf _NPROCESSORS_ONLN)
make VERBOSE=1 -j`expr $NPROC + 1` 2>&1 | tee build.ouput
popd

