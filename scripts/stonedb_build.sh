#!/bin/sh

set -e

# step 1. create dir: build
if [ ! -d ../build ]
then
    mkdir ../build
fi

build_type=Debug
#build_type=Release
branch=`git rev-parse --abbrev-ref HEAD`
cpu_arc=`uname -m`
os_dist=`lsb_release -a | grep Distributor | tail -n 1 | awk '{print $3}'`
os_dist_release=`lsb_release -a | grep Release | tail -n 1 | awk '{print $2}'`
build_time=`date "+%Y-%m-%d_%H-%M-%S"`
build_tag=build_${branch}_${cpu_arc}_${os_dist}${os_dist_release}_${build_type}_${build_time}
build_log=`pwd`/../build/build_log_${build_tag}.log

install_target=`pwd`/../build/install

# step 2. shutdown server first
if test -f ${install_target}/support-files/mysql.server
then
        ${install_target}/support-files/mysql.server stop 2>&1 | tee -a ${build_log}
fi
# then remove the target installation
rm -fr ${install_target}

# step 3. create build directory
rm -fr ../build/${build_tag}
mkdir ../build/${build_tag}
cd ../build/${build_tag}
mkdir ${install_target}

# step 4. begin to build
cmake ../../ \
-DCMAKE_BUILD_TYPE=${build_type} \
-DCMAKE_INSTALL_PREFIX=${install_target} \
-DMYSQL_DATADIR=${install_target}/data \
-DSYSCONFDIR=${install_target} \
-DMYSQL_UNIX_ADDR=${install_target}/tmp/mysql.sock \
-DWITH_EMBEDDED_SERVER=OFF \
-DWITH_TIANMU_STORAGE_ENGINE=1 \
-DWITH_MYISAM_STORAGE_ENGINE=1 \
-DWITH_INNOBASE_STORAGE_ENGINE=1 \
-DWITH_PARTITION_STORAGE_ENGINE=1 \
-DMYSQL_TCP_PORT=3306 \
-DENABLED_LOCAL_INFILE=1 \
-DEXTRA_CHARSETS=all \
-DDEFAULT_CHARSET=utf8mb4 \
-DDEFAULT_COLLATION=utf8mb4_general_ci \
-DDOWNLOAD_BOOST=0 \
-DWITH_BOOST=/usr/local/stonedb-boost \
-DWITH_MARISA=/usr/local/stonedb-marisa \
-DWITH_ROCKSDB=/usr/local/stonedb-gcc-rocksdb \
2>&1 | tee -a ${build_log}

# step 5. make & make install
make VERBOSE=1 -j`nproc`                                             2>&1 | tee -a ${build_log}
make install                                                         2>&1 | tee -a ${build_log}
echo "current dir is `pwd`"                                                              2>&1 | tee -a ${build_log}
