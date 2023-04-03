---
id: compile-using-ubuntu22.04-for-80
sidebar_position: 5.151
---

# Compile StoneDB for MySQL8.0 on Ubuntu 22.04

This topic describes how to compile StoneDB for MySQL8.0 on Ubuntu 22.04.

## Precautions

Ensure that the tools and third-party libraries used in your environment meet the following version requirements:
* GCC 9.4.0
* Make 3.82 or later
* CMake 3.7.2 or later
* marisa 0.77
* RocksDB 6.12.6
* Boost 1.77

## Procedure

### Step 1. Install the dependencies

```bash
sudo apt install -y gcc
sudo apt install -y g++
sudo apt install -y make
sudo apt install -y cmake
sudo apt install -y build-essential
sudo apt install -y autoconf
sudo apt install -y tree
sudo apt install -y bison
sudo apt install -y git
sudo apt install -y libtool
sudo apt install -y numactl
sudo apt install -y python3-dev
sudo apt install -y openssl
sudo apt install -y perl
sudo apt install -y binutils
sudo apt install -y libgmp-dev
sudo apt install -y libmpfr-dev
sudo apt install -y libmpc-dev
sudo apt install -y libisl-dev
sudo apt install -y zlib1g-dev
sudo apt install -y liblz4-dev
sudo apt install -y libbz2-dev
sudo apt install -y libzstd-dev
sudo apt install -y zstd
sudo apt install -y lz4
sudo apt install -y ncurses-dev
sudo apt install -y libsnappy-dev
sudo apt install -y libedit-dev
sudo apt install -y libaio-dev
sudo apt install -y libncurses5-dev 
sudo apt install -y libreadline-dev
sudo apt install -y libpam0g-dev
sudo apt install -y libicu-dev
sudo apt install -y libboost-dev
sudo apt install -y libgflags-dev
sudo apt install -y libjemalloc-dev
sudo apt install -y libssl-dev
sudo apt install -y pkg-config
```

> libssl-dev may cannot install use apt, you can install it use aptitude.
> ```bash
> sudo apt install aptitude
> sudo aptitude install libssl-dev
> ...
> # type n y y
> ```  

### Step 2. Install third-party dependencies

If your ubuntu version >= 20.04, skip to install marisa.
1.If cmake version < 3.72, install CMake
Check your CMake version first.

```bash
wget https://cmake.org/files/v3.7/cmake-3.7.2.tar.gz
tar -zxvf cmake-3.7.2.tar.gz
cd cmake-3.7.2
./bootstrap && make && make install
/usr/local/bin/cmake --version
apt remove cmake -y
ln -s /usr/local/bin/cmake /usr/bin/
cmake --version
```

> If your gcc version too high, it may cause the compilation to fail. You can add #include <limits> in the beginning of cmake-3.72/Source/cmServerProtocal.cxx to solve it.
> ```c++
> #include <algorithm>
> #include <string>
> #include <vector>
> #include <limits>
> ```

2.If make version < 3.82, install Make
Check your make version first.

```bash
wget http://mirrors.ustc.edu.cn/gnu/make/make-3.82.tar.gz
tar -zxvf make-3.82.tar.gz
cd make-3.82
./configure  --prefix=/usr/local/make
make && make install
rm -rf /usr/local/bin/make
ln -s /usr/local/make/bin/make /usr/local/bin/make
make --version
```

3.Install marisa

```bash
git clone https://github.com/s-yata/marisa-trie.git
cd marisa-trie
autoreconf -i
./configure --enable-native-code --prefix=/usr/local/stonedb-marisa
sudo make && make install 
```

The installation directory of marisa in the example is /usr/local/stonedb-marisa. You can change it based on your actual conditions. In this step, the following directories and files are generated in /usr/local/stonedb-marisa/lib.

```bash
root@htap-dev-64-2:/usr/local/stonedb-marisa/lib$ ls -l
total 4136
-rw-r--r-- 1 root root 2947618 Mar 20 16:25 libmarisa.a
-rwxr-xr-x 1 root root     967 Mar 20 16:25 libmarisa.la
lrwxrwxrwx 1 root root      18 Mar 20 16:25 libmarisa.so -> libmarisa.so.0.0.0
lrwxrwxrwx 1 root root      18 Mar 20 16:25 libmarisa.so.0 -> libmarisa.so.0.0.0
-rwxr-xr-x 1 root root 1273936 Mar 20 16:25 libmarisa.so.0.0.0
drwxrwxr-x 2 root root    4096 Mar 20 16:25 pkgconfig
```

4.Install RocksDB

```bash
wget https://github.com/facebook/rocksdb/archive/refs/tags/v6.12.6.tar.gz 
tar -zxvf v6.12.6.tar.gz
cd rocksdb-6.12.6

sudo cmake ./ \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX=/usr/local/stonedb-gcc-rocksdb \
  -DCMAKE_INSTALL_LIBDIR=/usr/local/stonedb-gcc-rocksdb \
  -DWITH_JEMALLOC=ON \
  -DWITH_SNAPPY=ON \
  -DWITH_LZ4=ON \
  -DWITH_ZLIB=ON \
  -DWITH_ZSTD=ON \
  -DUSE_RTTI=ON \
  -DROCKSDB_BUILD_SHARED=ON \
  -DWITH_GFLAGS=OFF \
  -DWITH_TOOLS=OFF \
  -DWITH_BENCHMARK_TOOLS=OFF \
  -DWITH_CORE_TOOLS=OFF 

sudo make -j`nproc`
sudo make install -j`nproc`
```

> Your gcc version may too high, modify your CMakeLists.txt row#310-317 like this.
> ```shell
> if(FAIL_ON_WARNINGS)
>   if(MSVC)
>     set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
>   else() # assume GCC
> 	  # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
>   endif()
> endif()
> ```

The installation directory of RocksDB in the example is /usr/local/stonedb-gcc-rocksdb. You can change it based on your actual conditions. In this step, the following directories and files are generated in /usr/local/stonedb-gcc-rocksdb

```bash
root@htap-dev-64-2:/usr/local/stonedb-gcc-rocksdb$ ls -l
total 29352
drwxr-xr-x 3 root root     4096 Mar 20 17:12 cmake
drwxr-xr-x 3 root root     4096 Mar 20 17:12 include
-rw-r--r-- 1 root root 20555728 Mar 20 17:11 librocksdb.a
lrwxrwxrwx 1 root root       15 Mar 20 17:12 librocksdb.so -> librocksdb.so.6
lrwxrwxrwx 1 root root       20 Mar 20 17:12 librocksdb.so.6 -> librocksdb.so.6.12.6
-rw-r--r-- 1 root root  9490272 Mar 20 17:12 librocksdb.so.6.12.6
```

5.Install Boost

```bash
wget https://sourceforge.net/projects/boost/files/boost/1.77.0/boost_1_77_0.tar.gz
tar -zxvf boost_1_77_0.tar.gz
cd boost_1_77_0
./bootstrap.sh --prefix=/usr/local/stonedb-boost177
./b2 install --with=all
```

The installation directory of Boost in the example is /usr/local/stonedb-boost. You can change it based on your actual conditions. In this step, the following directories and files are generated in /usr/local/stonedb-boost/lib.

```bash
root@htap-dev-64-2:/usr/local/stonedb-boost177/lib$ ls -l
total 41612
drwxrwxr-x 49 root root    4096 Mar 20 18:57 cmake
-rw-rw-r--  1 root root   14648 Mar 20 18:56 libboost_atomic.a
lrwxrwxrwx  1 root root      25 Mar 20 18:57 libboost_atomic.so -> libboost_atomic.so.1.77.0
-rwxrwxr-x  1 root root   22248 Mar 20 18:57 libboost_atomic.so.1.77.0
-rw-rw-r--  1 root root  181638 Mar 20 18:56 libboost_chrono.a
lrwxrwxrwx  1 root root      25 Mar 20 18:57 libboost_chrono.so -> libboost_chrono.so.1.77.0
-rwxrwxr-x  1 root root   55000 Mar 20 18:57 libboost_chrono.so.1.77.0
-rw-rw-r--  1 root root  166894 Mar 20 18:56 libboost_container.a
lrwxrwxrwx  1 root root      28 Mar 20 18:57 libboost_container.so -> libboost_container.so.1.77.0
-rwxrwxr-x  1 root root  111912 Mar 20 18:57 libboost_container.so.1.77.0
-rw-rw-r--  1 root root    6874 Mar 20 18:56 libboost_context.a
lrwxrwxrwx  1 root root      26 Mar 20 18:57 libboost_context.so -> libboost_context.so.1.77.0
-rwxrwxr-x  1 root root   16584 Mar 20 18:57 libboost_context.so.1.77.0
-rw-rw-r--  1 root root  275376 Mar 20 18:56 libboost_contract.a
lrwxrwxrwx  1 root root      27 Mar 20 18:57 libboost_contract.so -> libboost_contract.so.1.77.0
-rwxrwxr-x  1 root root  148672 Mar 20 18:57 libboost_contract.so.1.77.0
-rw-rw-r--  1 root root  178370 Mar 20 18:56 libboost_coroutine.a
lrwxrwxrwx  1 root root      28 Mar 20 18:57 libboost_coroutine.so -> libboost_coroutine.so.1.77.0
-rwxrwxr-x  1 root root   84440 Mar 20 18:57 libboost_coroutine.so.1.77.0
-rw-rw-r--  1 root root    1458 Mar 20 18:56 libboost_date_time.a
lrwxrwxrwx  1 root root      28 Mar 20 18:57 libboost_date_time.so -> libboost_date_time.so.1.77.0
-rwxrwxr-x  1 root root   15184 Mar 20 18:57 libboost_date_time.so.1.77.0
-rw-rw-r--  1 root root    1654 Mar 20 18:57 libboost_exception.a
-rw-rw-r--  1 root root  249818 Mar 20 18:56 libboost_fiber.a
lrwxrwxrwx  1 root root      24 Mar 20 18:56 libboost_fiber.so -> libboost_fiber.so.1.77.0
-rwxrwxr-x  1 root root  107648 Mar 20 18:56 libboost_fiber.so.1.77.0
-rw-rw-r--  1 root root  520738 Mar 20 18:56 libboost_filesystem.a
lrwxrwxrwx  1 root root      29 Mar 20 18:56 libboost_filesystem.so -> libboost_filesystem.so.1.77.0
-rwxrwxr-x  1 root root  178288 Mar 20 18:56 libboost_filesystem.so.1.77.0
-rw-rw-r--  1 root root 1015048 Mar 20 18:57 libboost_graph.a
lrwxrwxrwx  1 root root      24 Mar 20 18:56 libboost_graph.so -> libboost_graph.so.1.77.0
-rwxrwxr-x  1 root root  525440 Mar 20 18:56 libboost_graph.so.1.77.0
-rw-rw-r--  1 root root  250402 Mar 20 18:56 libboost_iostreams.a
lrwxrwxrwx  1 root root      28 Mar 20 18:56 libboost_iostreams.so -> libboost_iostreams.so.1.77.0
-rwxrwxr-x  1 root root  129440 Mar 20 18:56 libboost_iostreams.so.1.77.0
-rw-rw-r--  1 root root  478396 Mar 20 18:57 libboost_json.a
lrwxrwxrwx  1 root root      23 Mar 20 18:56 libboost_json.so -> libboost_json.so.1.77.0
-rwxrwxr-x  1 root root  329512 Mar 20 18:56 libboost_json.so.1.77.0
-rw-rw-r--  1 root root 3158276 Mar 20 18:57 libboost_locale.a
lrwxrwxrwx  1 root root      25 Mar 20 18:56 libboost_locale.so -> libboost_locale.so.1.77.0
-rwxrwxr-x  1 root root 1177200 Mar 20 18:56 libboost_locale.so.1.77.0
```

6.Install Gtest

```bash
sudo git clone https://github.com/google/googletest.git -b release-1.12.0
cd googletest
sudo mkdir build
cd build
sudo cmake .. -DBUILD_GMOCK=OFF
sudo make
sudo make install
```

Install in /usr/local/ by default.

```bash
ls /usr/local/include/
...... gtest
ls /usr/local/lib/ # 32-bit os
ls /usr/local/lib64/ # 64-bit os
...... cmake  libgtest.a  libgtest_main.a
```

### Step 3. Compile StoneDB

Currently, StoneDB has three branches: StoneDB-5.6 (for MySQL 5.6)、 StoneDB-5.7 (for MySQL 5.7) and StoneDB-8.0 (for MySQL 8.0). The link provided in this topic is to the source code package of StoneDB-8.0. In the following example, the source code package is saved to the root directory.

```bash
cd /
git clone https://github.com/stoneatom/stonedb.git
```

Before compilation, modify the compilation script as follows:

Change the installation directory of StoneDB based on your actual conditions. In the example, /stonedb/is used.
Change the installation directories of marisa, RocksDB, and Boost based on your actual conditions.

```bash
cd stonedb
git checkout -b 8.0 origin/stonedb-8.0-dev
mkdir build
cd build
mkdir install8 mysql8
cd mysql8
cmake  ../../ \
-DCMAKE_BUILD_TYPE=Debug \
-DCMAKE_INSTALL_PREFIX=/stonedb/build/install8 \
-DMYSQL_DATADIR=/stonedb/build/install8/data \
-DSYSCONFDIR=/stonedb/build/install8 \
-DMYSQL_UNIX_ADDR=/stonedb/build/install8/tmp/mysql.sock \
-DWITH_BOOST=/usr/local/stonedb-boost177  \
-DWITH_MARISA=/usr/local/stonedb-marisa \
-DWITH_ROCKSDB=/usr/local/stonedb-gcc-rocksdb \
-DDOWNLOAD_BOOST=0
make -j`nproc`
make install -j`nproc`
```

### Step 4. Start StoneDB

You need to manually create directories, and then initialize and start StoneDB. You also need to configure parameters in file my.cnf, including the installation directories and port.

```bash
cd ../install8
### Create directories.
mkdir -p /stonedb/build/install8/data
mkdir -p /stonedb/build/install8/binlog
mkdir -p /stonedb/build/install8/log
mkdir -p /stonedb/build/install8/tmp
mkdir -p /stonedb/build/install8/redolog
mkdir -p /stonedb/build/install8/undolog

### Configure parameters in my.cnf.
mv my.cnf my.cnf.bak
vim /stonedb/install8/my.cnf
[client]
port = 3306
socket          = /stonedb/build/install8/tmp/mysql.sock

[mysqld]
port                            = 3306
basedir                         = /stonedb/build/install8/
character-sets-dir              = /stonedb/build/install8/share/charsets/
lc-messages-dir                 = /stonedb/build/install8/share/
plugin_dir                      = /stonedb/build/install8/lib/plugin/
tmpdir                          = /stonedb/build/install8/tmp/
socket                          = /stonedb/build/install8/tmp/mysql.sock
datadir                         = /stonedb/build/install8/data/
pid-file                        = /stonedb/build/install8/data/mysqld.pid
log-error                       = /stonedb/build/install8/log/mysqld.log
lc-messages-dir                 = /stonedb/build/install8/share/english/
local-infile

### Initialize StoneDB.
sudo ./bin/mysqld --defaults-file=./my.cnf --initialize-insecure

### Start StoneDB.
sudo ./bin/mysqld --user=root &
```

### Step 5.Login to StoneDB

```bash
./bin/mysql -uroot
use mysql;
### user='root' password='stonedb123'
alter user 'root'@'localhost' identified by 'stonedb123';
update user set host='%' where user='root';
```
