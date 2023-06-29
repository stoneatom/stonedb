---
id: compile-using-centos7-for-80
sidebar_position: 5.121
---

# Compile StoneDB for MySQL8.0 on CentOS 7

This topic describes how to compile StoneDB for MySQL8.0 on CentOS 7.

## Precautions

Ensure that the tools and third-party libraries used in your environment meet the following version requirements:

 - GCC 11.2.0
 - Make 3.82 or later
 - CMake 3.7.2 or later
 - marisa 0.77
 - rocksdb 6.12.6
 - boost 1.77
 
 ## Procedure

:::info
Permission issues may occur when executing the following commands. You are advised to run the commands with the administrator permission
:::

:::tip
All commands in this article are run under root privileges by default
:::

### Step 1. Install the dependencies

```bash
yum install -y tree
yum install -y gcc
yum install -y gcc-c++
yum install -y libzstd-devel
yum install -y make
yum install -y ncurses
yum install -y ncurses-devel
yum install -y bison
yum install -y libaio
yum install -y perl
yum install -y perl-DBI
yum install -y perl-DBD-MySQL
yum install -y perl-Time-HiRes
yum install -y readline-devel
yum install -y numactl
yum install -y zlib
yum install -y zlib-devel
yum install -y openssl
yum install -y openssl-devel
yum install -y redhat-lsb-core
yum install -y git
yum install -y autoconf
yum install -y automake
yum install -y libtool
yum install -y lrzsz
yum install -y lz4
yum install -y lz4-devel
yum install -y snappy
yum install -y snappy-devel
yum install -y bzip2
yum install -y bzip2-devel
yum install -y zstd
yum install -y libedit
yum install -y libedit-devel
yum install -y libaio-devel
yum install -y libicu
yum install -y libicu-devel
yum install -y jemalloc-devel
```

:::info
If your current yum source cannot find the required package, configure another epel source, the command is as follows:
```bash
yum install -y epel-release
```
:::

### Step 2. Install GCC 11.2.0

Before performing the follow-up steps, you must ensure the GCC version is 11.2.0.You can run the following command to check the GCC version.

```bash
gcc --version
```

If the version is earlier than 11.0.0, perform the following steps to upgrade GCC.

#### 1. Install the SCL utility.

```bash
yum install centos-release-scl scl-utils-build -y
```

#### 2. Install GCC, GCC-C++, or BINUTILS of version 11.2.0.

```bash
yum install devtoolset-11-gcc devtoolset-11-gcc-c++ devtoolset-11-binutils -y
```

#### 3. Switch the version to 11.2.0.

```bash
scl enable devtoolset-11 bash
```

#### 4. Check that the version is switched to 11.2.0.

```bash
gcc --version
```

### Step 3. Install third-party libraries

StoneDB is dependent on marisa, rocksdb, and boost. You are advised to specify paths for saving these libraries when you install them, instead of using the default paths.

#### 1. Install cmake

Check if the current cmake version meets the installation requirements.

```bash
cmake --version
```

If your cmake version >= 3.7.2, you don’t need to install cmake, otherwise follow the steps below to compile and install the appropriate version of cmake.

```bash
wget https://cmake.org/files/v3.7/cmake-3.7.2.tar.gz
tar -zxvf cmake-3.7.2.tar.gz
cd cmake-3.7.2
./bootstrap && make && make install
/usr/local/bin/cmake --version
rm -rf /usr/bin/cmake
ln -s /usr/local/bin/cmake /usr/bin/cmake
cmake --version
```

:::info
GCC11 will report a compilation error when compiling cmake-3.7.2. You can add `#include <limits>` in the beginning of `cmake-3.7.2/Source/cmServerProtocol.cxx` to solve it.
```c++
#include <algorithm>
#include <string>
#include <vector>
#include <limits>
```
:::

#### 2. Install make

Check if the current make version meets the installation requirements.

```bash
make --version
```

If your make version >= 3.82, you don’t need to install make, otherwise follow the steps below to compile and install the appropriate version of make.

```bash
wget http://mirrors.ustc.edu.cn/gnu/make/make-4.3.tar.gz
tar -zxvf make-4.3.tar.gz
cd make-4.3
./configure  --prefix=/usr/local/make
make && make install
/usr/local/make/bin/make --version
rm -rf /usr/bin/make
ln -s /usr/local/make/bin/make /usr/bin/make
make --version
```

#### 3. Install marisa

```bash
git clone https://github.com/s-yata/marisa-trie.git
cd marisa-trie
autoreconf -i
./configure --enable-native-code --prefix=/usr/local/stonedb-marisa
sudo make && make install 
```

The installation directory of marisa in the example is `/usr/local/stonedb-marisa`. You can change it based on your actual conditions. After compiling and installing, the following directories and files are generated in `/usr/local/stonedb-marisa/lib`.

```bash
[root@localhost /usr/local/stonedb-marisa/lib]#ll
total 6768
-rw-r--r--. 1 root root 4977788 May  5 07:33 libmarisa.a
-rwxr-xr-x. 1 root root     946 May  5 07:33 libmarisa.la
lrwxrwxrwx. 1 root root      18 May  5 07:33 libmarisa.so -> libmarisa.so.0.0.0
lrwxrwxrwx. 1 root root      18 May  5 07:33 libmarisa.so.0 -> libmarisa.so.0.0.0
-rwxr-xr-x. 1 root root 1945584 May  5 07:33 libmarisa.so.0.0.0
drwxr-xr-x. 2 root root      23 May  5 07:33 pkgconfig
```

#### 4. Install rocksdb

```bash
wget https://github.com/facebook/rocksdb/archive/refs/tags/v6.12.6.tar.gz
tar -zxvf v6.12.6.tar.gz
cd rocksdb-6.12.6

cmake ./ \
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

make -j`nproc`
make install -j`nproc`
```

The installation directory of rocksdb in the example is `/usr/local/stonedb-gcc-rocksdb`. You can change it based on your actual conditions. After compiling and installing, the following directories and files are generated in `/usr/local/stonedb-gcc-rocksdb`.

```bash
[root@localhost /usr/local/stonedb-gcc-rocksdb]#ll
total 28736
drwxr-xr-x. 3 root root       21 May  5 07:39 cmake
drwxr-xr-x. 3 root root       21 May  5 07:39 include
-rw-r--r--. 1 root root 20332942 May  5 07:38 librocksdb.a
lrwxrwxrwx. 1 root root       15 May  5 07:39 librocksdb.so -> librocksdb.so.6
lrwxrwxrwx. 1 root root       20 May  5 07:39 librocksdb.so.6 -> librocksdb.so.6.12.6
-rwxr-xr-x. 1 root root  9086272 May  5 07:39 librocksdb.so.6.12.6
```

#### 5. Install boost

```bash
wget https://sourceforge.net/projects/boost/files/boost/1.77.0/boost_1_77_0.tar.gz
tar -zxvf boost_1_77_0.tar.gz
cd boost_1_77_0
./bootstrap.sh --prefix=/usr/local/stonedb-boost177
./b2 install --with=all
```

The installation directory of boost in the example is `/usr/local/stonedb-boost177`. You can change it based on your actual conditions. After compiling and installing, the following directories and files are generated in `/usr/local/stonedb-boost177/lib`.

```bash
[root@localhost /usr/local/stonedb-boost177/lib]#ll
total 43412
drwxr-xr-x. 50 root root    4096 May  5 08:34 cmake
-rw-r--r--.  1 root root   14152 May  5 08:33 libboost_atomic.a
lrwxrwxrwx.  1 root root      25 May  5 08:34 libboost_atomic.so -> libboost_atomic.so.1.77.0
-rwxr-xr-x.  1 root root   27632 May  5 08:34 libboost_atomic.so.1.77.0
-rw-r--r--.  1 root root  182964 May  5 08:33 libboost_chrono.a
lrwxrwxrwx.  1 root root      25 May  5 08:34 libboost_chrono.so -> libboost_chrono.so.1.77.0
-rwxr-xr-x.  1 root root  161408 May  5 08:34 libboost_chrono.so.1.77.0
-rw-r--r--.  1 root root  172262 May  5 08:33 libboost_container.a
lrwxrwxrwx.  1 root root      28 May  5 08:34 libboost_container.so -> libboost_container.so.1.77.0
-rwxr-xr-x.  1 root root  116424 May  5 08:34 libboost_container.so.1.77.0
-rw-r--r--.  1 root root    7202 May  5 08:33 libboost_context.a
lrwxrwxrwx.  1 root root      26 May  5 08:34 libboost_context.so -> libboost_context.so.1.77.0
-rwxr-xr-x.  1 root root   17024 May  5 08:34 libboost_context.so.1.77.0
-rw-r--r--.  1 root root  284652 May  5 08:33 libboost_contract.a
lrwxrwxrwx.  1 root root      27 May  5 08:34 libboost_contract.so -> libboost_contract.so.1.77.0
-rwxr-xr-x.  1 root root  254408 May  5 08:34 libboost_contract.so.1.77.0
-rw-r--r--.  1 root root  196532 May  5 08:33 libboost_coroutine.a
lrwxrwxrwx.  1 root root      28 May  5 08:34 libboost_coroutine.so -> libboost_coroutine.so.1.77.0
-rwxr-xr-x.  1 root root   84592 May  5 08:34 libboost_coroutine.so.1.77.0
-rw-r--r--.  1 root root    1466 May  5 08:33 libboost_date_time.a
lrwxrwxrwx.  1 root root      28 May  5 08:34 libboost_date_time.so -> libboost_date_time.so.1.77.0
-rwxr-xr-x.  1 root root   15832 May  5 08:34 libboost_date_time.so.1.77.0
-rw-r--r--.  1 root root    1662 May  5 08:34 libboost_exception.a
-rw-r--r--.  1 root root  255586 May  5 08:33 libboost_fiber.a
lrwxrwxrwx.  1 root root      24 May  5 08:32 libboost_fiber.so -> libboost_fiber.so.1.77.0
-rwxr-xr-x.  1 root root  108704 May  5 08:32 libboost_fiber.so.1.77.0
-rw-r--r--.  1 root root  520624 May  5 08:33 libboost_filesystem.a
lrwxrwxrwx.  1 root root      29 May  5 08:32 libboost_filesystem.so -> libboost_filesystem.so.1.77.0
-rwxr-xr-x.  1 root root  271120 May  5 08:32 libboost_filesystem.so.1.77.0
-rw-r--r--.  1 root root 1039814 May  5 08:33 libboost_graph.a
lrwxrwxrwx.  1 root root      24 May  5 08:32 libboost_graph.so -> libboost_graph.so.1.77.0
-rwxr-xr-x.  1 root root  504312 May  5 08:32 libboost_graph.so.1.77.0
-rw-r--r--.  1 root root  263000 May  5 08:33 libboost_iostreams.a
lrwxrwxrwx.  1 root root      28 May  5 08:32 libboost_iostreams.so -> libboost_iostreams.so.1.77.0
-rwxr-xr-x.  1 root root  124640 May  5 08:32 libboost_iostreams.so.1.77.0
-rw-r--r--.  1 root root  491206 May  5 08:33 libboost_json.a
lrwxrwxrwx.  1 root root      23 May  5 08:32 libboost_json.so -> libboost_json.so.1.77.0
-rwxr-xr-x.  1 root root  426856 May  5 08:32 libboost_json.so.1.77.0
-rw-r--r--.  1 root root 3221762 May  5 08:33 libboost_locale.a
lrwxrwxrwx.  1 root root      25 May  5 08:32 libboost_locale.so -> libboost_locale.so.1.77.0
-rwxr-xr-x.  1 root root 1088632 May  5 08:32 libboost_locale.so.1.77.0
-rw-r--r--.  1 root root 3715884 May  5 08:33 libboost_log.a
-rw-r--r--.  1 root root 3141980 May  5 08:33 libboost_log_setup.a
lrwxrwxrwx.  1 root root      28 May  5 08:32 libboost_log_setup.so -> libboost_log_setup.so.1.77.0
-rwxr-xr-x.  1 root root 1363320 May  5 08:32 libboost_log_setup.so.1.77.0
lrwxrwxrwx.  1 root root      22 May  5 08:32 libboost_log.so -> libboost_log.so.1.77.0
-rwxr-xr-x.  1 root root 1325072 May  5 08:32 libboost_log.so.1.77.0
-rw-r--r--.  1 root root  201806 May  5 08:33 libboost_math_c99.a
-rw-r--r--.  1 root root  185294 May  5 08:33 libboost_math_c99f.a
lrwxrwxrwx.  1 root root      28 May  5 08:32 libboost_math_c99f.so -> libboost_math_c99f.so.1.77.0
-rwxr-xr-x.  1 root root   71888 May  5 08:32 libboost_math_c99f.so.1.77.0
-rw-r--r--.  1 root root  184610 May  5 08:33 libboost_math_c99l.a
lrwxrwxrwx.  1 root root      28 May  5 08:32 libboost_math_c99l.so -> libboost_math_c99l.so.1.77.0
-rwxr-xr-x.  1 root root   70640 May  5 08:32 libboost_math_c99l.so.1.77.0
lrwxrwxrwx.  1 root root      27 May  5 08:32 libboost_math_c99.so -> libboost_math_c99.so.1.77.0
-rwxr-xr-x.  1 root root   73640 May  5 08:32 libboost_math_c99.so.1.77.0
-rw-r--r--.  1 root root 1168442 May  5 08:33 libboost_math_tr1.a
-rw-r--r--.  1 root root 1190212 May  5 08:33 libboost_math_tr1f.a
lrwxrwxrwx.  1 root root      28 May  5 08:32 libboost_math_tr1f.so -> libboost_math_tr1f.so.1.77.0
-rwxr-xr-x.  1 root root  278584 May  5 08:32 libboost_math_tr1f.so.1.77.0
-rw-r--r--.  1 root root 1137874 May  5 08:33 libboost_math_tr1l.a
lrwxrwxrwx.  1 root root      28 May  5 08:32 libboost_math_tr1l.so -> libboost_math_tr1l.so.1.77.0
-rwxr-xr-x.  1 root root  289168 May  5 08:32 libboost_math_tr1l.so.1.77.0
lrwxrwxrwx.  1 root root      27 May  5 08:32 libboost_math_tr1.so -> libboost_math_tr1.so.1.77.0
-rwxr-xr-x.  1 root root  294848 May  5 08:32 libboost_math_tr1.so.1.77.0
-rw-r--r--.  1 root root   22466 May  5 08:33 libboost_nowide.a
lrwxrwxrwx.  1 root root      25 May  5 08:32 libboost_nowide.so -> libboost_nowide.so.1.77.0
-rwxr-xr-x.  1 root root   24712 May  5 08:32 libboost_nowide.so.1.77.0
-rw-r--r--.  1 root root  176758 May  5 08:34 libboost_prg_exec_monitor.a
lrwxrwxrwx.  1 root root      35 May  5 08:32 libboost_prg_exec_monitor.so -> libboost_prg_exec_monitor.so.1.77.0
-rwxr-xr-x.  1 root root  195640 May  5 08:32 libboost_prg_exec_monitor.so.1.77.0
-rw-r--r--.  1 root root 1319422 May  5 08:33 libboost_program_options.a
lrwxrwxrwx.  1 root root      34 May  5 08:32 libboost_program_options.so -> libboost_program_options.so.1.77.0
-rwxr-xr-x.  1 root root  651768 May  5 08:32 libboost_program_options.so.1.77.0
-rw-r--r--.  1 root root   63270 May  5 08:33 libboost_random.a
lrwxrwxrwx.  1 root root      25 May  5 08:32 libboost_random.so -> libboost_random.so.1.77.0
-rwxr-xr-x.  1 root root  152064 May  5 08:32 libboost_random.so.1.77.0
-rw-r--r--.  1 root root  797262 May  5 08:33 libboost_regex.a
lrwxrwxrwx.  1 root root      24 May  5 08:32 libboost_regex.so -> libboost_regex.so.1.77.0
-rwxr-xr-x.  1 root root  473968 May  5 08:32 libboost_regex.so.1.77.0
-rw-r--r--.  1 root root 1177984 May  5 08:34 libboost_serialization.a
lrwxrwxrwx.  1 root root      32 May  5 08:32 libboost_serialization.so -> libboost_serialization.so.1.77.0
-rwxr-xr-x.  1 root root  457280 May  5 08:32 libboost_serialization.so.1.77.0
-rw-r--r--.  1 root root   23484 May  5 08:34 libboost_stacktrace_addr2line.a
lrwxrwxrwx.  1 root root      39 May  5 08:32 libboost_stacktrace_addr2line.so -> libboost_stacktrace_addr2line.so.1.77.0
-rwxr-xr-x.  1 root root  125792 May  5 08:32 libboost_stacktrace_addr2line.so.1.77.0
-rw-r--r--.  1 root root   13906 May  5 08:34 libboost_stacktrace_basic.a
lrwxrwxrwx.  1 root root      35 May  5 08:32 libboost_stacktrace_basic.so -> libboost_stacktrace_basic.so.1.77.0
-rwxr-xr-x.  1 root root   22456 May  5 08:32 libboost_stacktrace_basic.so.1.77.0
-rw-r--r--.  1 root root    2938 May  5 08:33 libboost_stacktrace_noop.a
lrwxrwxrwx.  1 root root      34 May  5 08:32 libboost_stacktrace_noop.so -> libboost_stacktrace_noop.so.1.77.0
-rwxr-xr-x.  1 root root   16376 May  5 08:32 libboost_stacktrace_noop.so.1.77.0
-rw-r--r--.  1 root root    1436 May  5 08:33 libboost_system.a
lrwxrwxrwx.  1 root root      25 May  5 08:32 libboost_system.so -> libboost_system.so.1.77.0
-rwxr-xr-x.  1 root root   15816 May  5 08:32 libboost_system.so.1.77.0
-rw-r--r--.  1 root root 2331082 May  5 08:33 libboost_test_exec_monitor.a
-rw-r--r--.  1 root root  322240 May  5 08:33 libboost_thread.a
lrwxrwxrwx.  1 root root      25 May  5 08:34 libboost_thread.so -> libboost_thread.so.1.77.0
-rwxr-xr-x.  1 root root  260920 May  5 08:34 libboost_thread.so.1.77.0
-rw-r--r--.  1 root root   53892 May  5 08:34 libboost_timer.a
lrwxrwxrwx.  1 root root      24 May  5 08:33 libboost_timer.so -> libboost_timer.so.1.77.0
-rwxr-xr-x.  1 root root   45472 May  5 08:33 libboost_timer.so.1.77.0
-rw-r--r--.  1 root root  123412 May  5 08:34 libboost_type_erasure.a
lrwxrwxrwx.  1 root root      31 May  5 08:33 libboost_type_erasure.so -> libboost_type_erasure.so.1.77.0
-rwxr-xr-x.  1 root root   76952 May  5 08:33 libboost_type_erasure.so.1.77.0
-rw-r--r--.  1 root root 2312932 May  5 08:34 libboost_unit_test_framework.a
lrwxrwxrwx.  1 root root      38 May  5 08:33 libboost_unit_test_framework.so -> libboost_unit_test_framework.so.1.77.0
-rwxr-xr-x.  1 root root 1028120 May  5 08:33 libboost_unit_test_framework.so.1.77.0
-rw-r--r--.  1 root root 4746706 May  5 08:34 libboost_wave.a
lrwxrwxrwx.  1 root root      23 May  5 08:33 libboost_wave.so -> libboost_wave.so.1.77.0
-rwxr-xr-x.  1 root root 1599392 May  5 08:33 libboost_wave.so.1.77.0
-rw-r--r--.  1 root root  779892 May  5 08:34 libboost_wserialization.a
lrwxrwxrwx.  1 root root      33 May  5 08:33 libboost_wserialization.so -> libboost_wserialization.so.1.77.0
-rwxr-xr-x.  1 root root  320040 May  5 08:33 libboost_wserialization.so.1.77.0
```

#### 6. Install gtest

```bash
git clone https://github.com/google/googletest.git -b release-1.12.0
cd googletest
mkdir build
cd build
cmake .. -DBUILD_GMOCK=OFF
make
make install
```

Install in `/usr/local/` by default.

```bash
ls /usr/local/include/
...... gtest
ls /usr/local/lib/ # 32-bit os
ls /usr/local/lib64/ # 64-bit os
...... cmake  libgtest.a  libgtest_main.a
```

### Step 4. Compile StoneDB

Currently, StoneDB has three branches: StoneDB-5.6 (for MySQL 5.6)、 StoneDB-5.7 (for MySQL 5.7) and StoneDB-8.0 (for MySQL 8.0). The link provided in this topic is to the source code package of StoneDB-8.0. In the following example, the source code package is saved to the root directory.

```bash
cd /
git clone -b stonedb-8.0-dev https://github.com/stoneatom/stonedb.git
```

Before compilation, modify the compilation script as follows:

:::info
You cna change the installation directory of StoneDB based on your actual conditions. In this example, `/stonedb/`is used. Remember to change the installation directories of marisa, rocksdb, and boost based on your actual conditions.
:::

```bash
cd stonedb
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

### Step 5. Start StoneDB

You need to manually create directories, and then initialize and start StoneDB. You also need to configure parameters in file my.cnf, including the installation directories and port.

```bash
cd ../install8

### Create directories.
mkdir data binlog log tmp redolog undolog

### Configure parameters in my.cnf.
cp ../../scripts/my.cnf.sample my.cnf
sed -i "s|YOUR_ABS_PATH|$(pwd)|g" my.cnf

### Initialize StoneDB.
./bin/mysqld --defaults-file=./my.cnf --initialize-insecure

### Start StoneDB.
./bin/mysqld --user=root &

### Stop StoneDB
./bin/mysqladmin -uroot shutdown
```

### Step 6. Login to StoneDB

```bash
./bin/mysql -uroot
### set the password of user root to stonedb123
mysql> alter user 'root'@'localhost' identified by 'stonedb123';
Query OK, 0 rows affected
### allow remote access
mysql> update user set host='%' where user='root';
```

