---
id: compile-using-redhat7
sidebar_position: 5.13
---

# Compile StoneDB on RHEL 7

This topic describes how to compile StoneDB on Red Hat Enterprise Linux (RHEL) 7.
## Prerequisites
The source code of StoneDB has been downloaded.

Download link: [https://github.com/stoneatom/stonedb.git](https://github.com/stoneatom/stonedb.git) 
## Procedure
### Step 1. Install the dependencies
```shell
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
```
### Step 2. Install GCC 7.3.0
Before executing **stonedb_build.sh** to compile StoneDB, you must ensure the GCC version is 7.3.0.

You can run the following command to check the GCC version.
```shell
gcc --version
```
If the version is earlier than 7.3.0, perform the following steps to upgrade GCC.

1. Install the SCL utility.
```shell
yum install centos-release-scl scl-utils-build -y
```

2. Install GCC, GCC-C++, or GDB of version 7.3.0.
```shell
yum install devtoolset-7-gcc.x86_64 devtoolset-7-gcc-c++.x86_64 devtoolset-7-gcc-gdb-plugin.x86_64 -y
```

3. Switch the version to 7.3.0.
```shell
scl enable devtoolset-7 bash
```

4. Check that the version is switched to 7.3.0.
```shell
gcc --version
```
### Step 3. Install CMake, Make, and third-party libraries
Before compiling StoneDB, install CMake 3.7 or later, Make 3.82 or later, and the following third-party libraries: marisa, RocksDB, and Boost.

1. Install CMake.
```shell
wget https://cmake.org/files/v3.7/cmake-3.7.2.tar.gz
tar -zxvf cmake-3.7.2.tar.gz
cd cmake-3.7.2
./bootstrap && make && make install
/usr/local/bin/cmake --version
apt remove cmake -y
ln -s /usr/local/bin/cmake /usr/bin/
```

2. Install Make.
```shell
http://mirrors.ustc.edu.cn/gnu/make/
tar -zxvf make-3.82.tar.gz
./configure  --prefix=/usr/local/make
make && make install
rm -rf /usr/local/bin/make
ln -s /usr/local/make/bin/make /usr/local/bin/make
```

3. Install marisa.
```shell
git clone https://github.com/s-yata/marisa-trie.git
cd marisa-trie
autoreconf -i
./configure --enable-native-code --prefix=/usr/local/stonedb-marisa
make && make install 
```

4. Install RocksDB.
```shell
wget https://github.com/facebook/rocksdb/archive/refs/tags/v6.12.6.tar.gz
tar -zxvf v6.12.6.tar.gz
cd rocksdb-6.12.6
make shared_lib
make install-shared INSTALL_PATH=/usr/local/stonedb-gcc-rocksdb
make static_lib
make install-static INSTALL_PATH=/usr/local/stonedb-gcc-rocksdb
```

5. Install Boost.
```shell
wget https://sourceforge.net/projects/boost/files/boost/1.66.0/boost_1_66_0.tar.gz
tar -zxvf boost_1_66_0.tar.gz
cd boost_1_66_0
./bootstrap.sh --prefix=/usr/local/stonedb-boost
./b2 install --with=all
```
### Step 4. Compile StoneDB
Execute the following script to compile StoneDB:
```shell
cd /stonedb/scripts
./stonedb_build.sh
```
After the compilation is complete, a folder named **/stonedb57** is generated.
### Step 5. Start StoneDB
Perform the following steps to start StoneDB.

1. Create an account.
```shell
groupadd mysql
useradd -g mysql mysql
passwd mysql
```

2. Execute **reinstall.sh**.
```shell
cd /stonedb57/install
./reinstall.sh
```
:::info
The aim of executing the script is to initialize and start StoneDB.
:::

3. Log in to StoneDB.

Before you log in to StoneDB, you must find the password of the super admin in **/stonedb57/install/log/mysqld.log**.
```shell
/stonedb56/install/bin/mysql -uroot -p -S /stonedb56/install/tmp/mysql.sock
Warning: Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1
Server version: 5.6.24-StoneDB-log build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| cache              |
| innodb             |
| test               |
+--------------------+
4 rows in set (0.08 sec)
```