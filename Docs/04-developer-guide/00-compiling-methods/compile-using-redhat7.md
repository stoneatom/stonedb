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
Before installing the dependencies, ensure that the GCC version in use is 4.8.5.
```bash
yum install -y gcc
yum install -y g++
yum install -y make
yum install -y build-essential
yum install -y autoconf
yum install -y tree
yum install -y bison
yum install -y git
yum install -y cmake
yum install -y libtool
yum install -y numactl
yum install -y python
yum install -y openssl
yum install -y perl
yum install -y binutils
yum install -y libgmp-dev
yum install -y libmpfr-dev
yum install -y libmpc-dev
yum install -y libisl-dev
yum install -y zlib1g-dev
yum install -y liblz4-dev
yum install -y libbz2-dev
yum install -y libzstd-dev
yum install -y lz4
yum install -y ncurses-dev
yum install -y libsnappy-dev
```
### Step 2. Install CMake and third-party libraries
Before compiling StoneDB, install CMake 3.7 or later and the following third-party libraries: marisa, RocksDB, and Boost.

1. Install CMake.
```bash
wget https://cmake.org/files/v3.7/cmake-3.7.2.tar.gz
tar -zxvf cmake-3.7.2.tar.gz
cd cmake-3.7.2
./bootstrap && make && make install
/usr/local/bin/cmake --version
apt remove cmake -y
ln -s /usr/local/bin/cmake /usr/bin/
```

2. Install marisa.
```bash
git clone https://github.com/s-yata/marisa-trie.git
cd marisa-trie
autoreconf -i
./configure --enable-native-code --prefix=/usr/local/stonedb-marisa
make && make install 
```

3. Install RocksDB.
```bash
wget https://github.com/facebook/rocksdb/archive/refs/tags/v4.13.tar.gz
tar -zxvf v4.13.tar.gz
cd rocksdb-4.13
make shared_lib
make install-shared INSTALL_PATH=/usr/local/stonedb-gcc-rocksdb
make static_lib
make install-static INSTALL_PATH=/usr/local/stonedb-gcc-rocksdb
```
:::info

Boost is automatically installed when the **stonedb_build.sh** script is executed in step 4. 

:::
### Step 3. Install GCC 7.3.0
Before executing **stonedb_build.sh** to compile StoneDB, you must ensure the GCC version is 7.3.0.

You can run the following command to check the GCC version.
```bash
gcc --version
```
If the version is earlier than 7.3.0, perform the following steps to upgrade GCC.

1. Install the SCL utility.
```bash
yum install centos-release-scl scl-utils-build -y
```

2. Install GCC, GCC-C++, or GDB of v7.3.0.
```bash
yum install devtoolset-7-gcc.x86_64 devtoolset-7-gcc-c++.x86_64 devtoolset-7-gcc-gdb-plugin.x86_64 -y
```

3. Switch the version to 7.3.0.
```bash
scl enable devtoolset-7 bash
```

4. Check that the version is switched to 7.3.0.
```bash
gcc --version
```
### Step 4. Compile StoneDB
Execute the following script to compile StoneDB:
```bash
cd /stonedb2022/scripts
./stonedb_build.sh
```
After the compilation is complete, a folder named **/stonedb56** is generated.
### **Step 5. Start StoneDB**
Perform the following steps to start StoneDB.

1. Create a group, a user, and relevant directories.
```bash
groupadd mysql
useradd -g mysql mysql
mkdir -p /stonedb56/install/{log/,tmp/,binlog/,data/innodb} && chown -R mysql:mysql /stonedb56
```

2. Start StoneDB.
```bash
cd /stonedb56/install/bin
./mysqld_safe --defaults-file=/stonedb56/install/stonedb.cnf --user=mysql &
```

3. Log in to StoneDB.
```bash
#./mysql -uroot -p -S /stonedb56/install/tmp/mysql.sock
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
