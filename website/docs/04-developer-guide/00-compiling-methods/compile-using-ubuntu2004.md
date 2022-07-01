---
id: compile-using-ubuntu20.04
sidebar_position: 5.14
---

# Compile StoneDB on Ubuntu 20.04

This topic describes how to compile StoneDB on Ubuntu 20.04.

## Step 1. Install GCC 7.3.0
Ubuntu 20.04 LTS uses GCC 9.4.0, by default. You must downgrade the GCC version to 7.3.0, because StoneDB can be complied only on GCC 7.3.0.
### 1. Install the dependencies

```shell
sudo apt install gcc
sudo apt install g++
sudo apt install make
sudo apt install build-essential
sudo apt install autoconf
sudo apt install tree
sudo apt install bison
sudo apt install git
sudo apt install cmake
sudo apt install libtool
sudo apt install numactl
sudo apt install python
sudo apt install openssl
sudo apt install perl
sudo apt install binutils
sudo apt install libgmp-dev
sudo apt install libmpfr-dev
sudo apt install libmpc-dev
sudo apt install libisl-dev
sudo apt install zlib1g-dev
sudo apt install liblz4-dev
sudo apt install libbz2-dev
sudo apt install libzstd-dev
sudo apt install lz4
sudo apt install ncurses-dev
sudo apt install libsnappy-dev
```
:::info

Ensure that all the dependencies are installed. Otherwise, a large number of errors will be reported.

:::
### 2. Decompress the source code package of GCC 7.3.0

[Download](http://ftp.gnu.org/gnu/gcc/), upload, and then decompress the source code package of GCC 7.3.0.

```shell
cd /home
tar -zxvf gcc-7.3.0.tar.gz
```
### 3. Prepare for compiling GCC
Modify the source code. 

```shell
cd /home/gcc-7.3.0/libsanitizer/sanitizer_common
cp sanitizer_platform_limits_posix.cc sanitizer_platform_limits_posix.cc.bak
vim sanitizer_platform_limits_posix.cc
# 1. Comment out row 158.
//#include <sys/ustat.h>
# 2. Add the following code after row 250.
// Use pre-computed size of struct ustat to avoid <sys/ustat.h> which
// has been removed from glibc 2.28.
#if defined(__aarch64__) || defined(__s390x__) || defined (__mips64) \
  || defined(__powerpc64__) || defined(__arch64__) || defined(__sparcv9) \
  || defined(__x86_64__)
#define SIZEOF_STRUCT_USTAT 32
#elif defined(__arm__) || defined(__i386__) || defined(__mips__) \
  || defined(__powerpc__) || defined(__s390__)
#define SIZEOF_STRUCT_USTAT 20
#else
#error Unknown size of struct ustat
#endif
  unsigned struct_ustat_sz = SIZEOF_STRUCT_USTAT;
```

*Here's a picture to add*

:::info

If GCC is compiled without the modification, an error will be reported, indicating that **sys/ustat.h** does not exist.

:::

### 4. Compile GCC
```
mkdir /gcc
cd /home/gcc-7.3.0
./contrib/download_prerequisites
./configure --prefix=/gcc --enable-bootstrap -enable-threads=posix --enable-checking=release --enable-languages=c,c++ --disable-multilib --disable-libsanitizer
sudo make && make install
```
### 5. Check the GCC version
```
# /gcc/bin/gcc --version
gcc (GCC) 7.3.0
Copyright (C) 2017 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
```
### 6. Delete GCC and G++ versions that are later than 7.3.0
```
sudo rm /usr/bin/gcc
sudo ln -s /gcc/bin/gcc /usr/bin/gcc
sudo rm /usr/bin/g++
sudo ln -s /gcc/bin/g++ /usr/bin/g++

# gcc --version
gcc (GCC) 7.3.0
Copyright (C) 2017 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

# g++ --version
g++ (GCC) 7.3.0
Copyright (C) 2017 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

# c++ --version
c++ (GCC) 7.3.0
Copyright (C) 2017 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

```
## Step 2. Compile StoneDB
### 1. Download the source code package of StoneDB
Download the source code from [https://github.com/stoneatom/stonedb.git](https://github.com/stoneatom/stonedb.git).
### 2. Install CMake and the third-party libraries

1. Install CMake.
```
wget https://cmake.org/files/v3.7/cmake-3.7.2.tar.gz
tar -zxvf cmake-3.7.2.tar.gz
cd cmake-3.7.2
./bootstrap && make && make install
/usr/local/bin/cmake --version
apt remove cmake -y
ln -s /usr/local/bin/cmake /usr/bin/
```

2. Install marisa.
```
git clone https://github.com/s-yata/marisa-trie.git
cd marisa-trie
autoreconf -i
./configure --enable-native-code --prefix=/usr/local/stonedb-marisa
make && make install 
```
The directories and files shown in the following figure are generated in directory **/usr/local/stonedb-marisa**.

*Here's a picture to add*

1. Install GCC 4.8.5 in an offline manner and configure it to be the default version.
```shell
sudo vim /etc/apt/sources.list
# Append the image sources to the paths.
deb http://dk.archive.ubuntu.com/ubuntu/ xenial main
deb http://dk.archive.ubuntu.com/ubuntu/ xenial universe
# Install GCC 4.8.5.
sudo apt update
sudo apt install gcc-4.8
sudo apt install gcc-4.8-locales
sudo apt install gcc-4.8-multilib
sudo apt install gcc-4.8-doc
sudo apt install g++-4.8
# Switch the GCC version to 4.8.5.
update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-4.8 20
update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-4.8 20
# Check the GCC version.
# gcc --version
gcc (Ubuntu 4.8.5-4ubuntu2) 4.8.5
Copyright (C) 2015 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

# c++ --version
c++ (Ubuntu 4.8.5-4ubuntu2) 4.8.5
Copyright (C) 2015 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

# g++ --version
g++ (Ubuntu 4.8.5-4ubuntu2) 4.8.5
Copyright (C) 2015 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
```

4. Install RocksDB.
```shell
wget https://github.com/facebook/rocksdb/archive/refs/tags/v4.13.tar.gz
tar -zxvf v4.13.tar.gz
cd rocksdb-4.13
make shared_lib
make install-shared INSTALL_PATH=/usr/local/stonedb-gcc-rocksdb
make static_lib
make install-static INSTALL_PATH=/usr/local/stonedb-gcc-rocksdb
```
The directories and files shown in the following figure are generated in directory **/usr/local/stonedb-gcc-rocksdb**.

*Here's a picture to add*

1. Switch the GCC version back to 7.3.0. Otherwise, errors will be reported.
```shell
sudo rm /usr/bin/gcc
sudo ln -s /gcc/bin/gcc /usr/bin/gcc
sudo rm /usr/bin/g++
sudo ln -s /gcc/bin/g++ /usr/bin/g++
```

6. Install Boost.

Boost can be automatically installed when you execute the **stonedb_build.sh** script stored in directory** /stonedb2022/scripts**. The following code shows how to manually install Boost.

```shell
tar -zxvf boost_1_66_0.tar.gz
cd boost_1_66_0
./bootstrap.sh --prefix=/usr/local/stonedb-boost
./b2 install --with=all
```

The files and directories shown in the following figure are generated in directory **/usr/local/stonedb-boost**.

*Here's a picture to add*
### 3. Compile StoneDB

1. Modify script** stonedb_build.sh**.

```shell
vim /stonedb2022/scripts/stonedb_build.sh
cmake ../../ \
-DCMAKE_BUILD_TYPE=${build_type} \
-DCMAKE_INSTALL_PREFIX=${install_target} \
-DMYSQL_DATADIR=${install_target}/data \
-DSYSCONFDIR=${install_target} \
-DMYSQL_UNIX_ADDR=${install_target}/tmp/mysql.sock \
-DWITH_EMBEDDED_SERVER=OFF \
-DWITH_STONEDB_STORAGE_ENGINE=1 \
-DWITH_MYISAM_STORAGE_ENGINE=1 \
-DWITH_INNOBASE_STORAGE_ENGINE=1 \
-DWITH_PARTITION_STORAGE_ENGINE=1 \
-DMYSQL_TCP_PORT=3306 \
-DENABLED_LOCAL_INFILE=1 \
-DEXTRA_CHARSETS=all \
-DDEFAULT_CHARSET=utf8 \
-DDEFAULT_COLLATION=utf8_general_ci \
-DDOWNLOAD_BOOST=0 \
-DWITH_BOOST=/usr/local/stonedb-boost/include \
-DCMAKE_CXX_FLAGS='-D_GLIBCXX_USE_CXX11_ABI=0'

cd /stonedb2022/scripts/
./stonedb_build.sh
```

After the compilation is complete, directory **/stonedb56** is generated.

:::info

- Because Boost in this example is manually installed, the value of **-DWITH_BOOST** must be set to **/usr/local/stonedb-boost/include**.
- For compatibility purposes, **-DCMAKE_CXX_FLAGS='-D_GLIBCXX_USE_CXX11_ABI=0** must be included in the script. Otherwise, an error will be reported when the complication progress reaches 82%.

:::
## Step 3. Start StoneDB
Perform the following steps to start StoneDB.
### 1. Create a user group, a user, and directories
```sql
groupadd mysql
useradd -g mysql mysql
mkdir -p /stonedb56/install/{log/,tmp/,binlog/,data/innodb} && chown -R mysql:mysql /stonedb56
```
### 2. Start StoneDB
```sql
cd /stonedb56/install/bin
./mysqld_safe --defaults-file=/stonedb56/install/stonedb.cnf --user=mysql &
```
### 3. Log in to StoneDB

```shell
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
