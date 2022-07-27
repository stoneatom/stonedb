---
id: compile-using-centos7
sidebar_position: 5.12
---

# CentOS 7 下编译StoneDB

## 下载源码包
下载地址： [https://github.com/stoneatom/stonedb.git](https://github.com/stoneatom/stonedb.git)
完成源码包下载后，需要执行以下五个步骤，完成编译工作
第一步：安装依赖文件
第二步：安装gcc 7.3.0
第三步：安装第三方库
第四步：执行编译
第五步：启动实例
## 第一步：安装依赖包
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
## 第二步：安装 gcc 7.3.0
执行stonedb_build.sh编译安装数据库要求版本在 gcc  7.3.0 。
通过执行以下语句，检查当前 gcc 版本是否符合安装要求
```shell
gcc --version
```
如果版本不符合要求，按照以下步骤将 gcc 切换为正确版本。
### 1. 安装 scl 源
```shell
yum install centos-release-scl scl-utils-build -y
```
### 2. 安装 7.3.0 版本的 gcc、gcc-c++、gdb 
```shell
yum install devtoolset-7-gcc.x86_64 devtoolset-7-gcc-c++.x86_64 devtoolset-7-gcc-gdb-plugin.x86_64 -y
```
### 3. 切换至 7.3.0 版本
```shell
scl enable devtoolset-7 bash
```
### 4. 执行版本检查，确保是7.3.0以上
```shell
gcc --version
```
## 第三步：安装第三方库
对 StoneDB 执行编译依赖于第三方库： marisa/rocksdb/boost
安装第三库前需要确认cmake版本是3.7.2以上，make版本是3.82以上。
### 1. 安装 cmake
```shell
wget https://cmake.org/files/v3.7/cmake-3.7.2.tar.gz
tar -zxvf cmake-3.7.2.tar.gz
cd cmake-3.7.2
./bootstrap && make && make install
/usr/local/bin/cmake --version
rm -rf /usr/bin/cmake
ln -s /usr/local/bin/cmake /usr/bin/
```
### 2. 安装 make
```shell
http://mirrors.ustc.edu.cn/gnu/make/
tar -zxvf make-3.82.tar.gz
./configure  --prefix=/usr/local/make
make && make install
rm -rf /usr/local/bin/make
ln -s /usr/local/make/bin/make /usr/local/bin/make
```
### 3. 安装 marisa 
```shell
git clone https://github.com/s-yata/marisa-trie.git
cd marisa-trie
autoreconf -i
./configure --enable-native-code --prefix=/usr/local/stonedb-marisa
make && make install 
```
### 4. 安装 rocksdb 
```shell
wget https://github.com/facebook/rocksdb/archive/refs/tags/v6.12.6.tar.gz
tar -zxvf v6.12.6.tar.gz
cd rocksdb-6.12.6
make shared_lib
make install-shared INSTALL_PATH=/usr/local/stonedb-gcc-rocksdb
make static_lib
make install-static INSTALL_PATH=/usr/local/stonedb-gcc-rocksdb
```
### 5. 安装 boost
```shell
wget https://sourceforge.net/projects/boost/files/boost/1.66.0/boost_1_66_0.tar.gz
tar -zxvf boost_1_66_0.tar.gz
cd boost_1_66_0
./bootstrap.sh --prefix=/usr/local/stonedb-boost
./b2 install --with=all
```
## 第四步：执行编译
通过执行以下脚本执行编译。
```shell
cd /stonedb/scripts
./stonedb_build.sh
```
编译完成后会自动生成目录/stonedb57。
## 第五步：启动实例
按照以下步骤启动StoneDB实例。
### 1. 创建用户
```shell
groupadd mysql
useradd -g mysql mysql
passwd mysql
```
### 2. 执行脚本reinstall.sh
```shell
cd /stonedb57/install
./reinstall.sh
```
执行脚本的过程就是初始化实例和启动实例。
### 3. 执行登录
登录前需要在/stonedb57/install/log/mysqld.log找到超级用户的密码。
```shell
more /stonedb57/install/log/mysqld.log |grep password
2022-07-12T11:41:59.849676Z 1 [Note] A temporary password is generated for root@localhost: %xjqgHux(6pr
```
```shell
/stonedb57/install/bin/mysql -uroot -p -S /stonedb57/install//tmp/mysql.sock
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 3
Server version: 5.7.36-StoneDB-debug-log

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

root@localhost [(none)]> show databases;
ERROR 1820 (HY000): You must reset your password using ALTER USER statement before executing this statement.
root@localhost [(none)]> alter user 'root'@'localhost' identified by 'xxx';
Query OK, 0 rows affected (0.00 sec)

root@localhost [(none)]> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| cache              |
| mysql              |
| performance_schema |
| sys                |
| sys_stonedb        |
+--------------------+
6 rows in set (0.00 sec)
```
