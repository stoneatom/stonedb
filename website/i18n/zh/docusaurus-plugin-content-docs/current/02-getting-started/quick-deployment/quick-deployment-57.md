---
id: quick-deployment-57
sidebar_position: 3.12
---

# 快速部署 StoneDB-5.7
为方便用户快速上手，安装包是已经编译好的，只需要检查自己的环境是否缺少依赖。
## 下载安装包
点击 [此处](https://static.stoneatom.com/custom/stonedb-ce-5.7-v1.0.3.el7.x86_64.tar.gz)下载最新的安装包。
## 上传tar包并解压
```shell
cd /
tar -zxvf stonedb-ce-5.7-v1.0.3.el7.x86_64.tar.gz
```
用户可根据安装规范将安装包上传至服务器，解压出来的目录是 stonedb57，示例中的安装路径是 /stonedb57。
# 检查依赖文件
```shell
cd /stonedb57/install/bin
ldd mysqld
ldd mysql
```
如果检查返回有关键字"not found"，说明缺少文件，需要安装对应的依赖包。

例如：

libsnappy.so.1 => not found

这个就说明需要在 Ubuntu 上使用命令 "sudo apt search libsnappy" 检查，说明需要安装 libsnappy-dev。

在 RedHat 或者 CentOS 上使用命令 "yum search all snappy" 检查，说明需要安装 snappy-devel、snappy。

- Ubuntu 需要安装的依赖包详见 [Ubuntu 20.04 下编译 StoneDB](../../04-developer-guide/00-compiling-methods/compile-using-ubuntu2004/compile-using-ubuntu2004-for-57.md)。

- CentOS 需要安装的依赖包详见 [CentOS 7 下编译 StoneDB](../../04-developer-guide/00-compiling-methods/compile-using-centos7/compile-using-centos7-for-57.md)。

- RedHat 需要安装的依赖包详见 [RedHat 7 下编译 StoneDB](../../04-developer-guide/00-compiling-methods/compile-using-redhat7/compile-using-redhat7-for-57.md)。
## 启动实例
用户可按照手动安装和自动安装两种方式启动 StoneDB。
### 1. 创建用户
```shell
groupadd mysql
useradd -g mysql mysql
passwd mysql
```
### 2. 手动安装
手动创建目录、配置参数文件、初始化和启动实例。
```shell
###创建目录
mkdir -p /stonedb57/install/data
mkdir -p /stonedb57/install/binlog
mkdir -p /stonedb57/install/log
mkdir -p /stonedb57/install/tmp
mkdir -p /stonedb57/install/redolog
mkdir -p /stonedb57/install/undolog
chown -R mysql:mysql /stonedb57

###配置my.cnf
mv my.cnf my.cnf.bak
vim /stonedb57/install/my.cnf
[mysqld]
port      = 3306
socket    = /stonedb57/install/tmp/mysql.sock
basedir   = /stonedb57/install
datadir   = /stonedb57/install/data
pid_file  = /stonedb57/install/data/mysqld.pid
log_error = /stonedb57/install/log/mysqld.log
innodb_log_group_home_dir   = /stonedb57/install/redolog/
innodb_undo_directory       = /stonedb57/install/undolog/

chown -R mysql:mysql /stonedb57/install/my.cnf

###初始化实例
/stonedb57/install/bin/mysqld --defaults-file=/stonedb57/install/my.cnf --initialize --user=mysql

###启动实例
/stonedb57/install/bin/mysqld_safe --defaults-file=/stonedb57/install/my.cnf --user=mysql &
```
### 3. 自动安装
执行 reinstall.sh 就是创建目录、初始化实例和启动实例的过程。
```shell
cd /stonedb57/install
./reinstall.sh
```
:::info
reinstall.sh 与 install.sh 的区别：

- reinstall.sh 是自动化安装脚本，执行脚本的过程是创建目录、初始化实例和启动实例的过程，只在第一次使用，其他任何时候使用都会删除整个目录，重新初始化数据库。
- install.sh 是手动安装提供的示例脚本，用户可根据自定义的安装目录修改路径，然后执行脚本，执行脚本的过程也是创建目录、初始化实例和启动实例。以上两个脚本都只能在第一次使用。
:::

### 4. 执行登录
```shell
cat /stonedb57/install/log/mysqld.log |grep password
[Note] A temporary password is generated for root@localhost: ceMuEuj6l4+!

/stonedb57/install/bin/mysql -uroot -p -S /stonedb57/install/tmp/mysql.sock
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 2
Server version: 5.7.36-StoneDB-debug-log build-

Copyright (c) 2021, 2022 StoneAtom Group Holding Limited
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> alter user 'root'@'localhost' identified by 'stonedb123';
```
## 关闭实例
```shell
/stonedb57/install/bin/mysqladmin -uroot -p -S /stonedb57/install/tmp/mysql.sock shutdown
```
