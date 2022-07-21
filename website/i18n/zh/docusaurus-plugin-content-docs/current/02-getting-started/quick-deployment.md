---
id: quick-deployment
sidebar_position: 3.1
---

# 快速部署

为方便用户快速上手，安装包是已经编译好的，只需要检查自己的环境是否缺少依赖。

## 下载安装包
点击[此处](../download.md)下载最新的安装包。

## 上传tar包并解压
```shell
tar -zxvf stonedb-ce-5.6-v1.0.0.el7.x86_64.tar.gz
```
上传至安装目录，解压出来的文件夹名是stonedb56。
## 检查依赖文件
```shell
cd /stonedb56/install/bin
ldd mysqld
ldd mysql
```
如果检查返回有关键字"not found"，说明缺少文件，需要安装对应的依赖包。
## 修改配置文件
```shell
cd /stonedb56/install/
cp stonedb.cnf stonedb.cnf.bak
vi stonedb.cnf
```
主要修改路径，如果安装目录就是stonedb56，只需要修改其它参数。
## 创建用户
```shell
groupadd mysql
useradd -g mysql mysql
passwd mysql
```
## 执行脚本reinstall.sh
```shell
cd /stonedb56/install
./reinstall.sh
```
执行脚本的过程就是初始化实例和启动实例。
## 登录
```shell
/stonedb56/install/bin/mysql -uroot -p -S /stonedb56/install/tmp/mysql.sock 
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1
Server version: 5.6.24-StoneDB-log build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
No entry for terminal type "xterm";
using dumb terminal settings.
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| cache              |
| innodb             |
| mysql              |
| performance_schema |
| sys_stonedb        |
| test               |
+--------------------+
7 rows in set (0.00 sec)
```
## 关闭实例
```shell
/stonedb56/install/bin/mysqladmin -uroot -p -S /stonedb56/install/tmp/mysql.sock shutdown
```
