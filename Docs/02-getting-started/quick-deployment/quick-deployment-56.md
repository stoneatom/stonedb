---
id: quick-deployment-56
sidebar_position: 3.11
---

# Quick Deploy StoneDB-5.6
## 1. Download the latest package
Click [here](https://static.stoneatom.com/stonedb-ce-5.6-v1.0.0.el7.x86_64.tar.gz) to download the latest installation package of StoneDB. 

:::info
To simplify deployment, the installation package provided here is pre-compiled to include all required dependencies.
:::
## 2. Upload and decompress the TAR package
```shell
cd /
tar -zxvf stonedb-ce-5.6-v1.0.0.el7.x86_64.tar.gz
```
You can uload the installation package to the server. The name of the folder extracted from the package is **stonedb56**. In this topic, **/stonedb56** is used as the installation package.
## 3. Check dependencies
```bash
cd /stonedb56/install/bin
ldd mysqld
ldd mysql
```
If the command output contains keywords **not found**, some dependencies are missing and must be installed. <br />For example, `libsnappy.so.1 => not found` is returned:

- If your OS is Ubuntu, run the `sudo apt search libsnappy` command. The command output will inform you to install `libsnappy-dev`. For more details, see [Compile StoneDB on Ubuntu 20.04](../../04-developer-guide/00-compiling-methods/compile-using-ubuntu2004/compile-using-ubuntu2004-for-56.md).
- If your OS is RHEL or CentOS, run the `yum search all snappy` command. The command output will inform you to install **snappy-devel** and **snappy**. For more details, see [Compile StoneDB on CentOS 7](../../04-developer-guide/00-compiling-methods/compile-using-centos7/compile-using-centos7-for-56.md) or [Compile StoneDB on RHEL 7](../../04-developer-guide/00-compiling-methods/compile-using-redhat7/compile-using-redhat7-for-56.md).
## 4. Start StoneDB
Users can start StoneDB in two ways: manual installation and automatic installation. 
### 4.1 Create an account
```bash
groupadd mysql
useradd -g mysql mysql
passwd mysql
```
### 4.2 Manually install StoneDB
You need to manually create directories, configure the parameter file, and then initialize and start StoneDB.
```shell
### Create directories
mkdir -p /stonedb56/install/data/innodb
mkdir -p /stonedb56/install/binlog
mkdir -p /stonedb56/install/log
mkdir -p /stonedb56/install/tmp
chown -R mysql:mysql /stonedb56

### Configure parameters in stonedb.cnf
vim /stonedb56/install/stonedb.cnf
[mysqld]
port      = 3306
socket    = /stonedb56/install/tmp/mysql.sock
datadir   = /stonedb56/install/data
pid-file  = /stonedb56/install/data/mysqld.pid
log-error = /stonedb56/install/log/mysqld.log

chown -R mysql:mysql /stonedb56/install/stonedb.cnf

### Initialize StoneDB
/stonedb56/install/scripts/mysql_install_db --datadir=/stonedb56/install/data --basedir=/stonedb56/install --user=mysql

### Start StoneDB
/stonedb56/install/bin/mysqld_safe --defaults-file=/stonedb56/install/stonedb.cnf --user=mysql &
```
### 4.3 Automatically install StoneDB
The process of executing the **reinstall.sh** script is to initialize and start the StoneDB.
```bash
cd /stonedb56/install
./reinstall.sh
```
:::info
Differences between **reinstall.sh** and **install.sh**:

- **reinstall.sh** is the script for automatic installation. When the script is being executed, directories are created, and StoneDB is initialized and started. Therefore, do not execute the script unless for the initial startup of StoneDB. Otherwise, all directories will be deleted and StoneDB will be initialized again.
- **install.sh** is the script for manual installation. You can specify the installation directories based on your needs and then execute the script. Same as reinstall.sh, when the script is being executed, directories are created, and StoneDB is initialized and started. Therefore, do not execute the script unless for the initial startup. Otherwise, all directories will be deleted and StoneDB will be initialized again.
:::
## 5. Log in to StoneDB
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
## 6. Stop StoneDB
```shell
/stonedb56/install/bin/mysqladmin -uroot -p -S /stonedb56/install/tmp/mysql.sock shutdown
```