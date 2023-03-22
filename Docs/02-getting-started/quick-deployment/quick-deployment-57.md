---
id: quick-deployment-57
sidebar_position: 3.12
---

# Quick Deploy StoneDB-5.7
## 1. Download the installation package
Click [here](https://static.stoneatom.com/custom/stonedb-ce-5.7-v1.0.3.el7.x86_64.tar.gz) to download the latest installation package of StoneDB. 
:::info
To simplify deployment, the installation package provided here is pre-compiled to include all required dependencies.
:::
## 2. Upload and decompress the TAR package
```shell
cd /
tar -zxvf stonedb-ce-5.7-v1.0.3.el7.x86_64.tar.gz
```
You can upload the installation package to the server. The name of the folder extracted from the package is **stonedb57**. In this topic, **/stonedb57** is used as the installation package.
## 3. Check dependencies
```shell
cd /stonedb57/install/bin
ldd mysqld
ldd mysql
```
If the command output contains keywords **not found**, some dependencies are missing and must be installed. <br />For example, `libsnappy.so.1 => not found` is returned:

- If your OS is Ubuntu, run the `sudo apt search libsnappy` command. The command output will inform you to install `libsnappy-dev`. For more details, see [Compile StoneDB on Ubuntu 20.04](../../04-developer-guide/00-compiling-methods/compile-using-ubuntu2004/compile-using-ubuntu2004-for-57.md).
- If your OS is RHEL or CentOS, run the `yum search all snappy` command. The command output will inform you to install **snappy-devel** and **snappy**. For more details, see [Compile StoneDB on CentOS 7](../../04-developer-guide/00-compiling-methods/compile-using-centos7/compile-using-centos7-for-57.md) or [Compile StoneDB on RHEL 7](../../04-developer-guide/00-compiling-methods/compile-using-redhat7/compile-using-redhat7-for-57.md).
## 4. Start StoneDB
Users can start StoneDB in two ways: manual installation and automatic installation. 
### 4.1 Create an account
```shell
groupadd mysql
useradd -g mysql mysql
passwd mysql
```
### 4.2 Manually install StoneDB
You need to manually create directories, configure the parameter file, and then initialize and start StoneDB. 
```shell
### Create directories.
mkdir -p /stonedb57/install/data
mkdir -p /stonedb57/install/binlog
mkdir -p /stonedb57/install/log
mkdir -p /stonedb57/install/tmp
mkdir -p /stonedb57/install/redolog
mkdir -p /stonedb57/install/undolog
chown -R mysql:mysql /stonedb57

### Configure my.cnf.
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

### Initialize StoneDB.
/stonedb57/install/bin/mysqld --defaults-file=/stonedb57/install/my.cnf --initialize --user=mysql

### Start StoneDB.
/stonedb57/install/bin/mysqld_safe --defaults-file=/stonedb57/install/my.cnf --user=mysql &
```
### 4.3 Automatically install StoneDB
The process of executing the **reinstall.sh** script is to initialize and start the StoneDB.
```shell
cd /stonedb57/install
./reinstall.sh
```
:::info
Differences between **reinstall.sh** and **install.sh**:

- **reinstall.sh** is the script for automatic installation. When the script is being executed, directories are created, and StoneDB is initialized and started. Therefore, do not execute the script unless for the initial startup of StoneDB. Otherwise, all directories will be deleted and StoneDB will be initialized again.
- **install.sh** is the script for manual installation. You can specify the installation directories based on your needs and then execute the script. Same as reinstall.sh, when the script is being executed, directories are created, and StoneDB is initialized and started. Therefore, do not execute the script unless for the initial startup. Otherwise, all directories will be deleted and StoneDB will be initialized again.
:::

### 5. Log in to StoneDB
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
# Allow root user to log in remotely
mysql> GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'stonedb123' WITH GRANT OPTION;
mysql> FLUSH PRIVILEGES;
```
## 6. Stop StoneDB
```shell
/stonedb57/install/bin/mysqladmin -uroot -p -S /stonedb57/install/tmp/mysql.sock shutdown
```