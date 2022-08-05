---
id: quick-deployment
sidebar_position: 3.1
---

# Quick Deployment

## **1. Download the latest package**
Click [here](https://static.stoneatom.com/stonedb-ce-5.6-v1.0.0.el7.x86_64.tar.gz) to download the latest installation package of StoneDB.
## **2. Upload and decompress the TAR package**
```shell
tar -zxvf stonedb-ce-5.6-v1.0.0.el7.x86_64.tar.gz
```
Upload the installation package to the directory. The name of the folder extracted from the package is **stonedb56**.
## **3. Check dependencies**
```bash
cd /stonedb56/install/bin
ldd mysqld
ldd mysql
```
If the command output contains keywords **not found**, some dependencies are missing and must be installed. <br />For example, `libsnappy.so.1 => not found` is returned:

- If your OS is Ubuntu, run the `sudo apt search libsnappy` command. The command output will inform you to install `libsnappy-dev`. 
- If your OS is RHEL or CentOS, run the `yum search all snappy` command. The command output will inform you to install **snappy-devel** and **snappy**.
## **4. Modify the configuration file**
```bash
cd /stonedb56/install/
cp stonedb.cnf stonedb.cnf.bak
vi stonedb.cnf
```
Modify the path and parameters. If the installation folder is **stonedb**, you only need to modify the parameters.
## **5. Create an account**
```bash
groupadd mysql
useradd -g mysql mysql
passwd mysql
```
## **6. Execute reinstall.sh**
```bash
cd /stonedb56/install
./reinstall.sh
```
The process of executing the script is to initialize and start the StoneDB.
## **7. Log in to StoneDB**
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
## **8. Stop StoneDB**
```shell
/stonedb56/install/bin/mysqladmin -uroot -p -S /stonedb56/install/tmp/mysql.sock shutdown
```
