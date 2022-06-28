---
id: quick-deployment
sidebar_position: 3.1
---

# Quick Deployment

## Upload and decompress the TAR package
```
tar -zxvf stonedb-build_stonedb5.7_0.1_x86_64_CentOS7.9.2009_Release_2022-05-17_12_06.bin.tar.gz
```

Upload the installation package to the directory. The name of the folder extracted from the package is **stonedb**.

## Check dependencies
```
cd stonedb/install/bin
ldd mysqld
ldd mysql
```
If the command output contains keywords **not found**, some dependencies are missing and must be installed.

## Modify the configuration file

```
cd stonedb/install/
cp stonedb.cnf stonedb.cnf.bak
vi stonedb.cnf
```

Modify the path and parameters. If the installation folder is **stonedb**, you only need to modify the parameters.

## Create an account and directories
```
groupadd stonedb
useradd -g stonedb stonedb
passwd stonedb
cd stonedb/install/
mkdir binlog
mkdir log
mkdir tmp
mkdir redolog
mkdir undolog
chown -R stonedb:stonedb stonedb
```

## Initialize StoneDB

```
/stonedb/install/bin/mysqld --defaults-file=/stonedb/install/stonedb.cnf --initialize-insecure --user=stonedb
```

When you initialize StoneDB, add** parameter --initialize-insecure** to allow the admin to initially log in without the need to enter a password. The admin is required to set a password after the initial login.

## Start or stop StoneDB
```
/stonedb/install/bin/mysqld_safe --defaults-file=/stonedb/install/stonedb.cnf --user=stonedb &
mysqladmin -uroot -p -S /stonedb/install/tmp/mysql.sock shutdown
```
## Log in as admin and reset the password
```
mysql -uroot -p -S /stonedb/install/tmp/mysql.sock
>set password = password('MYPASSWORD');
```
Replace **MYPASSWORD** with your password.
