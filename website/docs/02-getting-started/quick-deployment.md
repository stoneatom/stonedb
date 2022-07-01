---
id: quick-deployment
sidebar_position: 3.1
---

# Quick Deployment

## Download the latest package
Click [here](../download.md) to download the latest installation package of StoneDB.

## Upload and decompress the TAR package

```bash
tar -zxvf stonedb-ce-5.6-v1.0.0.el7.x86_64.tar.gz
```

Upload the installation package to the directory. The name of the folder extracted from the package is **stonedb**.

## Check dependencies

```bash
cd stonedb/install/bin
ldd mysqld
ldd mysql
```
If the command output contains keywords **not found**, some dependencies are missing and must be installed.

## Modify the configuration file

```bash
cd stonedb/install/
cp stonedb.cnf stonedb.cnf.bak
vi stonedb.cnf
```

Modify the path and parameters. If the installation folder is **stonedb**, you only need to modify the parameters.

## Create an account and directories

```bash
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

```bash
/stonedb/install/bin/mysqld --defaults-file=/stonedb/install/stonedb.cnf --initialize-insecure --user=stonedb
```

When you initialize StoneDB, add** parameter --initialize-insecure** to allow the admin to initially log in without the need to enter a password. The admin is required to set a password after the initial login.

## Start or stop StoneDB

```bash
/stonedb/install/bin/mysqld_safe --defaults-file=/stonedb/install/stonedb.cnf --user=stonedb &
mysqladmin -uroot -p -S /stonedb/install/tmp/mysql.sock shutdown
```
## Log in as admin and reset the password

```bash
mysql -uroot -p -S /stonedb/install/tmp/mysql.sock
>set password = password('MYPASSWORD');
```
Replace **MYPASSWORD** with your password.
