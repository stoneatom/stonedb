---
id: use-mysqldump-backup-and-restore
sidebar_position: 4.31
---

# Use mysqldump to Back Up or Recover StoneDB

## **mysqldump introduction**
mysqldump is offered by MySQL to perform logical backups. During a backup, mysqldump produces a set of SQL statements that can be executed to reproduce the original database object definitions and table data. It dumps one or more MySQL databases for backup or transfer to another SQL server. For information about how to use mysqldump, see [mysqldump — A Database Backup Program](https://dev.mysql.com/doc/refman/5.6/en/mysqldump.html) or the following code example.
```shell
/stonedb56/install/bin/mysqldump --help
```
## Precautions for backing up StoneDB
StoneDB does not support `LOCK TABLES` operations. Therefore, when you back up StoneDB, you must add the `--skip-opt` or `--skip-add-locks` parameter to remove the `LOCK TABLES… WRITE` statement from the backup file. Otherwise, backup data cannot be imported to StoneDB.
## **Examples**
### Backup
#### **Create databases and tables for backup and prepare test data**
```shell
mysql> create database dumpdb;
Query OK, 1 row affected (0.00 sec)

mysql> show databases;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| cache              |
| dumpdb             |
| innodb             |
| mysql              |
| performance_schema |
| sys_stonedb        |
| test               |
+--------------------+
8 rows in set (0.00 sec)

mysql> use dumpdb
Database changed

mysql> create table dumptb(id int primary key,vname varchar(20))engine=StoneDB;
Query OK, 0 rows affected (0.00 sec)

mysql> insert into dumpdb.dumptb(id,vname) values(1,'zhangsan'),(2,'lisi'),(3,'wangwu');
Query OK, 3 rows affected (0.00 sec)
Records: 3  Duplicates: 0  Warnings: 0

mysql> select * from dumpdb.dumptb;
+----+----------+
| id | vname    |
+----+----------+
|  1 | zhangsan |
|  2 | lisi     |
|  3 | wangwu   |
+----+----------+
3 rows in set (0.01 sec)
```

#### **Use mysqldump to back up a given database**
```bash
/stonedb56/install/bin/mysqldump  -uroot -p***** -P3306 --skip-opt --master-data=2 --single-transaction --set-gtid-purged=off  --databases dumpdb > /tmp/dumpdb.sql
```

#### **Use mysqldump to back up all databases, except system databases mysql, performation_schema, and information_schema**
```bash
/stonedb56/install/bin/mysql  -uroot -p****** -P3306 -e "show databases;" | grep -Ev "sys|performance_schema|information_schema|Database|test" | xargs /stonedb56/install/bin/mysqldump  -uroot -p****** -P3306 --master-data=1 --skip-opt --databases > /tmp/ig_sysdb.sql
```
### **Recovery**
#### **Import data to StoneDB**
```bash
/stonedb56/install/bin/mysql  -uroot -p****** -P3306 dumpdb < /tmp/dumpdb.sql
```