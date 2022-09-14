---
id: use-mysqldump-backup-and-restore
sidebar_position: 4.31
---

# 使用mysqldump备份恢复StoneDB

## mysqldump介绍
mysqldump 是 MySQL 逻辑备份工具，执行 mysqldump 后会生成一组SQL语句，可以通过这些语句来重现原始数据库定义的库表数据，它可以转储一个或者多个本地 MySQL 数据库或者远程可访问的数据库备份。mysqldump 的使用可以参考 MySQL 官方文档说明：[4.5.4 mysqldump — A Database Backup Program](https://dev.mysql.com/doc/refman/5.6/en/mysqldump.html)，或者参考以下 mysqldump 使用参数。
```shell
/stonedb56/install/bin/mysqldump --help
```
## 备份 StoneDB 注意事项
StoneDB 不支持 lock tables 操作，所以需要在备份时加上 --skip-opt 或者 --skip-add-locks 参数，去除备份文件中的 LOCK TABLES `xxxx` WRITE， 否则备份数据将无法导入到 StoneDB。
## 使用示例
### 备份
#### 创建备份库表和测试数据
```bash
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
#### 使用 mysqldump 备份指定库
```bash
/stonedb56/install/bin/mysqldump  -uroot -p***** -P3306 --skip-opt --master-data=2 --single-transaction --set-gtid-purged=off  --databases dumpdb > /tmp/dumpdb.sql
```
#### 使用 mysqldump 备份除系统库（mysql、performation_schema、information_schema）外其他库
```bash
/stonedb56/install/bin/mysql  -uroot -p****** -P3306 -e "show databases;" | grep -Ev "sys|performance_schema|information_schema|Database|test" | xargs /stonedb57/install/bin/mysqldump  -uroot -p****** -P3306 --master-data=1 --skip-opt --databases > /tmp/ig_sysdb.sql
```
### 恢复
#### 数据导入到StoneDB
```bash
/stonedb56/install/bin/mysql  -uroot -p****** -P3306 dumpdb < /tmp/dumpdb.sql
```