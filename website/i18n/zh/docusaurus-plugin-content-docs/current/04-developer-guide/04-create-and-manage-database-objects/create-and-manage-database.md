---
id: create-and-manage-database
sidebar_position: 5.51
---

# 创建和管理库

创建数据库，例如：要创建一个名为test_db的数据库，数据库的默认字符集是utf8mb4，可以使用以下SQL语句：
```sql
create database test_db DEFAULT CHARACTER SET utf8mb4;
```
查看数据库，可以使用以下SQL语句：
```sql
show databases;
```
使用数据库，可以使用以下SQL语句：
```sql
use test_db;
```
删除数据库，例如：要删除数据库test_db，可以使用以下SQL语句：
```sql
drop database test_db;
```
