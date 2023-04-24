---
id: basic-operations
sidebar_position: 3.4
---

# 基本操作

SQL是结构化查询语言（Structured Query Language）的简称，是一种特殊的编程语言，也是一种数据库查询和程序设计语言，用于存取、查询、更新和管理关系型数据库管理系统。

由于 StoneDB 是100%兼容 MySQL 的，那么可以使用 MySQL 支持的客户端连接 StoneDB，并且大多数情况下可以直接执行 MySQL 支持的 SQL 语法，本文将详细介绍 StoneDB 基本的 SQL 操作。

SQL语言按照不同的功能划分为以下的4个部分：

- **DDL**(Data Definition Language)：数据定义语言，用来定义数据库中的对象，如 create、alter、drop。
- **DML**(Data Manipulation Language)：数据操作语言，用来操作表数据，如 insert、delete、update。
- **DQL**(Data Query Language)：数据查询语言，用来查询对象，如 select。
- **DCL**(Data Control Language)：数据控制语言，用来定义访问权限和安全级别，如 grant、revoke。
## 1. 数据库相关操作
### 1）创建数据库
要创建一个名为 test_db 的数据库，数据库的默认字符集是 utf8mb4 ，可以使用以下 SQL 语句：
```sql
create database test_db DEFAULT CHARACTER SET utf8mb4;
```
### 2）查看数据库
查看数据库可以使用以下 SQL 语句：
```sql
show databases;
```
### 3）应用数据库
应用已创建的数据库可以使用以下 SQL 语句：
```sql
use test_db;
```
### 4）删除数据库
删除数据库 test_db 可以使用以下 SQL 语句：
```sql
drop database test_db;
```
## 2. 表相关操作
### 1）创建表
要创建一个表名为 student 的表，包括编号、姓名、年龄、生日等字段，可以使用以下SQL语句：
```sql
create table student(
id int(11) primary key,
name varchar(20),
age smallint,
birthday DATE
) engine=stonedb;
```
:::info

- StoneDB 5.6 的存储引擎名是 stonedb，5.7 的存储引擎名是 tianmu。
- 如果 SQL 语句中未指定“engine=stonedb”，则所创建的表的存储引擎由参数 default_storage_engine 决定，详情参见[设置参数](https://stonedb.io/zh/docs/developer-guide/appendix/configuration-parameters)。
:::

### 2）查看表
查看表结构使用以下 SQL 语句：
```sql
show create table student;
```
### 3）删除表
要删除表 student，可以使用以下 SQL 语句：
```sql
drop table student;
```
## 3. 数据相关操作
### 1）插入数据
使用 insert 向表插入记录：
```sql
insert into student values(1,'Jack',15,'20220506');
```
### 2）修改数据
使用 update 修改记录：
```sql
update student set age=25 where id=1;
```
### 3）删除数据
使用 delete 删除记录：
```sql
delete from student where id=1;
```
## 4. 查询表
1）查询 student 表中 ID=1 的学生的姓名和生日
```sql
select name,birthday from student where id=1;
```

2）查询 student 表的学生姓名和生日，并且按照生日排序
```sql
select name,birthday from student order by birthday;
```
## 5. 用户相关操作
### 1）创建用户
例如：要创建用户 tiger，密码为123456，可以使用以下 SQL 语句：
```sql
create user 'tiger'@'%' identified by '123456';
```
注：用户名和主机名（'username'@'host'）唯一表示一个用户，'tiger'@'%'和'tiger'@'localhost'是两个不同的用户。
### 2）向用户授权
例如：向用户 tiger 授予可查询数据库 test_db 所有的表，可以使用以下 SQL 语句：
```sql
grant select on test_db.* to 'tiger'@'%';
```
### 3）查询用户权限
例如：查询名为 tiger 的用户所拥有的权限
```sql
show grants for 'tiger'@'%';
```
### 4）删除用户
例如：要删除用户 tiger@%，可以使用以下 SQL 语句：
```sql
drop user 'tiger'@'%';
```