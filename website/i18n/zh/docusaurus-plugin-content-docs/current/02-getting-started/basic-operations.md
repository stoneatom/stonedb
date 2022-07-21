---
id: basic-operations
sidebar_position: 3.4
---

# 基本操作

SQL是结构化查询语言（Structured Query Language）的简称，是一种特殊的编程语言，也是一种数据库查询和程序设计语言，用于存取、查询、更新和管理关系型数据库管理系统。
由于StoneDB是高度兼容MySQL的，那么可以使用MySQL支持的客户端连接StoneDB，并且大多数情况下可以直接执行MySQL支持的SQL语法，本文将详细介绍StoneDB基本的SQL操作。
SQL语言按照不同的功能划分为以下的4个部分：

- **DDL**(Data Definition Language)：数据定义语言，用来定义数据库中的对象，如create、alter、drop。
- **DML**(Data Manipulation Language)：数据操作语言，用来操作表数据，如insert、delete、update。
- **DQL**(Data Query Language)：数据查询语言，用来查询对象，如select。
- **DCL**(Data Control Language)：数据控制语言，用来定义访问权限和安全级别，如grant、revoke。
## 1. 数据库相关操作
### 1）创建数据库
例如：要创建一个名为test_db的数据库，数据库的默认字符集是utf8mb4，可以使用以下SQL语句：
```sql
create database test_db DEFAULT CHARACTER SET utf8mb4;
```
### 2）查看数据库
使用以下SQL语句查看数据库
```sql
show databases;
```
### 3）应用数据库
使用以下SQL语句应用已创建的数据库
```sql
use test_db;
```
### 4）删除数据库
使用以下SQL语句删除数据库test_db
```sql
drop database test_db;
```
## 2. 表相关操作
### 1）创建列式存储表
例如：要创建一个引擎为StoneDB、名为student的表，包括编号、姓名、年龄、生日等字段，可以使用以下SQL语句：
```sql
create table student(
    id int(11) primary key,
    name varchar(255),
	age smallint,
    birthday DATE
    ) engine=stonedb;
```
注明：如果SQL语句中未指定“engine=stonedb”，则所创建的表的存储引擎由参数default_storage_engine决定。
详情参见设置参数-在参数文件中指定存储引擎类型
### 2）查看表
使用以下SQL语句查看表结构：
```sql
show create table student\G
```
### 3）删除表
例如：要删除表student，可以使用以下SQL语句：
```sql
drop table student;
```
## 3. 数据相关操作
### 1）插入数据
使用insert向表插入记录，例如：
```sql
insert into student values(1,'Jack',15,'20220506');
```
### 2）修改表数据
使用update修改表，例如：
```sql
update student set age=25 where id=1;
```
### 3）删除表数据
#### 清空表数据
使用 Truncate 可以清空表中全部数据，例如:
```sql
truncate table student ;
```
#### 删除表中指定数据
由于StoneDB是列式存储，不支持delete操作
## 4. 查询表
使用select查询表记录，例如：
1）查询 student 表中ID=1的学生的姓名和生日
```sql
select name,birthday from student where id=1;
```

2）查询将 student 表按照生日排序后的学生姓名和生日
```sql
select name,birthday from student order by birthday;
```
## 5. 用户相关操作
### 1）创建用户
例如：要创建用户tiger，密码为123456，可以使用以下SQL语句：
```sql
create user 'tiger'@'%' identified by '123456';
```
注：用户名和主机名（'username'@'host'）唯一表示一个用户，'tiger'@'%'和'tiger'@'localhost'是两个不同的用户。
### 2）向用户授权
例如：向用户tiger授予可查询数据库test_db所有的表，可以使用以下SQL语句：
```sql
grant select on test_db.* to 'tiger'@'%';
```
### 3）查询用户权限：
例如：查询名为 tiger 的用户所拥有的权限
```sql
show grants for 'tiger'@'%';
```
### 4）删除用户
例如：要删除用户tiger@%，可以使用以下SQL语句：
```sql
drop user 'tiger'@'%';
```
