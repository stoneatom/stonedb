---
id: create-and-manage-table
sidebar_position: 5.52
---

# 创建和管理表

创建表，例如：要创建一个名为student的表，包括编号、姓名、年龄、生日等字段，可以使用以下SQL语句：
```sql
create table student(
    id int(11) primary key,
    name varchar(10),
	  age smallint,
    birthday DATE
    ) engine=stonedb;
```
:::tip
StoneDB 5.6 的存储引擎名是 stonedb，5.7 的存储引擎名是 tianmu。
:::

查看表结构，可以使用以下SQL语句：
```sql
show create table student\G
```
删除表，例如：要删除表student，可以使用以下SQL语句：
```sql
drop table student;
```
