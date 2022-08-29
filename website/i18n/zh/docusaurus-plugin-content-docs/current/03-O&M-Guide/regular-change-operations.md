---
id: regular-change-operations
sidebar_position: 4.1
---

# 常规变更
## 表结构变更
StoneDB 只支持以下表结构变更和数据变更操作，其它没有说明的表示不支持。
### 创建相似表
1）创建一张引擎为 stonedb 、名为 t_name 的表；
```sql
CREATE TABLE t_name(
  col1 INT NOT NULL AUTO_INCREMENT,
  col2 VARCHAR(10) NOT NULL,
  ......
  PRIMARY KEY (`col1`)
) engine=stonedb;
```
:::info
StoneDB 5.6 的存储引擎名是 stonedb，5.7 的存储引擎名是 tianmu。
:::
2）创建一张与 t_name 结构相同的表 t_other，使用 create table like 语句：
```sql
create table t_other like t_name;
```
### 清空表数据
使用 truncate table 语句可以实现保留表结构，仅清空表中的数据。
```sql
truncate table t_name;
```
### 删除表
```sql
drop table t_name;
```
### 添加字段
使用 alter table  ... add column 语句实现向指定表中添加字段，新增加的字段默认置于最后一个字段。
```sql
alter table t_name add column c_name varchar(10);
```
### 删除字段
使用 alter table  ... drop 语句实现删除表中指定字段。
```sql
alter table t_name drop c_name;
```
### 重命名表
使用 alter table ... rename to 语句实现对指定表的重命名。
```sql
alter table t_name rename to t_name_new;
```
## 用户权限变更
### 创建用户
```sql
create user 'u_name'@'hostname' identified by 'xxx';
```
### 给用户赋权
```sql
grant all on *.* to 'u_name'@'hostname';
grant select on db_name.* to 'u_name'@'hostname';
grant select(column_name) on db_name.t_name to 'u_name'@'hostname';
```
### 回收用户权限
```sql
revoke all privileges on *.* from 'u_name'@'hostname';
revoke select on db_name.* from 'u_name'@'hostname';
revoke select(column_name) on db_name.t_name from 'u_name'@'hostname';
```
### 删除用户
```sql
drop user 'u_name'@'hostname';
```