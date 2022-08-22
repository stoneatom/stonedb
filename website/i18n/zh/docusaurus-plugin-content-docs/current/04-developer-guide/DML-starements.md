---
id: DML-statements
sidebar_position: 5.3
---

# DML语句

StoneDB 只支持以下DML操作，其它没有说明的表示不支持。
```sql
CREATE TABLE t_test(
  id INT NOT NULL AUTO_INCREMENT,
  first_name VARCHAR(10) NOT NULL,
  last_name VARCHAR(10) NOT NULL,
  sex VARCHAR(5) NOT NULL,
  score INT NOT NULL,
  copy_id INT NOT NULL,
  PRIMARY KEY (`id`)
) engine=stonedb;
```
:::info
- StoneDB 5.6 的存储引擎名是 stonedb;
- StoneDB 5.7 的存储引擎名是 tianmu。
:::
# insert
```sql
insert into t_test values(1,'jack','rose','0',58,1);
```
# update
```sql
update t_test set score=200 where id=1;
```
# insert into select
```sql
create table t_test2 like t_test;
insert into t_test2 select * from t_test;
```
# insert into on duplicate key update
```sql
insert into t_test1 values(1,'Bond','Jason','1',47,10) on duplicate key update last_name='James';
```
:::tip
语义的逻辑是插入一行数据，如果碰到主键约束或者唯一约束冲突，就执行后面的更新语句。
:::
