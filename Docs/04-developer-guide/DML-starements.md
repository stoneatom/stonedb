---
id: DML-statements
sidebar_position: 5.3
---

# DML Statements

This topic describes the DML statements supported by StoneDB.<br />In this topic, table **t_test** created by executing the following statement is used in all code examples. 
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
The column-based storage engine is named after StoneDB in StoneDB-5.6, and is renamed to Tianmu in StoneDB-5.7 to distinguish from the database StoneDB.
:::
## INSERT
```sql
insert into t_test values(1,'jack','rose','0',58,1);
```
## UPDATE
```sql
update t_test set score=200 where id=1;
```
## INSERT INTO ... SELECT
```sql
create table t_test2 like t_test;
insert into t_test2 select * from t_test;
```
## INSERT INTO ... ON DUPLICATE KEY UPDATE
```sql
insert into t_test1 values(1,'Bond','Jason','1',47,10) on duplicate key update last_name='James';
```
:::info
The logic of this syntax is to insert a row of data. The UPDATE statement is executed only if a primary key constraint or unique constraint conflict occurs.
:::