---
id: create-and-manage-stored-procedure
sidebar_position: 5.54
---

# 创建和管理存储过程

创建一个存储过程，用于随机插入100万条数据。

创建表
```sql
CREATE TABLE t_user(
  id INT NOT NULL AUTO_INCREMENT,
  first_name VARCHAR(10) NOT NULL,
  last_name VARCHAR(10) NOT NULL,
  sex VARCHAR(5) NOT NULL,
  score INT NOT NULL,
  copy_id INT NOT NULL,
  PRIMARY KEY (`id`)
) engine=stonedb;
```
:::tip
StoneDB 5.6 的存储引擎名是 stonedb，5.7 的存储引擎名是 tianmu。
:::

创建存储过程
```sql
DELIMITER //
create PROCEDURE add_user(in num INT)
BEGIN
DECLARE rowid INT DEFAULT 0;
DECLARE firstname VARCHAR(10);
DECLARE name1 VARCHAR(10);
DECLARE name2 VARCHAR(10);
DECLARE lastname VARCHAR(10) DEFAULT '';
DECLARE sex CHAR(1);
DECLARE score CHAR(2);
WHILE rowid < num DO
SET firstname = SUBSTRING(md5(rand()),1,4); 
SET name1 = SUBSTRING(md5(rand()),1,4); 
SET name2 = SUBSTRING(md5(rand()),1,4); 
SET sex=FLOOR(0 + (RAND() * 2));
SET score= FLOOR(40 + (RAND() *60));
SET rowid = rowid + 1;
IF ROUND(RAND())=0 THEN 
SET lastname =name1;
END IF;
IF ROUND(RAND())=1 THEN
SET lastname = CONCAT(name1,name2);
END IF;
insert INTO t_user(first_name,last_name,sex,score,copy_id) VALUES (firstname,lastname,sex,score,rowid);  
END WHILE;
END //
DELIMITER ;
```
调用存储过程
```sql
call add_user(1000000);
```
删除存储过程
```sql
drop PROCEDURE add_user;
```