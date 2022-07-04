---
id: quick-start
sidebar_position: 3.2
---

# 快速上手

本文的目的是通过简单的演示，让使用者体验StoneDB在大批量数据插入、数据压缩比和分析查询方面与InnoDB相比具有较高的性能。
## 第一步. 部署试用环境
在试用 StoneDB 功能前，请按照[快速部署](quick-deployment.md)中的步骤准备 StoneDB 测试环境并启动实例。
## 第二步. 准备测试数据
通过以下步骤，将生成一个测试数据集用于体验 StoneDB 功能。
### 1) 前提条件
在同一个测试环境，分别创建StoneDB和InnoDB存储引擎的表，并且以下三个参数也是相同的。
autocommit=1
innodb_flush_log_at_trx_commit=1
sync_binlog=0
### 2）创建数据库
创建名为test的数据库
```
create database test DEFAULT CHARACTER SET utf8mb4;
```
### 3) 创建表
在test数据库内创建名为t_user的表
```
use test
CREATE TABLE t_user(
  id INT NOT NULL AUTO_INCREMENT,
  first_name VARCHAR(20) NOT NULL,
  last_name VARCHAR(20) NOT NULL,
  sex VARCHAR(5) NOT NULL,
  score INT NOT NULL,
  copy_id INT NOT NULL,
  PRIMARY KEY (`id`)
) engine=STONEDB;
```
### 4) 创建存储过程
执行以下 SQL 语句创建存储过程
```
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
此存储过程用于随机生成一张人员信息表。
## 第三步. 插入性能测试
执行以下SQL语句调用存储过程
```
> call add_user_innodb(10000000);
Query OK, 1 row affected (24 min 46.62 sec)

> call add_user(10000000);
Query OK, 1 row affected (9 min 21.14 sec)
```
结果显示：插入1千万数据行，在StoneDB需要9分钟21秒，在InnoDB需要24分钟46秒。
## 第四步. 数据压缩测试
通过以下SQL语句验证数据压缩性能
```
> select count(*) from t_user_innodb;
+----------+
| count(*) |
+----------+
| 10000000 |
+----------+
1 row in set (1.83 sec)

> select count(*) from t_user;       
+----------+
| count(*) |
+----------+
| 10000000 |
+----------+
1 row in set (0.00 sec)

+--------------+---------------+------------+-------------+--------------+------------+---------+
| table_schema | table_name    | table_rows | data_length | index_length | total_size | engine  |
+--------------+---------------+------------+-------------+--------------+------------+---------+
| test         | t_user        |   10000000 | 119.99M     | 0.00M        | 119.99M    | STONEDB |
| test         | t_user_innodb |    9995867 | 454.91M     | 0.00M        | 454.91M    | InnoDB  |
+--------------+---------------+------------+-------------+--------------+------------+---------+
```
结果显示：相同的数据行，在StoneDB大小为120M，在InnoDB大小为455M。
## 第五步. 聚合查询测试
通过以下语句执行聚合查询测试
```
> select first_name,count(*) from t_user group by first_name order by 1; 
+------------+----------+
| first_name | count(*) |
+------------+----------+
.....
| fffb       |      142 |
| fffc       |      140 |
| fffd       |      156 |
| fffe       |      140 |
| ffff       |      132 |
+------------+----------+
65536 rows in set (0.98 sec)

> select first_name,count(*) from t_user_innodb group by first_name order by 1;
+------------+----------+
| first_name | count(*) |
+------------+----------+
......
| fffb       |      178 |
| fffc       |      161 |
| fffd       |      170 |
| fffe       |      156 |
| ffff       |      139 |
+------------+----------+
65536 rows in set (9.00 sec)
```
结果显示：执行相同的聚合查询，在StoneDB需要0.98s，在InnoDB需要9s。
