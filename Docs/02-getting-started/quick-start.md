---
id: quick-start
sidebar_position: 3.2
---

# Quick Start

This topic presents some examples to show you that the StoneDB storage engine has superior performance than InnoDB on processing bulk insert of data, compressing data, and executing analytical queries.

## Step 1. Deploy a test environment
Before using StoneDB, prepare your test environment according to instructions provided in [Quick Deployment](https://stoneatom.yuque.com/staff-ft8n1u/dghuxr/pv8ath) and start StoneDB.

## Step 2. Prepare test data
Perform the following steps to generate test data.

### 1. Prepare for the test
In the test environment, create a StoneDB table and a InnoDB table. Ensure the following parameter settings of the two tables are same:

- autocommit=1
- innodb_flush_log_at_trx_commit=1
- sync_binlog=0


### 2. Create a database

Create a database named **test**.

```
create database test DEFAULT CHARACTER SET utf8mb4;
```

### 3. Create a table

In database **test**, create a table named **t_test**.

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

### 4. Create a stored procedure

Create a stored procedure that is used to generate a table containing randomly generated names of persons.

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

## Step 3. Test insert performance

Call the stored procedure to insert 10,000,000 rows of data.
```
> call add_user_innodb(10000000);
Query OK, 1 row affected (24 min 46.62 sec)

> call add_user(10000000);
Query OK, 1 row affected (9 min 21.14 sec)
```
According to the returned result, StoneDB takes 9 minutes and 21 seconds, while InnoDB takes 24 minutes and 46 seconds.

## Step 4. Test data compression efficiency
Compress the inserted data.
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
According to the returned result, the data size after compression in StoneDB is 120 MB while that in InnoDB is 455 MB.

## Step 5. Test performance on processing aggregate queries

Execute an aggregate query.
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
According to the returned result, StoneDB takes 0.98 seconds while InnoDB takes 9 seconds.
