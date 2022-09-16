---
id: create-and-manage-stored-procedure
sidebar_position: 5.54
---

# Create and Manage a Stored Procedureiew

Create a stored procedure. For example, perform the following two steps to create a stored procedure named **add_user**, used to insert 1,000,000 random data records.

1. Execute the following SQL statement to create a table:
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
:::info
The column-based storage engine is named after StoneDB in StoneDB-5.6, and is renamed to Tianmu in StoneDB-5.7 to distinguish from the database StoneDB.
:::

2. Execute the following SQL statement to create the stored procedure:
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
Call a stored procedure. For example, execute the following SQL statement to call stored procedure **add_user**:
```sql
call add_user(1000000);
```
Drop a stored procedure. For example, execute the following SQL statement to drop stored procedure **add_user**:
```sql
drop PROCEDURE add_user;
```
