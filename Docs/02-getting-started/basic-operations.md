---
id: basic-operations
sidebar_position: 3.4
---

# Basic Operations
Structured Query Language (SQL) is a programming language for communicating with databases. You can use it to manage relational databases by performing insert, query, update, and other operations.

StoneDB is fully compatible with MySQL. You can use clients supported by MySQL to connect to StoneDB. In addition, StoneDB supports most SQL syntaxes. This section describes the basic SQL operations supported by StoneDB.

SQL can be classified into the following four parts by usage:

- Data Definition Language (DDL): is used to manage database objects, such as CREATE, ALTER, and DROP statements.
- Data Manipulation Language (DML): is used to manage data in tables, such as INSERT, DELETE, and UPDATE statements.
- Data Query Language (DQL): is used to query objects, such as SELECT statements.
- Data Control Language (DCL): is used to control access to data, such as GRANT and REVOKE statements.
# Operations on databases
This section provides examples of performing basic operations on databases.
## Create a database
Execute the following SQL statement to create a database named **test_db** and set the default character set to **utf8mb4**:
```sql
create database test_db DEFAULT CHARACTER SET utf8mb4;
```
## List databases
Execute the following SQL statement to list databases:
```sql
show databases;
```
## Use a database
Execute the following SQL statement to use database **test_db**:
```sql
use test_db;
```
## Drop a database
Execute the following SQL statement to drop database** test_db**:
```sql
drop database test_db;
```
# Operations on tables
This section provides examples of performing basic operations on tables.
## Create a StoneDB table
Execute the following SQL statement to create a table which is named **student** and consists of the **id**, **name**, **age**, and **birthday** fields:
```sql
create table student(
id int(11) primary key,
name varchar(20),
age smallint,
birthday DATE
) engine=stonedb;
```
:::info
The row-based storage engine is named StoneDB in StoneDB-5.6, and is renamed to Tianmu in StoneDB-5.7 to distinguish from the database StoneDB.<br />If you do not specify **engine=stonedb** in the SQL statement, the storage engine on which the table is created is determined by the value of parameter **default_storage_engine**. For more information, see [Configure parameters](https://stonedb.io/docs/developer-guide/appendix/configuration-parameters/.
:::
## Query the schema of a table
Execute the following SQL statement to query the schema of table **student**:
```sql
show create table student;
```
## Drop a table
Execute the following SQL statement to drop table **student**:
```sql
drop table student;
```
# Operations on data
This section provides examples of performing basic operations on data.
## Insert data into a table
Execute the following SQL statement to insert a record to table **student**:
```sql
insert into student values(1,'Jack',15,'20220506');
```
## Modify data in a table
Execute the following UPDATE statement to modify data in table **student**:
```sql
update student set age=25 where id=1;
```
## Remove data from a table
### Clear data in a table
Execute the following DELETE statement to remove data in table **student**:
```sql
delete from student where id=1;
```
## Query data from a table
Execute a SELECT statement to query data from a table.<br />Example 1: Query the name and birthday of the student whose **id** is **1** from table **student**.
```sql
select name,birthday from student where id=1;
```

Example 2: Query the name and birthday of each student and sort the result by birthday from table **student**.
```sql
select name,birthday from student order by birthday;
```
# Operations on users
This section provides examples of performing basic operations on users.
## Create a user
Execute the following SQL statement to create a user named **tiger** and set the password to **123456**:
```sql
create user 'tiger'@'%' identified by '123456';
```
:::info
The username together with the hostname uniquely identify a user in the format of '_username_'@'_host_'. In this way, 'tiger'@'%' and 'tiger'@'localhost' are two users.
:::
## Grant a user permissions
Execute the following SQL statement to grant user **tiger** the permissions to query all tables in database **test_db**:
```sql
grant select on test_db.* to 'tiger'@'%';
```
## Query user permissions
Execute the following SQL statement to query permissions granted to user **tiger**:
```sql
show grants for 'tiger'@'%';
```
## Drop a user
Execute the following SQL statement to drop user '**tiger'@'%'**:
```sql
drop user 'tiger'@'%';
```
