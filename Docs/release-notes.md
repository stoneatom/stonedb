---
id: release-notes
sidebar_position: 11.0
---

# Release Notes

## StoneDB-5.7-V1.0.2
Release date： January 15,2023
### New features

- Supports user-defined functions
- Supports  ESCAPE
- Supports Primary key and Supports syntactically index constraints
- Supports modifying the character set of table or field
- Supports BIT data type
   - Supports Creation, change and  deletion
   - Supports  logical operation
- Supports replace into statement
- Supports syntactically unsigned  and zerofill 
- Sql_mode add value 'MANDATORY_TIANMU' for mandatory Tianmu engine in table 
   - The following statement demonstrates the syntax：
```sql
# Global level
mysql>set global sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU';

# Session level
mysql>set session sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU';

#Set my.cnf parameter file 
[mysqld] 
sql_mode        =  'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU'
```

### Accessibility 

- Automatic detection  and identification  of installation package
- Rapid deployment of StoneDB as an analytical database for MySQL
### Stability

- Enhanced the stability as an analytical database
### Bug Fixes
The following bugs are fixed：

- The GROUP_CONCAT() function return result set error [#938](https://github.com/stoneatom/stonedb/issues/938)
- Bugs when use LIKE in WHERE [#1162](https://github.com/stoneatom/stonedb/issues/1162) [#1157](https://github.com/stoneatom/stonedb/issues/1157) [#763](https://github.com/stoneatom/stonedb/issues/763)
- Bugs when use  Primary key with AUTO_INCREMENT [#1144](https://github.com/stoneatom/stonedb/pull/1144) [#1142](https://github.com/stoneatom/stonedb/issues/1142)
- ALTER table..ADD numeric column, return error[#1140](https://github.com/stoneatom/stonedb/issues/1140)
- Clang-format execute failed in CI/CD[#973](https://github.com/stoneatom/stonedb/issues/973)
- INSERT INTO table error, but some data can be inserted successfully[#965](https://github.com/stoneatom/stonedb/issues/965)
- Query with UNION ALL return result set error [#854](https://github.com/stoneatom/stonedb/issues/854)
- The EXTRACT() function error [#845](https://github.com/stoneatom/stonedb/issues/845)
- Select...in...An error was reported when the date was included [#829](https://github.com/stoneatom/stonedb/issues/829)
- Update multiple values does not work with WHERE IN clause [#781](https://github.com/stoneatom/stonedb/issues/781)
- The syntax of the exists subquery in the TIANMU layer is compiled after the rule is incorrect, and the semantics is modified [#732](https://github.com/stoneatom/stonedb/issues/732)
- MTR binlog.binlog_unsafe Crash [#341](https://github.com/stoneatom/stonedb/issues/341)
- The other BUG [#682](https://github.com/stoneatom/stonedb/issues/682) [#553](https://github.com/stoneatom/stonedb/issues/553) [#508](https://github.com/stoneatom/stonedb/issues/508)
### Behavior Change
Using the Shell script for rapid deployment of  StoneDB as an analytical database for MySQL, parameter sql_mode default add  value of MANDATORY_TIANMU to enable the mandatory TIANMU engine

### Platforms

- CentOS 7.6 or obove
- Ubuntu 20

### Others

- Add more MTR test cases


## StoneDB-5.7-V1.0.1
Changes in StoneDB_5.7_v1.0.1 (2022-10-24, General Availability)
- Functionality Added or Changed
- Compilation Notes
- Document Notes
- Bugs Fixed


Functionality Added or Changed
- Tianmu:  From StoneDB_5.7 v1.0.1,  you can use delete statement to clear some data you don't need.
```sql
delete from table1;
delete from table1, table2, ...;
delete from table1 where ...;
delete from table1, table2, ... where ...;
```
- Tianmu:  From StoneDB_5.7 v1.0.1,  you can use alter table statement to modify the table structure as you need.
```sql
alter table tablename
```
- Tianmu:  Binlog replication supported ROW format；
```shell
binlog_format = ROW
```
- Tianmu: Added temporary table function；
```sql
CREATE TEMPORARY TABLE IF NOT EXISTS tablename
```
- Tianmu: From StoneDB_5.7 v1.0.1, you can create a trigger.  The trigger is a named database object that is associated with a table, and that activates when a particular event occurs for the table.
```sql
create trigger triggername
```
- Tianmu: Added  Create table AS... union...  statement；
- Tianmu: The Tianmu engine improved the performance of subqueries；
- Tianmu: Added gtest module；
- Tianmu: Added some mtr test cases；
Compilation Notes
- Added cmake parameter configuration for  build
```shell
cmake  .. -DWITH_MARISA  -DWITH_ROCKSDB
```
Document Notes
- The manual has been updated as the code was modified. ( [# address](https://stonedb.io/))
Bugs Fixed
- fix some inherited mtr from MySQL
- fix Tianmu bug: [#282](https://github.com/stoneatom/stonedb/issues/282),[#274](about:blank),[#270](https://github.com/stoneatom/stonedb/issues/270),[#663](https://github.com/stoneatom/stonedb/issues/663),[#669](https://github.com/stoneatom/stonedb/issues/669),[#670](https://github.com/stoneatom/stonedb/issues/670),[#675](https://github.com/stoneatom/stonedb/issues/675),[#678](https://github.com/stoneatom/stonedb/issues/678),[#682](https://github.com/stoneatom/stonedb/issues/682),[#487](https://github.com/stoneatom/stonedb/issues/487),[#426](https://github.com/stoneatom/stonedb/issues/426),[#250](https://github.com/stoneatom/stonedb/issues/250),[#247](https://github.com/stoneatom/stonedb/issues/247),[#569](https://github.com/stoneatom/stonedb/issues/569),[#566](https://github.com/stoneatom/stonedb/issues/566),[#290](https://github.com/stoneatom/stonedb/issues/290),[#736](https://github.com/stoneatom/stonedb/issues/736),[#567](https://github.com/stoneatom/stonedb/issues/567),[#500](https://github.com/stoneatom/stonedb/issues/500),[#300](https://github.com/stoneatom/stonedb/issues/300),[#289](https://github.com/stoneatom/stonedb/issues/289),[#566](https://github.com/stoneatom/stonedb/issues/566),[#279](https://github.com/stoneatom/stonedb/issues/279),[#570](https://github.com/stoneatom/stonedb/issues/570)[,#571](https://github.com/stoneatom/stonedb/issues/571),[#580](https://github.com/stoneatom/stonedb/issues/580),[#581](https://github.com/stoneatom/stonedb/issues/581),[#586](https://github.com/stoneatom/stonedb/issues/586),[#589](https://github.com/stoneatom/stonedb/issues/589),[#674](https://github.com/stoneatom/stonedb/issues/674),[#646](https://github.com/stoneatom/stonedb/issues/646),[#280](https://github.com/stoneatom/stonedb/issues/280),[#301](https://github.com/stoneatom/stonedb/issues/301),[#733](https://github.com/stoneatom/stonedb/issues/733) et. al.


## StoneDB-5.7-V1.0.0
Changes in StoneDB_5.7_v1.0.0 (2022-08-31, General Availability)

-  Support for MySQL 5.7
-  Functionality Added or Changed
- Compilation Notes
- Configuration Notes
- Document Notes
- Bugs Fixed

Support for MySQL 5.7

- Important Change: The Tianmu engine supports MySQL 5.7，base line edition MySQL 5.7.36. This change applies to the StoneDB database which would be fully compatible with the MySQL 5.7 protocols.

Functionality Added or Changed

- Important Change: The engine name  has been changed to tianmu. StoneDB is used as a HTAP database name, it is not suitable for used both as an engine name. As the dissusion result, we choose tianmu as our new eninge name: 

```sql
mysql> show engines;
+----------------+---------+--------------------------+--------------+------+------------+
| Engine         | Support | Comment                  | Transactions | XA   | Savepoints |
+----------------+---------+--------------------------+--------------+------+------------+
| TIANMU         | DEFAULT | Tianmu storage engine    | YES          | NO   | NO         |
+----------------+---------+--------------------------+--------------+------+------------+
```
- Tianmu:  Improved  the aggregation capabilities of the decimal data type. 
- Tianmu:  Improved the readability of code. The code does not spererate logically, or the variables name can not be understooed by the literal meaning. To refactor code make it more readable to anyone who are the first to read that. For example: Changes int DoGetSomething(); to int GetSomethingXXX();, int GetNoNulls() to int GetNumOfNulls().

- Tianmu: The date-related functions (such as DATE_ADD, DATE_SUB)  can be queried (DATE_ADD|DATE_SUB) when  creating view using the date func.([BUG #342](https://github.com/stoneatom/stonedb/issues/342))

Compilation Notes

- The version of the Boost library for server builds is now 1.66.0.

- The version of the Rocksdb for server builds is now 6.12.6.

Configuration Notes

- Important Change: Changed default config file stonedb.cnf to my.cnf. ([feature #182](https://github.com/stoneatom/stonedb/issues/182))

- Important Change: Use tianmu as the default storage engine. ([feature #255](https://github.com/stoneatom/stonedb/issues/255))

Document Notes

- The manual has been updated as the code was modified. ( [# address](https://stonedb.io/))

Bugs Fixed

- fix mtr case: [BUG #78](https://github.com/stoneatom/stonedb/issues/78), [BUG #73](https://github.com/stoneatom/stonedb/issues/73),[ BUG #170](https://github.com/stoneatom/stonedb/issues/170), [BUG #192](https://github.com/stoneatom/stonedb/issues/192), [BUG #191](https://github.com/stoneatom/stonedb/issues/191), [BUG #227](https://github.com/stoneatom/stonedb/issues/227),  [BUG #245](https://github.com/stoneatom/stonedb/issues/245), BUG  #263

- fix Tianmu bug: [BUG #338](https://github.com/stoneatom/stonedb/issues/388),[ BUG #327](https://github.com/stoneatom/stonedb/issues/327), [BUG #212](https://github.com/stoneatom/stonedb/issues/212), [BUG #142](https://github.com/stoneatom/stonedb/issues/142)

## StoneDB-5.6-V1.0.0
Release time: June 30, 2022

