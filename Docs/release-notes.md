---
id: release-notes
sidebar_position: 11.0
---

# Release Notes
## StoneDB-8.0-V2.1.0
Release date： October 31, 2023

### New Features
1. Support for transactions.
2. Support for master-slave replication.
3. Support for commonly used built-in functions.
4. Support for stored procedures, temporary tables, views, triggers, and other objects.
### Bug Fixes
1. The current thread cannot see the synchronized data and must log out and log in again to see the synchronized data.
2. Query returns the error message "Current transaction is aborted (please ROLLBACK)".
3. Pagination query result set error.
4. After killing a thread, the database cannot be closed.
5. After exiting the MDL metadata lock wait, querying the relevant table again returns an error and it is not possible to delete the table.
6. Querying another library's tables in the current library returns an error.
7. Subquery returns a single row of data, using 'in' returns the correct result set but '=' returns the incorrect result set.
### Stability Improvements
1. Handling of NULL values under master-slave replication.
2. Fixing crashes caused by ifnull and nullif.

## StoneDB-8.0-V2.0.0
Release date： September 25, 2023
### New architecture
1. StoneDB is the industry's first open-source, integrated MySQL real-time HTAP database with a row-columnar in-memory computing architecture.
2. The product is positioned against Oracle HeatWave. Users of MySQL do not need to migrate data. With StoneDB, they can achieve TP+AP mixed workloads, analyze performance improvements of over 100 times, and do not need to integrate with other AP solutions. It fills the gap in MySQL's analytical capabilities.
3. StoneDB is a new generation of enterprise-grade real-time HTAP database, 100% compatible with the MySQL protocol, capable of storing 100TB, supporting 10,000 concurrent operations, with 70% of the workload in TP scenarios and 30% in AP scenarios. By enhancing AP, it aims to achieve self-controlled TP capabilities and target the vast market of MySQL information technology upgrades and replacements.

### New features
1. StoneDB 2.0 provides real-time online transaction support and data analysis capabilities. While supporting TP transactions, it also supports a built-in AP engine that is transparent to users, providing high performance in billion-scale data join scenarios, with a speedup of 100 to 1000 times compared to MySQL.
2. It supports DDL statements such as create/alter table/drop table.
3. It also supports commands such as insert/update/load data to update data in real-time.

## StoneDB-5.7-V1.0.4-alpha
Release date： June 30th,2023

### Stability: 
1. Fixed a crash caused by incremental data when importing data #1805
2. Fixed a crash caused by the result set of the union all clause, #1875 
3. Fixed a crash caused by using aggregate functions in large data scenarios, #1855

### New features 
#### 2.1. Support for update ignore syntax feature. 

When updating tianmu, records with primary key conflicts will be skipped and subsequent update operations will be performed.
For example: 
```sql
CREATE TABLE t1  (id int(11) NOT NULL auto_increment,  parent_id int(11) DEFAULT '0' NOT NULL,  level tinyint(4)
DEFAULT '0' NOT NULL, PRIMARY KEY (id)) engine=tianmu;
INSERT INTO t1 VALUES (3,1,1),(4,1,1);
```

Executing the update ignore t1 set id=id+1; statement will ignore the update of PK=3, because the updated primary key will conflict with PK=4. Then continue to execute the update of pk=4, and the updated PK=5.

```sql
mysql>  CREATE TABLE t1  (id int(11) NOT NULL auto_increment,  parent_id int(11) DEFAULT '0' NOT NULL,  level tinyint(4)
         -> DEFAULT '0' NOT NULL, PRIMARY KEY (id)) engine=tianmu;
Query OK, 0 rows affected (0.01 sec)

mysql>   INSERT INTO t1 VALUES (3,1,1),(4,1,1);
Query OK, 2 rows affected (0.01 sec)
Records: 2  Duplicates: 0  Warnings: 0

mysql> update t1 set id=id+1;
ERROR 1062 (23000): Duplicate entry '4' for key 'PRIMARY'
mysql> select * from t1; 
+----+-----------+-------+
| id | parent_id | level |
+----+-----------+-------+
|  3 |         1 |     1 |
|  4 |         1 |     1 |
+----+-----------+-------+
2 rows in set (0.00 sec)

mysql> update ignore t1 set id=id+1;
Query OK, 2 rows affected (0.00 sec)
Rows matched: 2  Changed: 2  Warnings: 0

mysql> select * from t1; 
+----+-----------+-------+
| id | parent_id | level |
+----+-----------+-------+
|  3 |         1 |     1 |
|  5 |         1 |     1 |
+----+-----------+-------+
2 rows in set (0.00 sec)
```

#### 2.2 Support row format for “load data” statement.

When stonedb is used as the primary database, the load statement will be executed on the backup database in the form of “insert into”.

#### 2.3 Support AggregatorGroupConcat function

```sql
mysql> select GROUP_CONCAT(t.id) from sequence t;
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| GROUP_CONCAT(t.id)                                                                                                                                                                         |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 3000000000010000,3000000000010001,3000000000010002,3000000000010003,3000000000010004,3000000000010005,3000000000010006,3000000000010007,3000000000010008,3000000000010009,3000000000010010 |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

#### 2.4 Support using select 111 or select 111 from dual scenarios in uion/union all

```sql
mysql>  select id from tt union all select 2222 c1 from dual;
+------+
| id   |
+------+
| 1111 |
| 2222 |
+------+

mysql>  select id from tt union all select 2222 ;
+------+
| id   |
+------+
| 1111 |
| 2222 |
+------+
-- PS：select 111(from dual) appears in a position other than that of the first clause
```

### Others Bug fixs
1. Fix the default value problem of adding columns in the master and slave scenarios. #1187 
2. Fix the incorrect result set problem when using case…when in derived table. #1784 
3. Fix the incorrect result problem caused by precision loss when performing bit operations when the type is time. #1173 
4. Fix the incorrect query result problem caused by overflow when the type is bigint. #1564 
5. Fix the incorrect result problem when the filter condition is hexadecimal. #1625 
6. Fix the problem that the default value on the table did not take effect when importing data using the “load data” command. #1865 
7. Modify the default field separator of the load command to make it consistent with mysql behavior. #1609
8. Fix the query error caused by abnormal metadata. #1822 
9. Improve MTR stability on github.


### What's Changed
* docs(downlaod)：update the docs of download(#1453) by @Nliver in https://github.com/stoneatom/stonedb/pull/1454
* fix(mtr):Resolve the nightly run error problem(#1458) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1459
* fix(workflow): fix can not run lcov in workflow by @RingsC in https://github.com/stoneatom/stonedb/pull/1396
* feat(tianmu):New configuration parameters: "tianmu_mandatory" and "tianmu_no_key_error (#1462)" by @konghaiya in https://github.com/stoneatom/stonedb/pull/1466
* fix(tianmu):(Primary/Secondary)Error 1032 occasionally occurs during … by @konghaiya in https://github.com/stoneatom/stonedb/pull/1469
* test(mtr): add integer/unsigned/in subquery/create temporary testcases and update escape.test(#1196) by @davidshiz in https://github.com/stoneatom/stonedb/pull/1471
* fix(stonedb): Corrected errors in the documentation and updated the script of the official website(#1494) by @Nliver in https://github.com/stoneatom/stonedb/pull/1500
* fix(website): fix Baidu statistics script(#1502) by @Nliver in https://github.com/stoneatom/stonedb/pull/1503
* test(mtr): Optimize parallel scheduling to execute more innodb engine testcases, add date type and std func testcase(#1196) by @davidshiz in https://github.com/stoneatom/stonedb/pull/1504
* fix(tianmu): fix crash when the aggregated element was decimal (#1402) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1506
* build(deps): bump nth-check and unist-util-select in /website by @dependabot in https://github.com/stoneatom/stonedb/pull/741
* docs(stonedb): update the docs by @Nliver in https://github.com/stoneatom/stonedb/pull/1509
* fix(workflow): remove dup lines in git workflow(#1517) by @zzzz-vincent in https://github.com/stoneatom/stonedb/pull/1531
* feat(tianmu): mv func from public to protected(#1501) by @zzzz-vincent in https://github.com/stoneatom/stonedb/pull/1527
* fix(tianmu):Resolve DDL and insert inot select Possible space inflation issues(#366)  by @konghaiya in https://github.com/stoneatom/stonedb/pull/1497
* fix(tianmu):The myloader cann't work if the autocommit is closed. (#1510) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1524
* fix(core): fix bug: The instance occasionally crashes if both fields … by @wisehead in https://github.com/stoneatom/stonedb/pull/1528
* fix: page hover font style by @Agility6 in https://github.com/stoneatom/stonedb/pull/1535
* feat(mtr): To fix the mtr usage by @RingsC in https://github.com/stoneatom/stonedb/pull/1537
* fix(tianmu):The mysqld is crashed when you are starting replication.(#1523) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1540
* fix(optmizer):fix the crashes when the right table of hash join is in parallel(#1538) by @wisehead in https://github.com/stoneatom/stonedb/pull/1541
* feat(tianmu): To support volcano framwork by @RingsC in https://github.com/stoneatom/stonedb/pull/1543
* docs(quickstart): add stonedb-8.0 compiling guide #1449 by @Double0101 in https://github.com/stoneatom/stonedb/pull/1514
* fix(website): fix website error #1449 by @Double0101 in https://github.com/stoneatom/stonedb/pull/1548
* feat(tianmu): support volcano framewrok by @RingsC in https://github.com/stoneatom/stonedb/pull/1546
* fix(tianmu): revert code, mv ret value from try block back to catch block. (#1532) by @Dysprosium0626 in https://github.com/stoneatom/stonedb/pull/1555
* feat(tiamnu): hard code in defs.h (#1481) by @zzzz-vincent in https://github.com/stoneatom/stonedb/pull/1533
* docs:update the compile guides #1562 by @chenshengjiang in https://github.com/stoneatom/stonedb/pull/1565
* test(mtr): add more innodb testcases and tianmu range testcase(#1196) by @davidshiz in https://github.com/stoneatom/stonedb/pull/1569
* fix(tianmu):Remove excess log printing and add some code comments(#1545) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1547
* fix(tianmu): fix mysqld crash when exec query with AggregateRough, assert failed on i < m_idx.size() at tianmu_attr.h:387, msg: [bad dpn index 0/0] (#1580) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1582
* docs(webstie): update the latest website content by @Nliver in https://github.com/stoneatom/stonedb/pull/1589
* feat(tianmu): support volcano framework by @RingsC in https://github.com/stoneatom/stonedb/pull/1554
* fix: max-width navbar search style by @Agility6 in https://github.com/stoneatom/stonedb/pull/1600
* feat(website): Update site dependencies and fix some issues by @Nliver in https://github.com/stoneatom/stonedb/pull/1606
* fix(tianmu): The instance occasionally crashes when the memory leak. (#1549) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1598
* fix(website): fix the wrong QR code(#1624) by @Nliver in https://github.com/stoneatom/stonedb/pull/1634
* fix(tianmu): assert failed on oldv `<=` dpn_->max_i at pack_int.cpp:337 (#1610) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1627
* fix(tianmu): fix mysqld crash when query where JOIN::propagate_dependencies… (#1628) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1639
* fix(tianmu): fix MySQL server has gone away when exec query (#1641 #1640) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1642
* fix(tianmu):Support insert ignore syntax (#1637) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1643
* fix(tianmu): fix query input variables wrong result (#1647) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1660
* fix(tianmu): fix result of the query using the subquery derived table is incorrect (#1662) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1688
* fix(tianmu): fix results of two queries using a derived table and a custom face change are incorrect (#1696) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1697
* feat(tianmu): Test cases that supplement custom variables (#1703) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1705
* fix(tianmu): fix mysqld crash when assigning return values using both custom variables and derived tables (#1707) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1744
* fix(tianmu): Insert ignore can insert duplicate values.(#1699) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1746
* fix(tianmu): fix error occurred in the union all query result (#1599) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1750
* feat(sql): code refactor for misleading-indentation(#1542) by @zzzz-vincent in https://github.com/stoneatom/stonedb/pull/1646
* docs(quickstart): add stonedb-8.0 compiling guide for CentOS 7.x by @xxq-xu in https://github.com/stoneatom/stonedb/pull/1756
* fix(tianmu):Even if a primary key is defined, duplicate data may be imported.(#1648) by @konghaiya in https://github.com/stoneatom/stonedb/pull/1766
* feat: add delete/drop into tianmu log stat by @duanjr in https://github.com/stoneatom/stonedb/pull/1747
* fix(tianmu): fix Error result set of the IN subquery with semi join (#1764) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1770
* doc(develop-guide): modify method for compile stonedb using docker by @Dysprosium0626 in https://github.com/stoneatom/stonedb/pull/1768
* docs: add docker compile guide of stonedb8.0.(#1780) by @chenshengjiang in https://github.com/stoneatom/stonedb/pull/1783
* feat(tianmu): remove DBUG_OFF and repalce DEBUG_ASSERT with assert by @duanjr in https://github.com/stoneatom/stonedb/pull/1751
* fix(tianmu): fix storage of DT type by @duanjr in https://github.com/stoneatom/stonedb/pull/1752
* fix(tinmu): fix tianmu crash when set varchar to num when order by by @adofsauron in https://github.com/stoneatom/stonedb/pull/1807
* docs(website): update the documentation for Compile StoneDB 8.0 in Docker(#1823) by @davidshiz in https://github.com/stoneatom/stonedb/pull/1825
* fix(tianmu): fix up the incompatible type by @RingsC in https://github.com/stoneatom/stonedb/pull/1824
* fix(tianmu): Fix up the unknown exception after instance killed randomly by @RingsC in https://github.com/stoneatom/stonedb/pull/1841
* fix(tianmu): fix up the incorrect meta-info leads unexpected behavior by @RingsC in https://github.com/stoneatom/stonedb/pull/1840
* fix(workflow): nightly build failed #1830 by @hustjieke in https://github.com/stoneatom/stonedb/pull/1836
* feat(tianmu): revert assert() --> debug_assert() #1551 by @hustjieke in https://github.com/stoneatom/stonedb/pull/1842
* feat(tianmu): fixup the default delimeter for load data by @RingsC in https://github.com/stoneatom/stonedb/pull/1843
* fix(tianmu): revert PR #1841. by @RingsC in https://github.com/stoneatom/stonedb/pull/1850
* fix(tianmu): fix up mtr test case for delim of load data command by @RingsC in https://github.com/stoneatom/stonedb/pull/1854
* fix(tianmu): fix up the `group_concat` function in tianmu by @RingsC in https://github.com/stoneatom/stonedb/pull/1852
* fix(tianmu): To fixup the instance crashed if the result of aggregate is out of boundary by @RingsC in https://github.com/stoneatom/stonedb/pull/1856
* docs(developer-guide): update the compiling guide of StoneDB 8.0 for CentOS 7.x #1817 by @xxq-xu in https://github.com/stoneatom/stonedb/pull/1834
* fix(tianmu): Fixup the mem leakage of aggregation function by @RingsC in https://github.com/stoneatom/stonedb/pull/1867
* fix(tianmu): fix UNION of non-matching columns and express with aggregation (#1861 #1864 #1873 #1870) by @adofsauron in https://github.com/stoneatom/stonedb/pull/1881
* test(tianmu): add order by sentence in the mtr case various_join.test by @xxq-xu in https://github.com/stoneatom/stonedb/pull/1886
* test(mtr): add more test cases for tianmu(#1196) by @davidshiz in https://github.com/stoneatom/stonedb/pull/1888
* test(mtr): add order by sentence in the mtr case various_join.test by @xxq-xu in https://github.com/stoneatom/stonedb/pull/1891
* ci(codecov): update the config by @Nliver in https://github.com/stoneatom/stonedb/pull/1898
* fix(tianmu): To suuport ignore option for update statement by @RingsC in https://github.com/stoneatom/stonedb/pull/1884
* ci(codecov): update the codecov congfig by @Nliver in https://github.com/stoneatom/stonedb/pull/1899
* docs(intro): update the support for 8.0 by @Nliver in https://github.com/stoneatom/stonedb/pull/1901
* workflow(codecov): Filter out excess code files by @Nliver in https://github.com/stoneatom/stonedb/pull/1905
* workflow(coverage): Update the lcov running logic(#1908)(##1592) by @Nliver in https://github.com/stoneatom/stonedb/pull/1909
* fix(tianmu): default value of the field take unaffect in load #1865 by @Double0101 in https://github.com/stoneatom/stonedb/pull/1896
* fix(tianmu): To support union(all) the statement which is without from clause by @RingsC in https://github.com/stoneatom/stonedb/pull/1887
* fix(tianmu): To remove unnessary optimization in tianmu by @RingsC in https://github.com/stoneatom/stonedb/pull/1911
* fix(tianmu): hotfix corruption in ValueOrNull under multi-thread by @RingsC in https://github.com/stoneatom/stonedb/pull/1869
* fix(tianmu): incorrect result when using where expr and args > bigint_max #1564 by @Double0101 in https://github.com/stoneatom/stonedb/pull/1902
* fix(tianmu): add TIME_to_ulonglong_time_round process and fix up precision loss problem (#1173) by @xxq-xu in https://github.com/stoneatom/stonedb/pull/1895

### New Contributors
* @zzzz-vincent made their first contribution in https://github.com/stoneatom/stonedb/pull/1531
* @Agility6 made their first contribution in https://github.com/stoneatom/stonedb/pull/1535
* @Dysprosium0626 made their first contribution in https://github.com/stoneatom/stonedb/pull/1555
* @xxq-xu made their first contribution in https://github.com/stoneatom/stonedb/pull/1756
* @duanjr made their first contribution in https://github.com/stoneatom/stonedb/pull/1747

**Full Changelog**: https://github.com/stoneatom/stonedb/compare/5.7-v1.0.3-GA...5.7-v1.0.4-alpha


## StoneDB-5.7-V1.0.3
Release date： March 20th,2023

- Reconstructed the binlog mechanism to filter out DDL statements that are not supported by the Tianmu storage engine.
- Added an argument named **NO_KEY_ERROR** for SQL mode to directly skip DDL statements that are not supported by the SQL layer, instead of reporting errors.

Syntax:
```sql
# At global level:
mysql>set global sql_mode='NO_KEY_ERROR';

# At session level:
mysql>set session sql_mode='NO_KEY_ERROR';

# Configuration file my.cnf:
[mysqld] 
sql_mode =  'NO_KEY_ERROR'
```
### Ecosystem Adaptation
Better adapted to the ecosystem to display the version number of StoneDB.
### Perfomance
 Improved the primary/secondary synchronization performance. [#1213](https://github.com/stoneatom/stonedb/issues/1213)
### Bug Fixes
The following bugs are fixed:

- Incorrect result is returned when an `ALTER TABLE` statement is executed to add a TIMESTAMP field. [#](https://github.com/stoneatom/stonedb/issues/1327)[1327](https://github.com/stoneatom/stonedb/issues/1327)
- Incorrect result is returned for an UPDATE operation on a table after it is modified by an `ALTER TABLE` statement. [#](https://github.com/stoneatom/stonedb/issues/1253)[1253](https://github.com/stoneatom/stonedb/issues/1253)
- Incorrect result is returned for a query on BIGINT data that is unsigned. [#1203](https://github.com/stoneatom/stonedb/issues/1203)
- An error is reported when a statement is executed to load data. [#](https://github.com/stoneatom/stonedb/issues/1209)[1209](https://github.com/stoneatom/stonedb/issues/1209)
- The result returned for an AVG function call is incorrect. [#](https://github.com/stoneatom/stonedb/issues/1125)[1125](https://github.com/stoneatom/stonedb/issues/1125)
- An error is reported when an `ALTER TABLE` statement is executed to change the data type of a field. [#](https://github.com/stoneatom/stonedb/issues/752)[752](https://github.com/stoneatom/stonedb/issues/752)
- Other bugs. [#](https://github.com/stoneatom/stonedb/issues/103)[103](https://github.com/stoneatom/stonedb/issues/103)[#1230](https://github.com/stoneatom/stonedb/issues/1230)[#1255](https://github.com/stoneatom/stonedb/issues/1255)[#1188](https://github.com/stoneatom/stonedb/issues/1188)[#1262](https://github.com/stoneatom/stonedb/issues/1262)
### Supported OSs

- CentOS 7.6 and later
- Ubuntu 20

For more details on the update, please visit [Github](https://github.com/stoneatom/stonedb/releases) and [Gitee](https://gitee.com/StoneDB/stonedb/releases).


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

### Functionality Added or Changed
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

### Compilation Notes
- Added cmake parameter configuration for  build
```shell
cmake  .. -DWITH_MARISA  -DWITH_ROCKSDB
```
### Document Notes
- The manual has been updated as the code was modified. ( [# address](https://stonedb.io/))
Bugs Fixed
- fix some inherited mtr from MySQL
- fix Tianmu bug: [#282](https://github.com/stoneatom/stonedb/issues/282),[#274](about:blank),[#270](https://github.com/stoneatom/stonedb/issues/270),[#663](https://github.com/stoneatom/stonedb/issues/663),[#669](https://github.com/stoneatom/stonedb/issues/669),[#670](https://github.com/stoneatom/stonedb/issues/670),[#675](https://github.com/stoneatom/stonedb/issues/675),[#678](https://github.com/stoneatom/stonedb/issues/678),[#682](https://github.com/stoneatom/stonedb/issues/682),[#487](https://github.com/stoneatom/stonedb/issues/487),[#426](https://github.com/stoneatom/stonedb/issues/426),[#250](https://github.com/stoneatom/stonedb/issues/250),[#247](https://github.com/stoneatom/stonedb/issues/247),[#569](https://github.com/stoneatom/stonedb/issues/569),[#566](https://github.com/stoneatom/stonedb/issues/566),[#290](https://github.com/stoneatom/stonedb/issues/290),[#736](https://github.com/stoneatom/stonedb/issues/736),[#567](https://github.com/stoneatom/stonedb/issues/567),[#500](https://github.com/stoneatom/stonedb/issues/500),[#300](https://github.com/stoneatom/stonedb/issues/300),[#289](https://github.com/stoneatom/stonedb/issues/289),[#566](https://github.com/stoneatom/stonedb/issues/566),[#279](https://github.com/stoneatom/stonedb/issues/279),[#570](https://github.com/stoneatom/stonedb/issues/570)[,#571](https://github.com/stoneatom/stonedb/issues/571),[#580](https://github.com/stoneatom/stonedb/issues/580),[#581](https://github.com/stoneatom/stonedb/issues/581),[#586](https://github.com/stoneatom/stonedb/issues/586),[#589](https://github.com/stoneatom/stonedb/issues/589),[#674](https://github.com/stoneatom/stonedb/issues/674),[#646](https://github.com/stoneatom/stonedb/issues/646),[#280](https://github.com/stoneatom/stonedb/issues/280),[#301](https://github.com/stoneatom/stonedb/issues/301),[#733](https://github.com/stoneatom/stonedb/issues/733) et. al.


## StoneDB-5.7-V1.0.0
Changes in StoneDB_5.7_v1.0.0 (2022-08-31, General Availability)

- Support for MySQL 5.7
- Functionality Added or Changed
- Compilation Notes
- Configuration Notes
- Document Notes
- Bugs Fixed

### Support for MySQL 5.7

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

### Compilation Notes

- The version of the Boost library for server builds is now 1.66.0.

- The version of the Rocksdb for server builds is now 6.12.6.

### Configuration Notes

- Important Change: Changed default config file stonedb.cnf to my.cnf. ([feature #182](https://github.com/stoneatom/stonedb/issues/182))

- Important Change: Use tianmu as the default storage engine. ([feature #255](https://github.com/stoneatom/stonedb/issues/255))

### Document Notes

- The manual has been updated as the code was modified. ( [# address](https://stonedb.io/))

### Bugs Fixed

- fix mtr case: [BUG #78](https://github.com/stoneatom/stonedb/issues/78), [BUG #73](https://github.com/stoneatom/stonedb/issues/73),[ BUG #170](https://github.com/stoneatom/stonedb/issues/170), [BUG #192](https://github.com/stoneatom/stonedb/issues/192), [BUG #191](https://github.com/stoneatom/stonedb/issues/191), [BUG #227](https://github.com/stoneatom/stonedb/issues/227),  [BUG #245](https://github.com/stoneatom/stonedb/issues/245), BUG  #263

- fix Tianmu bug: [BUG #338](https://github.com/stoneatom/stonedb/issues/388),[ BUG #327](https://github.com/stoneatom/stonedb/issues/327), [BUG #212](https://github.com/stoneatom/stonedb/issues/212), [BUG #142](https://github.com/stoneatom/stonedb/issues/142)

## StoneDB-5.6-V1.0.0
Release time: June 30, 2022

