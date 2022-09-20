---
id: sql-tuning
sidebar_position: 7.42
---

# SQL Tuning
When an SQL statement passes through the optimizer, the optimizer analyzes the SQL statement to generate an execution plan. Then, the executor calls API operations to read data from the relevant tables and returns the query result. An SQL statement can have multiple execution plans, each of which describes a sequence of steps to access data for the SQL statement.

Though the query results retuned by the execution plans are the same, but the performance varies. The performance of an execution plan depends on many factors such as statistics information, whether to use temporary tables, offset of pagination query, and optimizer parameter settings. This topic describes how to optimize SQL statements from the following points:

- Execution process of an SQL statement
- Columns included in a StoneDB execution plan
- Common StoneDB execution plans
- Common optimization methods
- Table joins
## Execution process of an SQL statement

1. The client sends an SQL statement to the server. The connector verifies whether the username and password are correct and whether the user has sufficient permissions.
2. After the client is connected to the server, the server checks whether the SQL statement and query result exist in the cache. If yes, the query result is directly returned from the cache. If no, the next step is performed.
3. The analyzer performs lexical analysis and syntax analysis on the SQL statement to check whether the queried tables and columns exist and whether the SQL statement complies with the syntax.
4. The optimizer generates execution plans based on the statistics information and chooses the execution plan that has the lowest cost.
5. The executor checks whether the user has sufficient permissions on the relevant tables and calls API operations to read relevant rows of data. If the data pages where the rows reside are stored in memory, the data is directly returned to the executor. In other cases, the data is first read from the disks to the memory and then returned to the executor.
## Columns included in a StoneDB execution plan
The following table describes the columns included in a StoneDB execution plan.

| **Column** | **Description** |
| --- | --- |
| id | The sequence number of an operation. :::info
A higher _id_ indicates higher priority for execution.<br/>
If two or more operations have the same _id_, first come, first served.:::|
| select_type | The query type, used to categorize simple queries, JOIN queries, and subqueries. The value can be:<br />- **SIMPLE**: simple query.<br />- **PRIMARY**: outmost `SELECT`. If a query contains subqueries, the outermost `SELECT` is marked with **PRIMARY**.<br />- **SUBQUERY**: subquery, normally placed after a `SELECT` or `WHERE` clause.<br />- **DEPENDENT SUBQUERY**: first `SELECT` statement in subquery, dependent on the outer query. The number of times that the subquery is executed is equal to the number of records contained in the result set of the outer query.<br />- **DERIVED**: derived query, normally placed after a `FROM` clause.<br />- **UNION**: second or later `SELECT` statement in a `UNION` operation.<br />- **UNION RESULT**: result of a `UNION` operation.<br /> |
| table | The name of the table to access in the current step. |
| partitions | The partitions from which records would be matched by the query. |
| type | The join type. The value can be:<br />- **eq_ref**: One record is read from the table for each combination of rows from the previous tables. It is used when all parts of an index are used by the join and the index is a PRIMARY KEY or UNIQUE NOT NULL index.<br />- **ref**: All rows with matching index values are read from this table for each combination of rows from the previous tables. **ref** is used if the join uses only a leftmost prefix of the key or if the key is not a PRIMARY KEY or UNIQUE index.<br />- **range**: **range** can be used when a key column is compared to a constant using any of the `=`, `<>`, `>`, `>=`, `<`, `<=`, `IS NULL`, `<=>`, `BETWEEN`, `LIKE`, or `IN()` operators.<br />- **index_merge**: Indexes are merged.<br />- **index_subquery**: The outer query is associated with the subquery. Some of the join fields of the subquery contain are indexed.<br />- **all**: The full table is scaned.<br /> |
| possible_keys | The possible indexes to choose. |
| key | The index that is chosed. |
| key_len | The length of the chosen index, used to identify whether compound indexes are fully used. The algorithm to calculate **key_len** for a data type varies with the character set. |
| ref | Specifies which fields or constants are compared to the index named in the **key** column to select rows from the table. If a constant is used, the value is **const**, if a join field is used, the value is the join field of the driving table. |
| rows | The number of rows estimated to scan. A larger number indicates longer execution time. This value is an estimated value. Because StoneDB is a column-based storage engine, data is highly compressed, the estimated value may not be exact.  |
| filtered | An estimated percentage of records that are filtered to read. The maximum value is 100, which means no filtering of rows occurred. For MySQL 5.7 and later, this column is returned by default. For MySQL versions earlier than 5.7, this column is not returned unless you execute `EXPLAIN EXTENDED`. |
| Extra | The additional information about the execution. The value can be:<br />- **Using where with pushed condition**: The data returned by the storage engine is filtered on the server, regardless whether indexes are used.<br />- **Using filesort**: Sorting is required. <br />- **Using temporary**: A temporary table needs to be created to store the result set. In most cases, this happens if the query contains `UNION`, `DISTINC`, `GROUP BY`, or `ORDER BY` clauses that list columns differently.<br />- **Using union**: The result set is obtained by using at least two indexes and the indexed fields are joined by using `OR`.<br />- **Using join buffer (Block Nested Loop)**: The Block Nested-Loop algorithm is used, which indicates that the join fields of the driven table are not indexed.<br /> |

# **Common StoneDB execution plans**
## **Execution plans for index scans**
In an execution plan, the index scan type can be **eq_ref**, **ref**, **range**, **index_merge**, or **index_subquery**.
```sql
> explain select * from t_atomstore where id=1;
+----+-------------+-------------+------------+--------+---------------+---------+---------+-------+------+----------+-------+
| id | select_type | table       | partitions | type   | possible_keys | key     | key_len | ref   | rows | filtered | Extra |
+----+-------------+-------------+------------+--------+---------------+---------+---------+-------+------+----------+-------+
|  1 | SIMPLE      | t_atomstore | NULL       | eq_ref | PRIMARY       | PRIMARY | 4       | const |    1 |   100.00 | NULL  |
+----+-------------+-------------+------------+--------+---------------+---------+---------+-------+------+----------+-------+

> explain select first_name from t_atomstore where first_name='zhou';
+----+-------------+-------------+------------+------+---------------+---------------+---------+-------+------+----------+-------+
| id | select_type | table       | partitions | type | possible_keys | key           | key_len | ref   | rows | filtered | Extra |
+----+-------------+-------------+------------+------+---------------+---------------+---------+-------+------+----------+-------+
|  1 | SIMPLE      | t_atomstore | NULL       | ref  | idx_firstname | idx_firstname | 32      | const |   10 |   100.00 | NULL  |
+----+-------------+-------------+------------+------+---------------+---------------+---------+-------+------+----------+-------+
Note: In this execution plan, the value in the "Extra" column is "NULL" instead of "Using index" even if all queried columns are indexed.

> explain select * from t_atomstore where first_name in('zhou','liu');
+----+-------------+-------------+------------+-------+---------------+---------------+---------+------+------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------+
| id | select_type | table       | partitions | type  | possible_keys | key           | key_len | ref  | rows | filtered | Extra                                                                                                                                              |
+----+-------------+-------------+------------+-------+---------------+---------------+---------+------+------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------+
|  1 | SIMPLE      | t_atomstore | NULL       | range | idx_firstname | idx_firstname | 32      | NULL |   20 |   100.00 | Using where with pushed condition (`test`.`t_atomstore`.`first_name` in ('zhou','liu'))(t0) Pckrows: 2, susp. 2 (0 empty 0 full). Conditions: 1   |
+----+-------------+-------------+------------+-------+---------------+---------------+---------+------+------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------+
```
## **Execution plans for full table scans**
In an execution plan, the type of a full table scan can be only **ALL**. This is because StoneDB is a column-based storage engine and its data is highly compressed. Most queries involve full table scans.
```sql
> explain select first_name,count(*) from t_atomstore group by first_name;
+----+-------------+-------------+------------+------+---------------+------+---------+------+--------+----------+---------------------------------+
| id | select_type | table       | partitions | type | possible_keys | key  | key_len | ref  | rows   | filtered | Extra                           |
+----+-------------+-------------+------------+------+---------------+------+---------+------+--------+----------+---------------------------------+
|  1 | SIMPLE      | t_atomstore | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 100000 |   100.00 | Using temporary; Using filesort |
+----+-------------+-------------+------------+------+---------------+------+---------+------+--------+----------+---------------------------------+
1 row in set, 1 warning (0.00 sec)
```
In this execution plan, though field **first_name** is indexed so that sorting is eliminated and temporary tables do not need to be created, the optimizer still chooses full table scans instead of full index scans.

A warning message is displayed here because StoneDB rewrites the SQL statement by including `ORDER BY NULL` in the statement. This rewrite eliminates sorting on the returned grouping field. On InnoDB, if the returned grouping field is not indexed, sorting is performed.
## **Execution plans for aggregate operations**
Data in StoneDB is highly compressed. StoneDB uses the knowledge grid technique to record metadata of data packs in data pack nodes. When processing a statistical or aggregate query, StoneDB can quickly obtain the result set based on the metadata, ensuring optimal performance.
```sql
> explain select first_name,sum(score) from t_test1 group by first_name;
+----+-------------+---------+------------+------+---------------+------+---------+------+---------+----------+---------------------------------+
| id | select_type | table   | partitions | type | possible_keys | key  | key_len | ref  | rows    | filtered | Extra                           |
+----+-------------+---------+------------+------+---------------+------+---------+------+---------+----------+---------------------------------+
|  1 | SIMPLE      | t_test1 | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000000 |   100.00 | Using temporary; Using filesort |
+----+-------------+---------+------------+------+---------------+------+---------+------+---------+----------+---------------------------------+
```
## **Execution plans for JOIN queries**
```sql
> explain select t1.id,t1.first_name,t2.first_name from t_test1 t1,t_test2 t2 where t1.id=t2.id and t1.first_name='zhou';
+----+-------------+-------+------------+--------+---------------+---------+---------+----------+---------+----------+------------------------------------------------------------------------------------------------------------------------------+
| id | select_type | table | partitions | type   | possible_keys | key     | key_len | ref      | rows    | filtered | Extra                                                                                                                        |
+----+-------------+-------+------------+--------+---------------+---------+---------+----------+---------+----------+------------------------------------------------------------------------------------------------------------------------------+
|  1 | SIMPLE      | t1    | NULL       | ALL    | PRIMARY       | NULL    | NULL    | NULL     | 1000000 |    10.00 | Using where with pushed condition (`xx`.`t1`.`first_name` = 'zhou')(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1  |
|  1 | SIMPLE      | t2    | NULL       | eq_ref | PRIMARY       | PRIMARY | 4       | xx.t1.id |       1 |   100.00 | NULL                                                                                                                         |
+----+-------------+-------+------------+--------+---------------+---------+---------+----------+---------+----------+------------------------------------------------------------------------------------------------------------------------------+
Note: In the execution plan, if any join field of the driven table is indexed, the Index Nested-Loop algorithm is used to join the two tables.

mysql> explain select t1.id,t1.first_name,t2.first_name from t_test1 t1,t_test2 t2 where t1.copy_id=t2.copy_id and t1.first_name='zhou';
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+------------------------------------------------------------------------------------------------------------------------------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows    | filtered | Extra                                                                                                                              |
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+------------------------------------------------------------------------------------------------------------------------------------+
|  1 | SIMPLE      | t1    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000000 |    10.00 | Using where with pushed condition (`xx`.`t1`.`first_name` = 'zhou')(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1        |
|  1 | SIMPLE      | t2    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000000 |    10.00 | Using where; Using join buffer (Block Nested Loop)                                                                                 |
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+------------------------------------------------------------------------------------------------------------------------------------+
Note: If no join field of the driven table is indexed, the Block Nested-Loop algorithm is used to join the two tables. In this case, the performance is poor.
```
## **Execution plans for subqueries**
```sql
> explain select t1.first_name from t_test1 t1 where t1.id in (select t2.id from t_test2 t2 where t2.first_name='zhou');
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows    | filtered | Extra                                                                                                                                                                                                                                                                                                                                                                                                                                              |
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|  1 | PRIMARY     | t1    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000000 |   100.00 | Using where with pushed condition <in_optimizer>(`xx`.`t1`.`id`,`xx`.`t1`.`id` in ( <materialize> (/* select#2 */ select `xx`.`t2`.`id` from `xx`.`t_test2` `t2` where (`xx`.`t2`.`first_name` = 'zhou') ), <primary_index_lookup>(`xx`.`t1`.`id` in <temporary table> on <auto_key> where ((`xx`.`t1`.`id` = `materialized-subquery`.`id`)))))(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1                                            |
|  2 | SUBQUERY    | t2    | NULL       | ALL  | PRIMARY       | NULL | NULL    | NULL | 1000000 |    10.00 | Using where with pushed condition (`xx`.`t2`.`first_name` = 'zhou')(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1                                                                                                                                                                                                                                                                                                                        |
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

> explain select t1.first_name from t_test1 t1 where exists (select 1 from t_test2 t2 where t1.id=t2.id and t2.first_name='zhou');
+----+--------------------+-------+------------+--------+---------------+---------+---------+----------------+---------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| id | select_type        | table | partitions | type   | possible_keys | key     | key_len | ref            | rows    | filtered | Extra                                                                                                                                                                                                                                                       |
+----+--------------------+-------+------------+--------+---------------+---------+---------+----------------+---------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|  1 | PRIMARY            | t1    | NULL       | ALL    | NULL          | NULL    | NULL    | NULL           | 1000000 |   100.00 | Using where with pushed condition exists(/* select#2 */ select 1 from `xx`.`t_test2` `t2` where ((`xx`.`t1`.`id` = `xx`.`t2`.`id`) and (`xx`.`t2`.`first_name` = 'zhou')))(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1                          |
|  2 | DEPENDENT SUBQUERY | t2    | NULL       | eq_ref | PRIMARY       | PRIMARY | 4       | xx.t1.id       |       1 |    10.00 | Using where with pushed condition (`xx`.`t2`.`first_name` = 'zhou')(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1                                                                                                                                 |
+----+--------------------+-------+------------+--------+---------------+---------+---------+----------------+---------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```
# **Common optimization methods**
## GROUP BY
In MySQL, the GROUP BY operation sorts data first and then group the data. If you want to prevent temporary tables from being created and sorting, you must ensure that the field used for grouping is indexed. However, StoneDB uses the knowledge grid technique so that it can quickly filter needed data for statistical and aggregate queries based on the metadata recorded in data pack nodes. In this way, you do not need to create indexes.
## IN/EXISTS
In a join query, ensure that the table with smaller result set is used to drive the table with the larger result set. For example:
```sql
select * from A where id in (select id from B);
```
The optimizer executes the subquery first and then use the result set of the subquery as parameters of the outer query. Therefore, the previous example is equivalent to:
```sql
for select id from B
for select * from A where A.id = B.id
```
If the result set of table B is smaller than that of table A, `IN`  is superior to `EXISTS`, as shown in the following example:
```sql
> select count(*) from t_test;
+----------+
| count(*) |
+----------+
|     1000 |
+----------+
1 row in set (0.00 sec)

> select count(*) from t_test2;
+----------+
| count(*) |
+----------+
|  1000000 |
+----------+
1 row in set (0.00 sec)

> select count(*) from t_test2 where id in (select id from t_test);
+----------+
| count(*) |
+----------+
|     1000 |
+----------+
1 row in set (0.00 sec)

> select count(*) from t_test2 where exists (select 1 from t_test where t_test.id=t_test2.id);
+----------+
| count(*) |
+----------+
|     1000 |
+----------+
1 row in set (4.19 sec)
```
:::info
If `EXISTS` is used in this example, table A is used to drive table B and the execution time is 4.19s.
:::

```sql
select * from A where exists (select 1 from B where B.id = A.id);
```
The optimizer passes the value of the outer query to the subquery. the subquery is dependent on the outer query. The previous example is equivalent to:
```sql
for select * from A
for select * from B where B.id = A.id
```
If the result set of table A is smaller than that of table B, `EXISTS` is superior to `IN`, as shown in the following example:
```sql
> select count(*) from t_test where id in (select id from t_test2);
+----------+
| count(*) |
+----------+
|     1000 |
+----------+
1 row in set (0.55 sec)

> select count(*) from t_test where exists (select 1 from t_test2 where t_test2.id=t_test.id);
+----------+
| count(*) |
+----------+
|     1000 |
+----------+
1 row in set (0.03 sec)
```
:::info

If `IN` is used in this example, table B is used to drive table A and the execution time is 0.55s.

:::
## **Use IN and EXISTS interchangeably**
Only when the join fields of the subquery are not null, `IN` can be converted to or from `EXISTS`. If you want the join fields of your subquery to use indexes, you can convert `IN` to `EXISTS`, as shown in the following example:
```sql
mysql> explain select * from t_test1 t1 where t1.id in (select t2.id from t_test2 t2 where t2.first_name='zhou');
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows    | filtered | Extra                                                                                                                                                                                                                                                                                                                                                                                                                                              |
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|  1 | PRIMARY     | t1    | NULL       | ALL  | NULL          | NULL | NULL    | NULL | 1000000 |   100.00 | Using where with pushed condition <in_optimizer>(`xx`.`t1`.`id`,`xx`.`t1`.`id` in ( <materialize> (/* select#2 */ select `xx`.`t2`.`id` from `xx`.`t_test2` `t2` where (`xx`.`t2`.`first_name` = 'zhou') ), <primary_index_lookup>(`xx`.`t1`.`id` in <temporary table> on <auto_key> where ((`xx`.`t1`.`id` = `materialized-subquery`.`id`)))))(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1                                            |
|  2 | SUBQUERY    | t2    | NULL       | ALL  | PRIMARY       | NULL | NULL    | NULL | 1000000 |    10.00 | Using where with pushed condition (`xx`.`t2`.`first_name` = 'zhou')(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1                                                                                                                                                                                                                                                                                                                        |
+----+-------------+-------+------------+------+---------------+------+---------+------+---------+----------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 rows in set, 1 warning (0.00 sec)

mysql> explain select * from t_test1 t1 where exists (select 1 from t_test2 t2 where t1.id=t2.id and t2.first_name='zhou');
+----+--------------------+-------+------------+--------+---------------+---------+---------+----------------+---------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| id | select_type        | table | partitions | type   | possible_keys | key     | key_len | ref            | rows    | filtered | Extra                                                                                                                                                                                                                                                       |
+----+--------------------+-------+------------+--------+---------------+---------+---------+----------------+---------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|  1 | PRIMARY            | t1    | NULL       | ALL    | NULL          | NULL    | NULL    | NULL           | 1000000 |   100.00 | Using where with pushed condition exists(/* select#2 */ select 1 from `xx`.`t_test2` `t2` where ((`xx`.`t1`.`id` = `xx`.`t2`.`id`) and (`xx`.`t2`.`first_name` = 'zhou')))(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1                          |
|  2 | DEPENDENT SUBQUERY | t2    | NULL       | eq_ref | PRIMARY       | PRIMARY | 4       | xx.t1.id       |       1 |    10.00 | Using where with pushed condition (`xx`.`t2`.`first_name` = 'zhou')(t0) Pckrows: 16, susp. 16 (0 empty 0 full). Conditions: 1                                                                                                                                 |
+----+--------------------+-------+------------+--------+---------------+---------+---------+----------------+---------+----------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
2 rows in set, 2 warnings (0.00 sec)
```
## **Pagination**
Convert parts of pagination queries to subqueries and specify that only the primary keys are queried. This method ensures high performance and associates the subqueries with the main query as result set. The following code provides an example:
```sql
> select * from t_test1 order by id asc limit 949420,10;
+--------+------------+-----------+-----+-------+---------+
| id     | first_name | last_name | sex | score | copy_id |
+--------+------------+-----------+-----+-------+---------+
| 949421 | hou        | tianli    | 1   |    72 |  949421 |
| 949422 | li         | jingqi    | 1   |    45 |  949422 |
| 949423 | gao        | jingqi    | 1   |    84 |  949423 |
| 949424 | chen       | liyi      | 1   |    80 |  949424 |
| 949425 | ruan       | liyi      | 0   |    53 |  949425 |
| 949426 | lin        | liyi      | 0   |    92 |  949426 |
| 949427 | sun        | yi        | 0   |    92 |  949427 |
| 949428 | li         | chengyi   | 0   |    71 |  949428 |
| 949429 | yang       | chengyi   | 0   |    65 |  949429 |
| 949430 | chen       | chengyi   | 1   |    81 |  949430 |
+--------+------------+-----------+-----+-------+---------+
10 rows in set (0.41 sec)

> select a.* from t_test1 a,(select id from t_test1 order by id asc limit 949420, 10) b where a.id=b.id;
+--------+------------+-----------+-----+-------+---------+
| id     | first_name | last_name | sex | score | copy_id |
+--------+------------+-----------+-----+-------+---------+
| 949421 | hou        | tianli    | 1   |    72 |  949421 |
| 949422 | li         | jingqi    | 1   |    45 |  949422 |
| 949423 | gao        | jingqi    | 1   |    84 |  949423 |
| 949424 | chen       | liyi      | 1   |    80 |  949424 |
| 949425 | ruan       | liyi      | 0   |    53 |  949425 |
| 949426 | lin        | liyi      | 0   |    92 |  949426 |
| 949427 | sun        | yi        | 0   |    92 |  949427 |
| 949428 | li         | chengyi   | 0   |    71 |  949428 |
| 949429 | yang       | chengyi   | 0   |    65 |  949429 |
| 949430 | chen       | chengyi   | 1   |    81 |  949430 |
+--------+------------+-----------+-----+-------+---------+
10 rows in set (0.13 sec)
```
# Table joins
## Nested loop joins
The execution process is as follows:

1. The optimizer determines which table (table T1 or T2) is the driving table and which is the driven table based on certain rules. The driving table is used for the outer loop, and the driven table is used for the inner loop. In this example, the driving table is T1 and the driven table is T2.
2. Access driving table T1 based on the predicate condition specified in the SQL statement and record the result set as 1.
3. Traverse result set 1 and driven table T2: Read the records in result set 1 one by one. After reading each record, use the record to traverse driven table T2, checking whether a matching record exists in T2 based on the join condition.

If the join fields are indexed, use indexes to obtain rows that match the condition. For example, T1 has 100 rows and T2 has 1000 rows. T2 will be run for 100 times, and each time one row is scanned. The total number of rows scanned during the whole process is 200 rows.

If the fields that are joined are not indexed, the full table is scanned to obtain rows that match the condition. For example, T1 has 100 rows and T2 has 1000 rows. T2 will be run 100 times, and each time 1000 rows are scanned. The total number of rows scanned during the whole process is 100100.

Nested loop joins are suitable for join queries with small result sets.
## Hash joins
Suppose two tables A and B exist. Table A contains 100,000 records of data and table B contains 1,000,000 records of data. The range of IDs of table A is 1 to 100000 and that of table B is 1 to 1000000. The tables are joined based on IDs. 

SQL statement example:
```sql
SELECT * FROM A,B WHERE A.ID=B.ID
```

1. The optimizer selects table A as the driving table and table B as the driven table, and then creates a hash table in the memory.
2. The optimizer scans all content in table A and uses the hash function to calculate the hash value of each field, and then saves the hash value to the hash table.
3. The optimizer scans all content in table B and uses the hash function to calculate the hash value of each field.
4. The optimizer compares each hash value in table B with hash values stored in the hash table. If a matching record is found, the corresponding data is returned. Otherwise, the data is discarded.

Hash joins are suitable for join queries with large result sets.
## Sort-merge joins
The execution process is as follows:

1. Access table T1 based on the predicate condition specified in the SQL statement, sort the result set based on the column in table T1 used for join, and then mark the result set as result set 1.
2. Access table T2 based on the predicate condition specified in the SQL statement, sort the result set based on the column in table T2 used for join, and then mark the result set as result set 2.
3. Traverse result set 1: Read the records in result set 1 one by one. After reading each record, use the record to traverse result set 2, checking whether a matching record exists in result set 2 based on the join condition.
