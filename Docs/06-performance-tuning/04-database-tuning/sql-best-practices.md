---
id: sql-best-practices
sidebar_position: 7.41
---

# Best Practices for SQL Coding

## **Best practices for designing tables**
- Define primary keys for your StoneDB tables. We recommend that you use primary keys to uniquely identify each record in your StoneDB tables, though StoneDB does not require you to create indexes.
- Use auto-increment primary keys and do not use Universally Unique Identifiers (UUIDs) as primary keys. Auto-increment primary keys can be used to improve performance of insert operations and prevent data page splits and fragmentation to improve space utilization. UUIDs are not ordered and space consuming.
- Do not use foreign key constraints. Each time after an insert, update, or delete operation on a table that is defined with foreign keys, an integrity check is performed on the table. This reduces query performance.
- Use data type CHAR to define fixed-length character fields and data type VARCHAR to define variable-length character fields.
- Properly define the length of each field. If the defined length is much longer than that of the stored record, a large amount of space will be wasted and the access efficiency is reduced.
- Define each field to not null and provide each field with a default value, if possible.
- Define a timestamp field in each table. Timestamps can be used for obtaining incremental data to estimate the number of rows generated in a specified time range, and to facilitate data cleaning and archiving.
- Do not use big object field types. If big object fields are retrieved for a query, a large amount of network and I/O resources will be consumed. We recommend that you store big objects in external storage.
- Do not use a reserved keyword such as **desc**, **order**, **group**, or **distinct** as a table or field name.
- Ensure that the fields in a table use the same character set as the table.
## **Best practices for writting SQL queries**

### **Avoid the USE of `SELECT *` **
When you use a `SELECT` statement, specify the names of columns, instead of using a wildcard (*). This is because using `SELECT *` has the following negative impacts:

- Transmit irrelevant fields from the server to the client, incurring additional network overhead.
- Affect the execution plan of the statement. A `SELECT *` statement is much slower than a `SELECT _Column names_` statement because a `SELECT _Column names_` statement can return data by using only column indexes.

Following are statement examples:

Avoid:
```sql
select * from test;
```
Prefer:
```sql
select id,name from test;
```
### **Avoid use `OR` in a `WHERE` clause**
Use `UNION ALL` instead of `OR` when combing multiple fields in a `WHERE` clause to split the query into multiple queries. Otherwise, the indexes may become invalid.

Following are statement examples:

Avoid:
```sql
select * from test where group_id='40' or user_id='uOrzp9ojhfgqcwRCiume';
```
Prefer:
```sql
select * from test where group_id='40'
union all
select * from test where user_id='uOrzp9ojhfgqcwRCiume';
```
### **Do not compute on indexed columns**
If an indexed column is used for computation, the index will become invalid.

Following are statement examples:

Avoid:
```sql
select * from test where id-1=99;
```
Prefer:
```sql
select * from test where id=100;
```
### **Avoid enclosing indexed columns in functions**
If a function is used on an indexed column, the index will become invalid.

Following are statement examples:

Avoid:
```sql
select * from test where date_add(create_time,interval 10 minute)=now();
```
Prefer:
```sql
select * from test where create_time=date_add(now(),interval - 10 minute);
```
### **Use a pair of apostrophes to quote the value for an indexed column whose data type is string**
If the data type of an indexed column is string and a number not quoted with a pair of apostrophes is specified as the value in the indexed column, the number will be automatically converted to a string. As a result, the index will become invalid.

Following are statement examples:

Avoid:
```sql
select * from test where group_id=40;
```
Prefer:
```sql
select * from test where group_id='40';
```
### **Avoid use `NOT` or `<>` on indexed columns**
If `NOT` or `<>` is used on an indexed column, the index will become invalid.
### **Avoid use `IS NOT NULL` on an indexed column**
If `IS NOT NULL` is used on an indexed column, the index will become invalid.
### **Do not use leading wildcards unless necessary**
If leading wildcards are used, relevant indexes will become invalid.
### **Use TRUNCATE instead of DELETE to delete a large table if no WHERE clause is used**
`TRUNCATE` is an DDL operation, which is faster and will release space after the table is deleted.
### **Use batch operations when deleting or updating a large amount of data**
We recommend that you split a large transaction into small transactions, since the locking scope for each small transaction is much smaller and the locking duration is much shorter. By doing this, the efficiency of system resources is improved.
### **Use batch operations when inserting a large amount of data**
We recommend you use batch operations when inserting a large amount of data. This can greatly reduce the number of commits, improving query performance.
### **Commit transactions as soon as possible**
We recommend that you commit transactions as soon as possible to reduce the lock time.
### **Avoid use `HAVING` to filter data**
When `HAVING` is used to filter the data set at the last step, the data set is sorted and summarized. Therefore, use `WHERE` to replace `HAVING` if possible.

Following are statement examples:

Avoid:
```sql
select job,avg(salary) from test group by job having job = 'managent';
```
Prefer:
```sql
select job,avg(salary) from test where job = 'managent' group by job;
```
### **Exercise with caution when using user-defined functions**
When a function is called by an SQL statement, the number of times that the function is called is equal to the number of records contained in the result set returned. If the result set of the query is large, the query performance will be deteriorated.
### **Exercise with caution when using scalar subqueries**
The number of times that a scalar subquery is executed is equal to the number of records returned for its main query. If the result set of the main query is large, the query performance will be deteriorated.

Following are statement examples:

Avoid:
```sql
select e.empno, e.ename, e.sal,e.deptno,
(select d.dname from dept d where d.deptno = e.deptno) as dname
from emp e;
```
Prefer:
```sql
select e.empno, e.ename, e.sal, e.deptno, d.dname
from emp e
left join dept d
on e.deptno = d.deptno;
```
### **Try to use the same sorting sequence, if fields need to be sorted**
If the fields in the same SQL statement need to be sorted and use the same sorting sequence, indexes can be used to eliminate CPU overhead caused by sorting. Otherwise, excessive CPU time will be consumed. In the first example provided bellowed, field **a** is sorted in descending order while field **b** is sorted in ascending order. As a result, the optimizer cannot use indexes to avoid the sorting process.

Following are statement examples:

Avoid:
```sql
select a,b from test order by a,b desc;
```
Prefer:
```sql
select a,b from test order by a,b;
select a,b from test order by a desc,b desc;
```
### **Use as few joins as possible**
The more tables that are joined in an SQL statement indicates longer time and higher cost spent in compiling the statement. In addition, the optimizer has a higher probability of failing to choose the best execution plan.
### **Keep levels of nesting as few as possible**
If too many nesting levels exist in an SQL statement, temporary tables will be generated and the execution plan generated for the SQL statement may have poor performance.

Following are statement examples:

Avoid:
```sql
select * from t1 a where a.proj_no in
(select b.proj_no from t2 b where b.proj_name = 'xxx' 
and not exists
(select 1 from t3 c where c.mess_id = b.t_project_id))
and a.oper_type <> 'D';
```
### **Specify the join condition when joining two tables**
If no join condition is specified when two tables are joined, Cartesian products will be generated. In such case, if both tables store a large amount of data, such SQL statement will consume a lot of CPU and memory resources.

Following are statement examples:

Avoid:
```sql
select * from a,b;
```
Prefer:
```sql
select * from a,b where a.id=b.id;
```
### **Use a comparatively small offset for pagination with `LIMIT`**
When a pagination query with `LIMIT` is processed, the offset data is first obtained, the data for pagination is later obtained, and the offset data is discarded to return only the paginated data. Therefore, if the offset is large, the performance of the SQL statement will be poor.

Following are statement examples:

Avoid:
```sql
select id,name from test limit 10000,10;
```
Prefer:
```sql
select id,name from test where id>10000 limit 10;
```
### **In a `LEFT JOIN` operation, ensure the table on the left has a smaller result set**
In most cases, the table on the left in a `LEFT JOIN` functions as the driving table. The number of records in the result set of the driving table is equal to the number of times that the driven table is executed. Therefore, if the result set of the driving table is large, the performance will be poor.
### **Use EXIST and IN accurately**
When to use `EXISTS` or `IN` is determined by the result set sizes of the outer query and inner query. If the result set of the outer query is larger than that of the inner query, `IN` is superior to `EXIST`. Otherwise, `EXIST` is preferred.
### **Use `UNION ALL` and `UNION` accurately**
A `UNION ALL` operation simple combines the two result sets and returns the collection. A `UNION` operation combines the two result sets and sort, deduplicates the records in the collection, and then returns the collection. We recommend that you use `UNION ALL` if possible, because `UNION` consumes more resources.
### **Use `LEFT JOIN` and `INNER JOIN` accurately**
In a `LEFT JOIN` operation, the rows that match in both tables and the remaining rows in the table on the left are returned. In an `INNER JOIN` operation, only the rows that match in both tables are returned.
### **In a `LEFT JOIN`, use `ON … AND` and `ON … WHERE` accurately**
The following information describes the main differences between `ON … AND` and `ON … WHERE`:

- `ON … AND` does not provide the filtering capability. Rows that have no match in the table on the right are filled with null.
- `ON … WHERE` provides the filtering capability. No matter whether the predicate condition is placed after `ON` or `WHERE`, rows in the table on the right are filtered first. However, if the predicate condition is placed after `WHERE`, the `LEFT JOIN` operation will be converted into an `INNER JOIN` operation.
### **In an `INNER JOIN`, use `ON … AND` and `ON … WHERE` accurately**
In an `INNER JOIN` operation, `ON … AND` is equivalent to `ON … WHERE`. The both provide the filtering capability.
### **Avoid uncessary sorting**
For count operations, sorting is unnecessary.

Following are statement examples:

Avoid:
```sql
select count(*) as totalCount from 
(select * from enquiry e where 1 = 1
AND status = 'closed'
AND is_show = 1
order by id desc, expire_date asc) _xx;
```
Prefer:
```sql
select count(*) from enquiry e where 1 = 1
AND status = 'closed'
AND is_show = 1;
```
### **Avoid unnecessary nesting**
For queries that can be implemented by a single `SELECT`, do not use nested `SELECT`.

Following are statement examples:

Avoid:
```sql
select count(*) as totalCount from 
(select * from enquiry e where 1 = 1 
AND status = 'closed'
AND is_show = 1
order by id desc, expire_date asc) _xx;
```
Prefer:
```sql
select count(*) from enquiry e where 1 = 1
AND status = 'closed'
AND is_show = 1;
```
### Each time after an SQL statement is written, execute an EXPLAIN statement to query its execution plan
Each time after you write an SQL statement, we recommend that you execute `EXPLAN` to check the execution plan of the SQL statement and pay special attention to parameters **type**, **rows**, and **extra**.
