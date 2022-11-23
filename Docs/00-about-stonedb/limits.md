---
id: limits
sidebar_position: 1.3
---

# Limits
Developed based on MySQL, StoneDB is compatible with the MySQL 5.6 and 5.7 protocols, and the ecosystem, common features, and common syntaxes of MySQL. However, due to characteristics of StoneDB, the following MySQL operations and features are not supported.
## Unsupported DDL operations
StoneDB does not support the following DDL operations:

- Change the character set of a table.
- Optimize a table.
- Analyze a table.
- Repair a table.
- Lock a table.
- Execute a CREATE TABLE … AS SELECT statement.
- Add a unique constraint.
- Delete a unique constraint.
- Add a primary key constraint.
- Delete a primary key constraint.
- Create an index.
- Drop an index.
- Modify a table comment.

The table attributes and column attributes are difficult to modify. Therefore, the character sets, data types, constraints, and indexes must be properly defined when tables are being created.
## Unsupported DML operations
StoneDB does not support the following DML operations:

- Use subqueries in an `UPDATE` statement.
- Execute an `UPDATE ... JOIN` statement to update multiple tables.
- Execute a `REPLACE ... INTO` statement.
:::info
For a `REPLACE ... INTO` statement that is used to insert one row, it can be executed only when no primary key or unique constraint conflict exists.
:::
StoneDB is not suitable for applications that are frequently updated. It supports only single-table update and insert operations. This is because a column-oriented store needs to find each corresponding column and update the value in the row when processing an update operation. However, a row-oriented store organizes data by row. When processing an update operation, the row-oriented store only needs to find the corresponding page or block and update the data directly in the row.
## Joins across storage engines not supported
By default, StoneDB does not support joins across storage engines. If a join involves tables in both other storage engines and StoneDB, an error will be reported. You can set the **tianmu_ini_allowmysqlquerypath** parameter to **1** in the **my.cnf** configuration file to remove this limit.
## Unsupported objects
StoneDB does not support the following objects:

- Global indexes
- Unique constraints
- Stored procedures containing dynamic SQL statements
- User-defined functions containing nested SQL statements
## Unsupported data types
StoneDB does not support the following data types:

- bit
- enum
- set
- json
- decimal whose precision is higher than 18, for example, decimal(19,x)
- Data types that contain keyword **unsigned** or **zerofill**
## Transactions not supported
Transactions must strictly comply with the ACID attributes. However, StoneDB does not support redo and undo logs and thus does not support transactions.
## Partitions not supported
Column-based storage engines do not support partitioning.
## Row locks and table locks not supported
Column-based storage engines do not support row locks or table locks.
