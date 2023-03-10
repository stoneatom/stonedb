---
id: limits
sidebar_position: 1.3
---

# Limits

Developed based on MySQL, StoneDB is compatible with the MySQL 5.6 and 5.7 protocols, and the ecosystem, common features, and common syntaxes of MySQL. However, due to characteristics of StoneDB, the following MySQL operations and features are not supported.
# Unsupported DDL operations
StoneDB does not support the following DDL operations:

- Optimize a table.
- Analyze a table.
- Repair a table.
- Lock a table.
- Create  foreign key
- Drop foreign key
# Joins across storage engines not supported
By default, StoneDB does not support joins across storage engines. If a join involves tables in both other storage engines and StoneDB, an error will be reported. You can set the **tianmu_ini_allowmysqlquerypath** parameter to **1** in the **my.cnf** configuration file to remove this limit.
# Unsupported objects
StoneDB does not support the following objects:

- Full text index
- Unique constraints
- Secondary index
- Foreign key
# Unsupported data types
StoneDB does not support the following data types:

- enum
- set
- json
- decimal whose precision is higher than 18, for example, decimal(19,x)
# Transactions not supported
Transactions must strictly comply with the ACID attributes. However, StoneDB does not support redo and undo logs and thus does not support transactions.
# Partitions not supported
Column-based storage engines do not support partitioning.
# Row locks and table locks not supported
Column-based storage engines do not support row locks or table locks.
