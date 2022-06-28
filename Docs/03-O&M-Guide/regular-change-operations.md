---
id: regular-change-operations
sidebar_position: 4.1
---

# Regular Change Operations

## Change operations on schemas and data of tables
This section describes the supported change operations on schemas and data of tables.

### Create tables using the same schema

1. Execute a `CREATE TABLE` statement to create a StoneDB table.

Code example:
```sql
CREATE TABLE t_name(
  col1 INT NOT NULL AUTO_INCREMENT,
  col2 VARCHAR(10) NOT NULL,
  ......
  PRIMARY KEY (`id`)
) engine=STONEDB;
```

2. Execute a `CREATE TABLE... LIKE `statement to create another table that uses the same schema as the table created in the step 1.

Code example:
```
create table t_other like t_name;
```

### Clear data in a table
Execute a `TRUNCATE TABLE` statement to clear data in a table and retain the table schema.

Code example:
```
truncate table t_name;
```

### Add a field
Execute an `ALTER TABLE... ADD COLUMN` statement to add a field in a given table. The added field is the last field, by default. 
Code example:

```
alter table t_name add column c_name varchar(10);
```

### Drop a field
Execute an `ALTER TABLE... DROP` statement to drop a field from a table.<br />Code example:
```
alter table t_name drop c_name;
```

### Rename a table
Execute an `ALTER TABLE... RENAME TO` statement to rename a given table.<br />Code example:
```
alter table t_name rename to t_name_new;
```

## Change operations on user permissions

### Create a user
Code example:
```
create user 'u_name'@'hostname' identified by 'xxx';
```

### Grant user permissions
Code example:
```
grant all on *.* to 'u_name'@'hostname';
grant select on db_name.* to 'u_name'@'hostname';
grant select(column_name) on db_name.t_name to 'u_name'@'hostname';
```

### Revoke permissions
Code example:
```
revoke all privileges on *.* from 'u_name'@'hostname';
revoke select on db_name.* from 'u_name'@'hostname';
revoke select(column_name) on db_name.t_name from 'u_name'@'hostname';
```

### Drop a user
Code example:
```
drop user 'u_name'@'hostname';
```
