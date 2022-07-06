---
id: create-and-manage-table
sidebar_position: 5.52
---

# Create and Manage a Table

Create a table. For example, execute the following SQL statement to create a table which is named **student** and consists of the **id**, **name**, **age**, and **birthday** fields:

```sql
create table student(
  id int(11) primary key,
  name varchar(255),
  age smallint,
  birthday DATE
    ) engine=stonedb;
```

Query the schema of a table. For example, execute the following SQL statement to query the schema of table **student**:

```sql
show create table student\G
```

Drop a table. For example, execute the following SQL statement to drop table **student**:

```sql
drop table student;
```