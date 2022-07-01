---
id: create-and-manage-database
sidebar_position: 5.51
---

# Create and Manage a Database

Create a database. For example, execute the following SQL statement to create a database named **test_db** that uses **utf8mb4** as the default character set:

```sql
create database test_db DEFAULT CHARACTER SET utf8mb4;
```

List databases by executing the following SQL statement:

```sql
show databases;
```

Use a database. For example, execute the following SQL statement to use database **test_db**:

```sql
use test_db;
```

Drop a datable. For example, execute the following SQL statement to drop database **test_db**:

```sql
drop database test_db;
```
