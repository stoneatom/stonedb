---
id: create-and-manage-view
sidebar_position: 5.53
---

# Create and Manage a View

Create a view. For example, execute the following SQL statement to create a view named **v_s**, used to query teachers aged over 18:
```sql
create view v_s as select name from teachers where age>18;
```
Check the statement used for creating a view. For example, execute the following SQL statement to check the statement used for creating view **v_s**:
```sql
show create view v_s;
```
Drop a view. For example, execute the following SQL statement to drop view **v_s**:
```sql
drop view v_s;
```