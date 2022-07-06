---
id: assignment-operators
sidebar_position: 6.35
---

# Assignment Operators

In the SET clause of an UPDATE statement, the equal sign (`=`) functions as an assignment operator. In this case, the value on the right side of the operator is assigned to the variable on the right side, provided that any WHERE conditions specified in the UPDATE statement are met.

The following code provides an example.

```sql
mysql> select * from t;
+------+------+
| col1 | col2 |
+------+------+
|  100 |  100 |
+------+------+
1 row in set (0.00 sec)

mysql> update t set col1=col1+100,col2=col1+100;
Query OK, 1 row affected (0.00 sec)
Rows matched: 1  Changed: 1  Warnings: 0

mysql> select * from t;                         
+------+------+
| col1 | col2 |
+------+------+
|  200 |  300 |
+------+------+
1 row in set (0.00 sec)
```