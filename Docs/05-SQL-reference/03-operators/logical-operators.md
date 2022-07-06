---
id: logical-operators
sidebar_position: 6.34
---

# Logical Operators

This topic describes the logical operators supported by StoneDB.

| **Operator** | **Description** |
| --- | --- |
| NOT | Logical NOT |
| AND | Logical AND |
| OR | Logical OR |
| XOR | Logical XOR |

The following code provides an example of using each operator.

```sql
> select not 1;
+-------+
| not 1 |
+-------+
|     0 |
+-------+
1 row in set (0.00 sec)

> select !0;
+----+
| !0 |
+----+
|  1 |
+----+
1 row in set (0.00 sec)

> select 2 and 0;
+---------+
| 2 and 0 |
+---------+
|       0 |
+---------+
1 row in set (0.00 sec)

> select 2 and 1;   
+---------+
| 2 and 1 |
+---------+
|       1 |
+---------+
1 row in set (0.00 sec)

> select 2 or 0;
+--------+
| 2 or 0 |
+--------+
|      1 |
+--------+
1 row in set (0.00 sec)

> select 2 or 1;
+--------+
| 2 or 1 |
+--------+
|      1 |
+--------+
1 row in set (0.00 sec)

> select 1 xor 1;
+---------+
| 1 xor 1 |
+---------+
|       0 |
+---------+
1 row in set (0.00 sec)

> select 0 xor 0;
+---------+
| 0 xor 0 |
+---------+
|       0 |
+---------+
1 row in set (0.00 sec)

> select 1 xor 0;
+---------+
| 1 xor 0 |
+---------+
|       1 |
+---------+
1 row in set (0.00 sec)

> select null or 1;
+-----------+
| null or 1 |
+-----------+
|         1 |
+-----------+
1 row in set (0.00 sec)
```