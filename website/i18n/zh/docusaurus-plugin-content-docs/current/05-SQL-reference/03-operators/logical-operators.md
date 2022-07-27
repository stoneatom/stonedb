---
id: logical-operators
sidebar_position: 6.34
---

# 逻辑运算符

StoneDB的逻辑运算符有如下。

| **逻辑运算符** | **作用** |
| --- | --- |
| NOT | 逻辑非 |
| AND | 逻辑与 |
| OR | 逻辑或 |
| XOR | 逻辑异或 |

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
