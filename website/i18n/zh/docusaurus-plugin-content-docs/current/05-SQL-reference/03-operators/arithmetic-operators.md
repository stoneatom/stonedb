---
id: arithmetic-operators
sidebar_position: 6.31
---

# 算术运算符

StoneDB的算术运算符有如下。

| **运算符** | **作用** |
| --- | --- |
| + | 加 |
| - | 减 |
| * | 乘 |
| /（或div） | 除 |
| %（mod） | 取模 |

```sql
> select 10+2 from dual;
+------+
| 10+2 |
+------+
|   12 |
+------+
1 row in set (0.00 sec)

> select 10-2 from dual;
+------+
| 10-2 |
+------+
|    8 |
+------+
1 row in set (0.00 sec)

> select 10*2 from dual;
+------+
| 10*2 |
+------+
|   20 |
+------+
1 row in set (0.00 sec)

> select 10/2 from dual;
+--------+
| 10/2   |
+--------+
| 5.0000 |
+--------+
1 row in set (0.00 sec)

> select 10 div 2 from dual;
+----------+
| 10 div 2 |
+----------+
|        5 |
+----------+
1 row in set (0.00 sec)

> select 10 mod 3 from dual;
+----------+
| 10 mod 3 |
+----------+
|        1 |
+----------+
1 row in set (0.00 sec)

> select 10 % 3 from dual;
+--------+
| 10 % 3 |
+--------+
|      1 |
+--------+
1 row in set (0.00 sec)

> select 10 mod 0 from dual;
+----------+
| 10 mod 0 |
+----------+
|     NULL |
+----------+
1 row in set (0.00 sec)

> select 10 / 0 from dual;  
+--------+
| 10 / 0 |
+--------+
|   NULL |
+--------+
1 row in set (0.00 sec)
```
注：在除法运算和模运算中，如果除数为0，将是非法除数，返回结果为NULL。
