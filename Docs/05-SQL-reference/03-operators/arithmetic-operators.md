---
id: arithmetic-operators
sidebar_position: 6.31
---

# Arithmetic Operators

This topic describes the arithmetic operators supported by StoneDB.

| **Operator** | **Description** |
| --- | --- |
| `+` | Addition operator |
| `-` | Minus operator |
| `*` | Multiplication operator |
| `/`, `div` | Division operator |
| `%`, `mod` | Modulo operator |

The following code provides an example of using each operator.

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
:::tip
If the divisor is 0 in a division operation or a modulo operation, the operation is invalid and NULL is returned.
:::
