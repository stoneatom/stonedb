---
id: bitwise-operators
sidebar_position: 6.32
---

# Bitwise Operators

This topic describes the bitwise operators supported by StoneDB.

| **Operator** | **Description** |
| --- | --- |
| `&` | Bitwise AND |
| &#124; | Bitwise OR |
| `^` | Bitwise XOR |
| `!` | Bitwise inversion |
| `<<` | Left shift |
| `>>` | Right shift |

Bitwise operators are used to operate on binary numbers. In a bitwise operation, the involved numbers are first converted to binary numbers to compute the result, and then the result is converted back to a decimal value.

The following code provides an example of using each operator.

```sql
> select 3&5;
+-----+
| 3&5 |
+-----+
|   1 |
+-----+
1 row in set (0.00 sec)

> select 3|5;
+-----+
| 3|5 |
+-----+
|   7 |
+-----+
1 row in set (0.00 sec)

> select 3^5;
+-----+
| 3^5 |
+-----+
|   6 |
+-----+
1 row in set (0.00 sec)

> select ~18446744073709551612;
+-----------------------+
| ~18446744073709551612 |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

> select 3>>1;
+------+
| 3>>1 |
+------+
|    1 |
+------+
1 row in set (0.00 sec)

> select 3<<1;
+------+
| 3<<1 |
+------+
|    6 |
+------+
1 row in set (0.00 sec)
```