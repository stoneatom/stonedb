---
id: comparison-operators
sidebar_position: 6.33
---

# Comparison Operators

This topic describes the comparison operators supported StoneDB. 

| **Operator** | **Description** |
| --- | --- |
| `=` | Equal operator |
| `>`  | Greater than operator |
| `<`  | Less than operator |
| `>=` | Greater than or equal operator |
| `<=` | Less than or equal operator |
| `!=`, `<>` | Not equal operator |
| `<=>` | NULL-safe equal operator |
| `BETWEEN… AND…` | Whether a value is within a value range |
| `IN` | Whether a value is within a set of values |
| `NOT IN` | Whether a value is not within a set of values |
| `LIKE` | Simple pattern matching |
| `regexp` | Regular expression |
| `IS NULL` | NULL value test |
| `IS NOT NULL` | NOT NULL value test |

The following code provides an example of using each operator.

```sql
> select 2=3;
+-----+
| 2=3 |
+-----+
|   0 |
+-----+
1 row in set (0.00 sec)

> select 2>3;
+-----+
| 2>3 |
+-----+
|   0 |
+-----+
1 row in set (0.00 sec)

> select 2<3;
+-----+
| 2<3 |
+-----+
|   1 |
+-----+
1 row in set (0.00 sec)

> select 2>=3;
+------+
| 2>=3 |
+------+
|    0 |
+------+
1 row in set (0.00 sec)

> select 2<=3;
+------+
| 2<=3 |
+------+
|    1 |
+------+
1 row in set (0.00 sec)

> select 2<>3;
+------+
| 2<>3 |
+------+
|    1 |
+------+
1 row in set (0.00 sec)

> select 2<=>3;
+-------+
| 2<=>3 |
+-------+
|     0 |
+-------+
1 row in set (0.01 sec)

> select 5 between 1 and 10;
+--------------------+
| 5 between 1 and 10 |
+--------------------+
|                  1 |
+--------------------+
1 row in set (0.00 sec)

> select 5 in (1,2,3,4,5);
+------------------+
| 5 in (1,2,3,4,5) |
+------------------+
|                1 |
+------------------+
1 row in set (0.00 sec)

> select 5 not in (1,2,3,4,5);
+----------------------+
| 5 not in (1,2,3,4,5) |
+----------------------+
|                    0 |
+----------------------+
1 row in set (0.00 sec)

> select '12345' like '12%';
+--------------------+
| '12345' like '12%' |
+--------------------+
|                  1 |
+--------------------+
1 row in set (0.00 sec)

> select 'beijing' REGEXP 'jing';
+-------------------------+
| 'beijing' REGEXP 'jing' |
+-------------------------+
|                       1 |
+-------------------------+
1 row in set (0.00 sec)

> select 'beijing' REGEXP 'xi';
+-----------------------+
| 'beijing' REGEXP 'xi' |
+-----------------------+
|                     0 |
+-----------------------+
1 row in set (0.00 sec)

> select 'a' is NULL;
+-------------+
| 'a' is NULL |
+-------------+
|           0 |
+-------------+
1 row in set (0.00 sec)

> select 'a' IS NOT NULL;
+-----------------+
| 'a' IS NOT NULL |
+-----------------+
|               1 |
+-----------------+
1 row in set (0.00 sec)
```