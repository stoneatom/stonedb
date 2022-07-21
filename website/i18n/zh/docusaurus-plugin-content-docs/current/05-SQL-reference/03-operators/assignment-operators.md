---
id: assignment-operators
sidebar_position: 6.35
---

# 赋值运算符

在UPDATE语句的SET子句中，= 还可充当赋值运算符。在这种情况下，如果满足UPDATE语句的WHERE条件就会将运算符右侧的值赋值给左侧的列。
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
