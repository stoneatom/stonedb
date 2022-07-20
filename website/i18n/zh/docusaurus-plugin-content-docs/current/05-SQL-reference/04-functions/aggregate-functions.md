---
id: aggregate-functions
sidebar_position: 6.45
---

# 聚合函数

| 函数名 | 描述 | 示例 |
| --- | --- | --- |
| SUM(expression) | 返回指定字段的总和 | SELECT SUM(money) FROM account; |
| AVG(expression) | 返回一个表达式的平均值，expression 是一个字段 | SELECT AVG(money) FROM account; |
| COUNT(expression) | 返回查询的记录总数，expression 参数是一个字段或者 * 号 | SELECT COUNT(*) FROM account; |
| MAX(expression) | 返回字段 expression 中的最大值 | SELECT MAX(money) FROM account; |
| MIN(expression) | 返回字段 expression 中的最小值 | SELECT MIN(money) FROM account; |

