---
id: aggregate-functions
sidebar_position: 6.45
---

# Aggregate Functions
This topic describes the aggregate functions supported by StoneDB.

| **Function** | **Description** | **Example** |
| --- | --- | --- |
| SUM(expression) | Returns the sum of values in field _expression_. | SELECT SUM(money) FROM account; |
| AVG(expression) | Returns the average value of field _expression_. | SELECT AVG(money) FROM account; |
| COUNT(expression) | Returns a count of the number of returned records. _expression_ can be set to a field or an asterisk (*). | SELECT COUNT(*) FROM account; |
| MAX(expression) | Returns the maximum value in field _expression_. | SELECT MAX(money) FROM account; |
| MIN(expression) | Returns the minimum value in field _expression_. | SELECT MIN(money) FROM account; |