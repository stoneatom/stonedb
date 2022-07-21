---
id: failed-to-operate-table
sidebar_position: 9.5
---

# Failed to Operate on Data in StoneDB Tables

StoneDB has limits on some DML operations. For example, the following error is returned if a DELETE operation is performed because StoneDB does not support DELETE operations.
```
ERROR 1031 (HY000): Table storage engine for 'xxx' doesn't have this option
```
In addition, StoneDB does not support following operations:

- Execute an REPLACE… INTO statement.
- Use subqueries in an UPDATE statement. 
- Execute an UPDATE… JOIN statement to update multiple tables.

If you perform any of them, the system output indicates that the operation is successful. However, if you query relevant data, the data is not updated. You can execute `SHOW WARNINGS`, and the following warning information will be displayed.
```sql
mysql> show warnings;
+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Level | Code | Message                                                                                                                                                                                                                                                                                                                                                                          |
+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Note  | 1592 | Unsafe statement written to the binary log using statement format since BINLOG_FORMAT = STATEMENT. Statements writing to a table with an auto-increment column after selecting from another table are unsafe because the order in which rows are retrieved determines what (if any) rows will be written. This order cannot be predicted and may differ on master and the slave. |
+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
StoneDB supports only statement-based binlog. For this reason, the three DML operations are identified as insecure. If StoneDB is deployed in active/standby architecture, these operations may also cause data inconsistency.
