---
id: failed-to-operate-table
sidebar_position: 9.5
---

# 无法操作StoneDB表中数据

StoneDB对部分DML操作是有限制的，如执行delete会有如下报错，这是因为StoneDB不支持delete。
```
ERROR 1031 (HY000): Table storage engine for 'xxx' doesn't have this option
```
执行replace into、update多表关联、update关联子查询操作时，虽然返回成功了（有"Warnings"提示），但查询数据时却发现数据没有被更新。执行"show warnings"，有如下提示。
```sql
mysql> show warnings;
+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Level | Code | Message                                                                                                                                                                                                                                                                                                                                                                          |
+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Note  | 1592 | Unsafe statement written to the binary log using statement format since BINLOG_FORMAT = STATEMENT. Statements writing to a table with an auto-increment column after selecting from another table are unsafe because the order in which rows are retrieved determines what (if any) rows will be written. This order cannot be predicted and may differ on master and the slave. |
+-------+------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
```
这是因为StoneDB只支持statement的binlog格式，执行以上三种DML操作会被认为是不安全的操作。如果有主从，可能会导致主从数据不一致。
