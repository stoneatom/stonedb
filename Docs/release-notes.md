---
id: release-notes
sidebar_position: 11.0
---

# Release Notes

## StoneDB-5.7-V1.0.0
Changes in StoneDB_5.7_v1.0.0 (2022-08-31, General Availability)

-  Support for MySQL 5.7
-  Functionality Added or Changed
- Compilation Notes
- Configuration Notes
- Document Notes
- Bugs Fixed

Support for MySQL 5.7

- Important Change: The Tianmu engine supports MySQL 5.7，base line edition MySQL 5.7.36. This change applies to the StoneDB database which would be fully compatible with the MySQL 5.7 protocols.

Functionality Added or Changed

- Important Change: The engine name  has been changed to tianmu. StoneDB is used as a HTAP database name, it is not suitable for used both as an engine name. As the dissusion result, we choose tianmu as our new eninge name: 

```bash
mysql> show engines;
+----------------+---------+--------------------------+--------------+------+------------+
| Engine         | Support | Comment                  | Transactions | XA   | Savepoints |
+----------------+---------+--------------------------+--------------+------+------------+
| TIANMU         | DEFAULT | Tianmu storage engine    | YES          | NO   | NO         |
+----------------+---------+--------------------------+--------------+------+------------+
```
- Tianmu:  Improved  the aggregation capabilities of the decimal data type. 
- Tianmu:  Improved the readability of code. The code does not spererate logically, or the variables name can not be understooed by the literal meaning. To refactor code make it more readable to anyone who are the first to read that. For example: Changes int DoGetSomething(); to int GetSomethingXXX();, int GetNoNulls() to int GetNumOfNulls().

- Tianmu: The date-related functions (such as DATE_ADD, DATE_SUB)  can be queried (DATE_ADD|DATE_SUB) when  creating view using the date func.([BUG #342](https://github.com/stoneatom/stonedb/issues/342))

Compilation Notes

- The version of the Boost library for server builds is now 1.66.0.

- The version of the Rocksdb for server builds is now 6.12.6.

Configuration Notes

- Important Change: Changed default config file stonedb.cnf to my.cnf. ([feature #182](https://github.com/stoneatom/stonedb/issues/182))

- Important Change: Use tianmu as the default storage engine. ([feature #255](https://github.com/stoneatom/stonedb/issues/255))

Document Notes

- The manual has been updated as the code was modified. ( [# address](https://stonedb.io/))

Bugs Fixed

- fix mtr case: [BUG #78](https://github.com/stoneatom/stonedb/issues/78), [BUG #73](https://github.com/stoneatom/stonedb/issues/73),[ BUG #170](https://github.com/stoneatom/stonedb/issues/170), [BUG #192](https://github.com/stoneatom/stonedb/issues/192), [BUG #191](https://github.com/stoneatom/stonedb/issues/191), [BUG #227](https://github.com/stoneatom/stonedb/issues/227),  [BUG #245](https://github.com/stoneatom/stonedb/issues/245), BUG  #263

- fix Tianmu bug: [BUG #338](https://github.com/stoneatom/stonedb/issues/388),[ BUG #327](https://github.com/stoneatom/stonedb/issues/327), [BUG #212](https://github.com/stoneatom/stonedb/issues/212), [BUG #142](https://github.com/stoneatom/stonedb/issues/142)

## StoneDB-5.6-V1.0.0
Release time: June 30, 2022

