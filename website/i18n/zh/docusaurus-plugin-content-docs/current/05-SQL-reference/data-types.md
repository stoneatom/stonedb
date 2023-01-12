---
id: data-types
sidebar_position: 6.2
---

# 数据类型

StoneDB 支持如下的数据类型。

| 类型 | 成员 |
| --- | --- |
| 整型 | TINYINT、SMALLINT、MEDIUMINT、INT、BIGINT |
| 浮点型 | FLOAT、DOUBLE |
| 定点型 | DECIMAL |
| 日期时间型 | YEAR、TIME、DATE、DATETIME、TIMESTAMP |
| 字符串型 | CHAR、VARCHAR、TINYTEXT、TEXT、MEDIUMTEXT、LONGTEXT |
| 二进制字符串型 | BINARY、VARBINARY、TINYBLOB、BLOB、MEDIUMBLOB、LONGBLOB |

各整型的取值范围如下。

| 类型 | 字节 | 有/无符号 | 最小值 | 最大值 |
| --- | --- | --- | --- | --- |
| TINYINT | 1 | 有符号 | -128 | 127 |
|  |  | 无符号 | 0 | 127 |
| SMALLINT | 2 | 有符号 | -32768 | 32767 |
|  |  | 无符号 | 0 | 32767 |
| MEDIUMINT | 3 | 有符号 | -8388608 | 8388607 |
|  |  | 无符号 | 0 | 8388607 |
| INT | 4 | 有符号 | -2147483647 | 2147483647 |
|  |  | 无符号 | 0 | 2147483647 |
| BIGINT | 8 | 有符号 | -9223372036854775808 | 9223372036854775807 |
|  |  | 无符号 | 0 | 18446744073709551615 |

DECIMAL 精度必须小于或等于18，否则不支持，如 decimal(19) 就会报错。DECIMAL(6, 2) 表示整数部分和小数部分最大有效位数分别为4和2，所以值域为 [-9999.99, 9999.99]。<br />不同的字符集，即使长度相同，但占用的存储空间不同，以下是以字符集为 latin1 的字符串类型的大小范围。

| 类型 | 大小 |
| --- | --- |
| CHAR(M) | [0,255] |
| VARCHAR(M) | [0,65535] |
| TINYTEXT | [0,255] |
| TEXT | [0,65535] |
| MEDIUMTEXT | [0,16777215] |
| LONGTEXT | [0,4294967295] |

日期类型的日期范围如下。

| 类型 | 日期格式 | 最小值 | 最大值 |
| --- | --- | --- | --- |
| YEAR | YYYY | 1901 | 2155 |
| TIME | HH:MM:SS | -838:59:59 | 838:59:59 |
| DATE | YYYY-MM-DD | 0000-00-00 | 9999-12-31 |
| DATETIME | YYYY-MM-DD HH:MM:SS | 0000-00-00 00:00:00 | 9999-12-31 23:59:59 |
| TIMESTAMP | YYYY-MM-DD HH:MM:SS | 1970-01-01 08:00:01 | 2038-01-19 11:14:07 |
