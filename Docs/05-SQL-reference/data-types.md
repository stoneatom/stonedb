---
id: data-types
sidebar_position: 6.2
---

# Data Types

The following table lists the data types supported by StoneDB.

| **Category** | **Data type** |
| --- | --- |
| Integer | TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT |
| Floating point | FLOAT, DOUBLE |
| Fixed point | DECIMAL |
| Date and time | YEAR, TIME, DATE, DATETIME, TIMESTAMP |
| String | CHAR, VARCHAR, TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT |
| Binary string | BINARY, VARBINARY, TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB |

# Integer data types
The following table provides the value range of each integer data type.

| **Data type** | **Bytes of storage** | **Signed** | **Min. value** | **Max. value** |
| --- | --- | --- | --- | --- |
| TINYINT | 1 | Yes | -128 | 127 |
|  |  | No | 0 | 127 |
| SMALLINT | 2 | Yes | -32768 | 32767 |
|  |  | No | 0 | 32767 |
| MEDIUMINT | 3 | Yes | -8388608 | 8388607 |
|  |  | No | 0 | 8388607 |
| INT | 4 | Yes | -2147483647 | 2147483647 |
|  |  | No | 0 | 2147483647 |
| BIGINT | 8 | Yes | -9223372036854775806 | 9223372036854775807 |
|  |  | No | 0 | 9223372036854775807 |

On StoneDB, the precision for DECIMAL numbers cannot be higher than 18. For example, if you specify **decimal(19)** in your code, an error will be reported. **DECIMAL(6, 2)** indicates that up to 6 places are supported at the left of the decimal and up to 2 at the right, and thus the value range is [-9999.99, 9999.99].
# String data types
The storage required for a string varies according to the character set in use. The length range also differs. The following table describes the length range of each string data type when character set latin1 is in use.

| **Data type** | **Size** |
| --- | --- |
| CHAR(M) | [0, 255] |
| VARCHAR(M) | [0, 65535] |
| TINYTEXT | [0, 255] |
| TEXT | [0, 65535] |
| MEDIUMTEXT | [0, 16777215] |
| LONGTEXT | [0, 4294967295] |

# Date and time data types
The following table describes the value range of each date and time data type.

| **Data type** | **Format** | **Min. value** | **Max. value** |
| --- | --- | --- | --- |
| YEAR | YYYY | 1901 | 2155 |
| TIME | HH:MM:SS | -838:59:59 | 838:59:59 |
| DATE | YYYY-MM-DD | 0000-00-00 | 9999-12-31 |
| DATETIME | YYYY-MM-DD HH:MM:SS | 0000-00-00 00:00:00 | 9999-12-31 23:59:59 |
| TIMESTAMP | YYYY-MM-DD HH:MM:SS | 1970-01-01 08:00:01 | 2038-01-19 11:14:07 |