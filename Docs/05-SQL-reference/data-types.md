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

Type | Storage (Bytes) | Minimum Value Signed | Minimum Value Unsigned | Maximum Value Signed | Maximum Value Unsigned
-- | -- | -- | -- | -- | --
TINYINT | 1 | -128 | 0 | 127 | 255
SMALLINT | 2 | -32768 | 0 | 32767 | 65535
MEDIUMINT | 3 | -8388608 | 0 | 8388607 | 16777215
INT | 4 | -2147483647 | 0 | 2147483647 | 4294967295
BIGINT  | 8 | -9223372036854775806 | 0 | 9223372036854775807 | 9223372036854775807

**Notice**:

For `INT` type: `-2147483648` is reserved to indicate `INT_NULL` in `Tianmu` engine, `Minimum Value Signed` start from -2147483647.

For `BIGINT` type: `-9223372036854775807` is reserved to indicate `BIGINT_NULL` in `Tianmu`, `Maximum Value Signed` start from -9223372036854775806.

For `BIGINT` type: Maximum Value Unsigned currently limited to 9223372036854775807, we'll expand it to 18446744073709551615 in the future.

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
