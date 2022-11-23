---
id: error-codes
sidebar_position: 5.72
---

# Error Codes

This topic describes the common error codes that may be returned in StoneDB.


| **Error code** | **Error message** | **Description** |
| --- | --- | --- |
| 2233 (HY000) | Be disgraceful to storage engine, operating is forbidden! | The error message is returned because the DDL operation is not supported. |
| 1031 (HY000) | Table storage engine for 'xxx' doesn't have this option | The error message is because the DML operation is not supported. |
| 1040 (HY000) | Too many connections | The error message is because the number of connections has reached the maximum number. |
| 1045 (28000) | Access denied for user 'u_test'@'%' (using password: YES) | The error message is because the username or password is incorrect or the permissions are insufficient. |

