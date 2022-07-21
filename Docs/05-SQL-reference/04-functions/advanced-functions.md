---
id: advanced-functions
sidebar_position: 6.44
---

# Advanced Functions

This topic describes the advanced functions supported by StoneDB.

| **Function** | **Description** | **Example** |
| --- | --- | :-- |
| BIN(x) | Returns the binary string equivalent to _x_. | SELECT BIN(28);<br /><br />->11100 |
| BINARY(s) | Converts string _s_ to a binary string. | SELECT BINARY('STONEDB');<br /><br />-> STONEDB |
| CASE expression<br />WHEN condition1 THEN result1<br />WHEN condition2 THEN result2<br />...<br />WHEN conditionN THEN resultN<br />ELSE result<br />END | **CASE** specifies the start of the function and **END** specifies the end of the function. <br />If _condition1_ is met, _result1_ is returned. If _condition2_ is met, _result2_ is returned. If all conditions are not met, _result_ is returned. <br />This function stops checking subsequent conditions immediately after a condition is met and returns the corresponding result. | SELECT CASE <br />WHEN 1>0<br />THEN '1 > 0'<br />WHEN 2>0<br />THEN '2 > 0'<br />ELSE '3 > 0'<br />END;<br /><br />->1>0 |
| CAST(x AS type) | Converts the data type of _x_. | SELECT CAST('2022-06-11' AS DATE);<br /><br />->2022-06-11 |
| COALESCE(expr1, expr2, ...., expr_n) | Returns the first non-null value in the specified list. | SELECT COALESCE(NULL, NULL,'CHINA', NULL, NULL,'STONEDB');<br /><br />->CHINA |
| CONNECTION_ID() | Returns the ID of the current connection. | SELECT CONNECTION_ID();<br /><br />->5 |
| CONV(x,f1,f2) | Converts _x_ from base _f1_ to_ f2_. | SELECT CONV(28,10,16);<br /><br />->1C |
| CONVERT(s USING cs) | Changes the character set of string _s_ to character set _cs_. | SELECT CHARSET('ABC');<br /><br />->utf8 <br /><br />SELECT CHARSET(CONVERT('ABC' USING gbk));<br /><br />->gbk |
| CURRENT_USER() | Returns the current user. | SELECT CURRENT_USER();<br /><br />->root@localhost |
| DATABASE() | Returns the name of the current database. | SELECT DATABASE(); <br /><br />->test |
| IF(expr,v1,v2) | Returns value _v1_ if expression _expr_ is true or value _v2_ if expression _expr_ is false. | SELECT IF(1>10,'true','false') ;<br /><br />->false |
| IFNULL(v1,v2) | Returns value _v1_ if value _v1_ is not **null**. Otherwise, value _v2_ is returned. | SELECT IFNULL(null,'Hello Word');<br /><br />->HelloWord |
| ISNULL(expression) | Checks whether _expression_ is **NULL**. | SELECT ISNULL(NULL);<br /><br />->1 |
| LAST_INSERT_ID() | Returns the last AUTO_INCREMENT value. | SELECT LAST_INSERT_ID();<br /><br />->0 |
| NULLIF(expr1, expr2) | Compares two strings _expr1_ and _expr2_.<br />If they are the same, **NULL** is returned. Otherwise, _expr1_ is returned. | SELECT NULLIF(25,25);<br /><br />->NULL |
| SESSION_USER()<br />SYSTEM_USER()<br />USER() | Returns the current user. | SELECT SESSION_USER();<br /><br />->root@localhost |
| VERSION() | Returns the version number of the database. | SELECT VERSION();<br /><br />->5.6.24-StoneDB-log |