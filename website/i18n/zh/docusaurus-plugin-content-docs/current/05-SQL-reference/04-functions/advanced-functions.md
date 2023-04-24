---
id: advanced-functions
sidebar_position: 6.44
---

# 高级函数

| **函数名** | **描述** | **示例** |
| --- | --- | --- |
| BIN(x) | 返回 x 的二进制编码 | SELECT BIN(28);<br />->11100 |
| BINARY(s) | 将字符串 s 转换为二进制字符串 | SELECT BINARY('STONEDB');<br />-> STONEDB |
| CASE expression<br />    WHEN condition1 THEN result1<br />    WHEN condition2 THEN result2<br />   ...<br />    WHEN conditionN THEN resultN<br />    ELSE result<br />END | CASE 表示函数开始，END 表示函数结束。如果 condition1 成立，则返回 result1, 如果 condition2 成立，则返回 result2，当全部不成立则返回 result，而当有一个成立之后，后面的就不执行了 | SELECT CASE <br /> WHEN 1>0<br /> THEN '1 > 0'<br /> WHEN 2>0<br /> THEN '2 > 0'<br /> ELSE '3 > 0'<br />END;<br />->1>0 |
| CAST(x AS type) | 转换数据类型 | SELECT CAST('2022-06-11' AS DATE);<br />->2022-06-11 |
| COALESCE(expr1, expr2, ...., expr_n) | 返回参数中的第一个非空表达式(从左向右) | SELECT COALESCE(NULL, NULL,'CHINA', NULL, NULL,'STONEDB');<br />->CHINA |
| CONNECTION_ID() | 返回当前线程 ID | SELECT CONNECTION_ID();<br />->5 |
| CONV(x,f1,f2) | 返回 f1 进制数变成 f2 进制数 | SELECT CONV(28,10,16);<br />->1C |
| CONVERT(s USING cs) | 函数将字符串 s 的字符集变成 cs | SELECT CHARSET('ABC');<br />->utf8 <br /><br />SELECT CHARSET(CONVERT('ABC' USING gbk));<br />->gbk |
| CURRENT_USER() | 返回当前用户 | SELECT CURRENT_USER();<br />->root@localhost |
| DATABASE() | 返回当前数据库名 | SELECT DATABASE();   <br />->test |
| IF(expr,v1,v2) | 如果表达式 expr 成立，返回结果 v1；否则返回结果 v2 | SELECT IF(1>10,'true','false') ;<br />->false |
| IFNULL(v1,v2) | 如果 v1 的值不为 NULL，则返回 v1，否则返回 v2 | SELECT IFNULL(null,'Hello Word');<br />->HelloWord |
| ISNULL(expression) | 判断表达式是否为 NULL | SELECT ISNULL(NULL);<br />->1 |
| LAST_INSERT_ID() | 返回最近生成的 AUTO_INCREMENT 值 | SELECT LAST_INSERT_ID();<br />->0 |
| NULLIF(expr1, expr2) | 比较两个字符串，如果字符串 expr1 与 expr2 相等，返回 NULL，否则返回 expr1 | SELECT NULLIF(25,25);<br />->NULL |
| SESSION_USER()<br />SYSTEM_USER()<br />USER()| 返回当前用户 | SELECT SESSION_USER();<br />->root@localhost |
| VERSION() | 返回数据库的版本号 | SELECT VERSION();<br />->5.6.24-StoneDB-log |