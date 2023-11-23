---
id: string-functions
sidebar_position: 6.42
---

# 字符串函数
| **函数** | **描述** | **示例** |
| --- | --- | --- |
| ASCII(s) | 返回字符串 s 的第一个字符的 ASCII 码 | select ASCII('CHINA');<br />->67 |
| CHAR_LENGTH(s)<br />CHARACTER_LENGTH(s) | 返回字符串 s 的字符数 | SELECT CHAR_LENGTH('CHINA');<br />->5 |
| CONCAT(s1,s2...sn)<br />CONCAT_WS(x, s1,s2...sn) | 字符串 s1,s2 等多个字符串合并为一个字符串 | SELECT CONCAT('Welcome to ','CHINA');<br />->Welcome to CHINA  |
| FIELD(s,s1,s2...) | 返回第一个字符串 s 在字符串列表(s1,s2...)中的位置 | SELECT FIELD("c","a","b","c","d","e");<br />->3 |
| FIND_IN_SET(s1,s2) | 返回在字符串 s2 中与 s1 匹配的字符串的位置 | SELECT FIND_IN_SET("c","a,b,c,d,e");<br />->3 |
| FORMAT(x,n) | 函数可以将数字 x 进行格式化 "#,###.##", 将 x 保留到小数点后 n 位，最后一位四舍五入 | SELECT FORMAT(9105885500.534,2); <br />->9,105,885,500.53 |
| INSERT(s1,x,len,s2) | 字符串 s2 替换 s1 的 x 位置开始长度为 len 的字符串 | SELECT INSERT('Welcome to CHINA',1,6,'I love');<br />->I lovee to CHINA  |
| LOCATE(s1,s) | 从字符串 s 中获取 s1 的开始位置 | SELECT LOCATE('db','stonedb'); <br />->6 |
| LCASE(s)<br />LOWER(s) | 将字符串 s 的所有字母变成小写字母 | SELECT LCASE('STONEDB');<br />->stonedb |
| LEFT(s,n) | 返回字符串 s 的前 n 个字符 | SELECT LEFT('stonedb',5);<br />->stone |
| LPAD(s1,len,s2) | 在字符串 s1 的开始处填充字符串 s2，使字符串长度达到 len | SELECT LPAD('one',5,'st');<br />->stone |
| LTRIM(s) | 去掉字符串 s 开始处的空格 | SELECT LTRIM('    STONEDB');<br />->STONEDB |
| MID(s,n,len) | 从字符串 s 的 n 位置截取长度为 len 的子字符串，同 SUBSTRING(s,n,len) | SELECT MID('stonedb',2,3);<br />->ton |
| POSITION(s1 IN s) | 从字符串 s 中获取 s1 的开始位置 | SELECT POSITION('db'in'stonedb');<br />->6 |
| REPEAT(s,n) | 将字符串 s 重复 n 次 | SELECT REPEAT('hello',3);<br />->hellohellohello |
| REPLACE(s,s1,s2) | 将字符串 s2 替代字符串 s 中的字符串 s1 | SELECT REPLACE('stonedb','db','x');<br />->stonex |
| REVERSE(s) | 将字符串s的顺序反过来 | SELECT REVERSE('stonedb');<br />->bdenots |
| RIGHT(s,n) | 返回字符串 s 的后 n 个字符 | SELECT RIGHT('stonedb',5);<br />->onedb |
| RPAD(s1,len,s2) | 在字符串 s1 的结尾处添加字符串 s2，使字符串的长度达到 len | SELECT RPAD('stone',7,'db');<br />->stonedb |
| RTRIM(s) | 去掉字符串 s 结尾处的空格 | SELECT RTRIM('STONEDB ');<br />->STONEDB |
| SPACE(n) | 返回 n 个空格 | SELECT SPACE(10);<br />-> |
| SUBSTR(s, start, length) | 从字符串 s 的 start 位置截取长度为 length 的子字符串 | SELECT SUBSTR('STONEDB',2,3);<br />->TON |
| STRCMP(s1,s2) | 比较字符串 s1 和 s2, 如果 s1 与 s2 相等返回 0, 如果 s1>s2 返回 1,如果 s1小于s2 返回 -1|SELECT STRCMP('stonedb','stone'); <br />-> 1|
| SUBSTRING_INDEX(s, delimiter, number) | 返回从字符串 s 的第 number 个出现的分隔符 delimiter 之后的子串 | SELECT SUBSTRING_INDEX('stonedb','n',1);<br />->sto |
| TRIM(s) | 去掉字符串 s 开始和结尾处的空格 | SELECT TRIM('    STONEDB    ');<br />->STONEDB |
| UCASE(s)<br />UPPER(s) | 将字符串转换为大写 | SELECT UCASE('stonedb');<br />->STONEDB |