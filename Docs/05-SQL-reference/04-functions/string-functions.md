---
id: string-functions
sidebar_position: 6.42
---

# String Functions

This topic describes the string functions supported by StoneDB.

| **Function** | **Description** | **Example** |
| --- | --- | --- |
| ASCII(s) | Returns the numeric value of the leftmost character of string _s_. | select ASCII('CHINA');<br /><br />->67 |
| CHAR_LENGTH(s)<br />CHARACTER_LENGTH(s) | Returns the number of characters in string _s_. | SELECT CHAR_LENGTH('CHINA');<br /><br />->5 |
| CONCAT(s1,s2...sn)<br />CONCAT_WS(x, s1,s2...sn) | Concatenates strings _s1_, _s2_, … _Sn_. | SELECT CONCAT('Welcome to ','CHINA');<br /><br />->Welcome to CHINA  |
| FIELD(s,s1,s2...) | Returns the index of the first string _s_ in the list of subsequent strings (_s1_, _s2_, …). | SELECT FIELD("c","a","b","c","d","e");<br /><br />->3 |
| FIND_IN_SET(s1,s2) | Returns the index of the first string _s1_ within the second string _s2_. | SELECT FIND_IN_SET("c","a,b,c,d,e");<br /><br />->3 |
| FORMAT(x,n) | Returns a number formatted to _n_ decimal places. <br />_x_ specifies the number to format. The return value is in the #,###.## format. | SELECT FORMAT(9105885500.534,2); <br /><br />->9,105,885,500.53 |
| INSERT(s1,x,len,s2) | Inserts substring _s2_ at a given position up to the specified number of characters in string _s1_. <br />_x_ specifies position to insert. _len_ specifies the number of characters. | SELECT INSERT('Welcome to CHINA',1,6,'I love');<br /><br />->I lovee to CHINA  |
| LOCATE(s1,s) | Returns the position of the first occurrence of substring s1 in string s. | SELECT LOCATE('db','stonedb'); <br /><br />->6 |
| LCASE(s)<br />LOWER(s) | Returns every character in string _s_ in lowercase. | SELECT LCASE('STONEDB');<br /><br />->stonedb | 
| LEFT(s,n) | Returns the leftmost n characters in string_ s_. | SELECT LEFT('stonedb',5);<br /><br />->stone |
| LPAD(s1,len,s2) | Left-pads string _s2_ with string _s1_ to the specified length _len_. | SELECT LPAD('one',5,'st');<br /><br />->stone |
| LTRIM(s) | Remove leading spaces in string _s_. | SELECT LTRIM(' STONEDB');<br /><br />->STONEDB |
| MID(s,n,len) | Returns a substring of string s starting from a given position. <br />_n_ specifies the position. _len_ specifies the length of the substring. This function is the Synonym for SUBSTRING(s,n,len). | SELECT MID('stonedb',2,3);<br /><br />->ton |
| POSITION(s1 IN s) | Returns the position of the first occurrence of substring _s1_ in string _s_. | SELECT POSITION('db'in'stonedb');<br /><br />->6 |
| REPEAT(s,n) | Repeats string _s_ for _n_ times. | SELECT REPEAT('hello',3);<br /><br />->hellohellohello |
| REPLACE(s,s1,s2) | Replaces substring _s1_ in string _s_ with substring _s2_. | SELECT REPLACE('stonedb','db','x');<br /><br />->stonex |
| REVERSE(s) | Reverses the characters in string _s_. | SELECT REVERSE('stonedb');<br /><br />->bdenots |
| RIGHT(s,n) | Returns _n_ rightmost characters in string _s_. | SELECT RIGHT('stonedb',5);<br /><br />->onedb |
| RPAD(s1,len,s2) | Right-pads string _s2_ to string _s1_ to the specified length _len_. | SELECT RPAD('stone',7,'db');<br /><br />->stonedb |
| RTRIM(s) | Remove trailing spaces in string _s_. | SELECT RTRIM('STONEDB ');<br /><br />->STONEDB |
| SPACE(n) | Returns _n_ spaces. | SELECT SPACE(10);<br /><br />->           |
| STRCMP(s1,s2) | Compares the lengths of strings _s1_ and_ s2_. <br />- If _s1_ = _s2_, **0** is returned. <br />- If _s1_ > _s2_, **1** is returned. <br />- If _s1_ < _s2_, **-1** is returned.<br /> | SELECT STRCMP('stonedb','stone'); <br /><br />->1 |
| SUBSTR(s, start, length) | Returns a substring of the specified length within string _s_. <br />_start_ specifies the position from which the substring starts. | SELECT SUBSTR('STONEDB',2,3);<br /><br />->TON |
| SUBSTRING_INDEX(s, delimiter, number) | Returns a substring from string _s_ before the specified number of occurrences of the delimiter. | SELECT SUBSTRING_INDEX('stonedb','n',1);<br /><br />->sto |
| TRIM(s) | Removes leading and trailing spaces in string _s_. | SELECT TRIM(' STONEDB ');<br /><br />->STONEDB |
| UCASE(s)<br />UPPER(s) | Returns every character in string _s_ in uppercase. | SELECT UCASE('stonedb');<br /><br />->STONEDB |
