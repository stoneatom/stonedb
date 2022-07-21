---
id: mathematical-functions
sidebar_position: 6.43
---

# Mathematical Functions
This topic describes the mathematical functions supported by StoneDB.

| **Function** | **Description** | **Example** |
| --- | --- | --- |
| ABS(x) | Returns the absolute value. | SELECT ABS(-1);<br /><br />->1 |
| ACOS(x) | Returns the arc cosine of numeral value _x_. | SELECT ACOS(0.5);<br /><br />->1.0471975511965979 |
| ASIN(x) | Returns the arc sine of numeral value _x_. | SELECT ASIN(0.5);<br /><br />->0.5235987755982989 |
| ATAN(x) | Returns the arc tangent of numeral value _x_. | SELECT ATAN(2.5);<br /><br />->1.1902899496825317 |
| ATAN2(n, m) | Returns the arc tangent of two numeral values. | SELECT ATAN2(1,2);<br /><br />->0.4636476090008061 |
| COS(x) | Returns the cosine of radian _x_. | SELECT COS(2);<br /><br />->-0.4161468365471424 |
| COT(x) | Returns the cotangent of radian _x_. | SELECT COT(2);<br /><br />->-0.45765755436028577 |
| TAN(x) | Returns the tangent of radian _x_. | SELECT TAN(45); <br /><br />->1.6197751905438615 |
| SIN(x) | Returns the sine of radian _x_. | SELECT SIN(RADIANS(30));<br /><br />->0.49999999999999994 |
| CEIL(x)<br />CEILING(x) | Returns the smallest integer value that is not smaller than _x_. | SELECT CEIL(4.19);<br /><br />->5 |
| DEGREES(x) | Converts radian _x_ to a degree. | SELECT DEGREES(3.1415926535898);<br /><br />->180.0000000000004 |
| EXP(x) | Returns the natural exponential of _x_. | SELECT EXP(2);<br /><br />->7.38905609893065 |
| FLOOR(x) | Returns the largest integer value that is not greater than _x_. | SELECT FLOOR(3.24);<br /><br />->3 |
| GREATEST(expr1, expr2, expr3, ...) | Returns the greatest value within the specified list. | SELECT GREATEST(79,36,3,8,1);<br /><br />->79<br /><br />SELECT GREATEST('Hello','CHINA','STONEDB'); <br /><br />->STONEDB |
| LEAST(expr1, expr2, expr3, ...) | Returns the smallest value within the specified list. | SELECT LEAST(79,36,3,8,1);<br />->1<br /><br />SELECT LEAST('Hello','CHINA','STONEDB'); <br />->CHINA |
| LN(x) | Returns the natural logarithm of _x_. | SELECT LN(3); <br /><br />->1.0986122886681098 |
| LOG(x,y) | Returns the base-x logarithm of_ y_. | SELECT LOG(3,81);<br /><br />->4 |
| LOG2(x) | Returns the base-2 logarithm of_ x_. | SELECT LOG2(64); <br /><br />->6 |
| PI() | Returns the value of pi. | SELECT PI();<br /><br />->3.141593 |
| POW(x,y)，<br />POWER(x,y) | Returns _x_ raised to the specified power of _y_. | SELECT POW(2,4);<br /><br />->16 |
| RAND() | Returns a random number between 0 and 1. | SELECT RAND();<br /><br />->0.12216221831940322 |
| ROUND(x) | Returns the value of _x_ rounded to the nearest integer. | SELECT ROUND(-5.26);<br /><br />->-5 |
| SIGN(x) | Returns the sign of _x_. <br />- If _x_ is a negative value, **-1** is returned. <br />- If_ x_ is 0, **0** is returned. <br />- If _x_ is a positive value, **1** is returned.<br /> | SELECT SIGN(-10);<br /><br />->(-1) |
| SQRT(x) | Returns the square root of _x_. | SELECT SQRT(81);<br /><br />->9 |
| TRUNCATE(x,y) | Truncates _x_ to retain _y_ decimal places. | SELECT TRUNCATE(2.2849106,3);<br /><br />->2.284 |

