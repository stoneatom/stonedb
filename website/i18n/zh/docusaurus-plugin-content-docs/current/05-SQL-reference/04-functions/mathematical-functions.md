---
id: mathematical-functions
sidebar_position: 6.43
---

# 数学函数

| **函数名** | **描述** | **示例** |
| --- | --- | --- |
| ABS(x) | 求绝对值  | SELECT ABS(-1);<br />->1 |
|  |  | 
| ASIN(x) | 求反正弦值(单位为弧度)，x 为一个数值 | SELECT ASIN(0.5);<br />->0.5235987755982989 |
|  |  | 
| ATAN2(n, m) | 求反正切值(单位为弧度) | SELECT ATAN2(1,2);<br />->0.4636476090008061 |
| COS(x) | 求余弦值(参数是弧度) | SELECT COS(2);<br />->-0.4161468365471424 |
| COT(x) | 求余切值(参数是弧度) | SELECT COT(2);<br />->-0.45765755436028577 |
| TAN(x) | 求正切值(参数是弧度) | SELECT TAN(45); <br />->1.6197751905438615 |
| SIN(x) | 求正弦值(参数是弧度)  | SELECT SIN(RADIANS(30));<br />->0.49999999999999994 |
| CEIL(x)<br />CEILING(x) | 返回大于或等于 x 的最小整数  | SELECT CEIL(4.19);<br />->5 |
| DEGREES(x) | 将弧度转换为角度  | SELECT DEGREES(3.1415926535898);<br />->180.0000000000004 |
| EXP(x) | 返回 e 的 x 次方  | SELECT EXP(2);<br />->7.38905609893065 |
| FLOOR(x) | 返回小于或等于 x 的最大整数  | SELECT FLOOR(3.24);<br />->3 |
| GREATEST(expr1, expr2, expr3, ...) | 返回列表中的最大值 | SELECT GREATEST(79,36,3,8,1);<br />->79 |
|  |  | 
| LEAST(expr1, expr2, expr3, ...) | 返回列表中的最小值 | SELECT LEAST(79,36,3,8,1);<br />->1 |
|  |  | 
| LN | 返回数字的自然对数，以 e 为底 | SELECT LN(3); <br />->1.0986122886681098 |
| LOG(x,y) | 返回以 x 为低的对数 | SELECT LOG(3,81);<br />->4 |
| LOG2(x) | 返回以 2 为底的对数 | SELECT LOG2(64); <br />->6 |
|  |  | 
| PI() | 返回圆周率 | SELECT PI();<br />->3.141593 |
| POW(x,y)<br />POWER(x,y) | 返回 x 的 y 次方  | SELECT POW(2,4);<br />->16 |
| RAND() | 返回 0 到 1 的随机数  | SELECT RAND();<br />->0.12216221831940322 |
| ROUND(x) | 返回离 x 最近的整数 | SELECT ROUND(-5.26);<br />->-5 |
|  |  | 
| SQRT(x) | 返回 x 的平方根  | SELECT SQRT(81);<br />->9 |
| TRUNCATE(x,y) | 返回数值 x 保留到小数点后 y 位的值，与 ROUND 最大的区别是不会进行四舍五入 | SELECT TRUNCATE(2.2849106,3);<br />->2.284 |
|  |  | 
| ABS(x) | 求绝对值  | SELECT ABS(-1);<br />->1 |
|  |  | 
| ASIN(x) | 求反正弦值(单位为弧度)，x 为一个数值 | SELECT ASIN(0.5);<br />->0.5235987755982989 |
| ATAN(x) | 求反正切值(单位为弧度)，x 为一个数值 | SELECT ATAN(2.5);<br />->1.1902899496825317 |
| ATAN2(n, m) | 求反正切值(单位为弧度) | SELECT ATAN2(1,2);<br />->0.4636476090008061 |
| COS(x) | 求余弦值(参数是弧度) | SELECT COS(2);<br />->-0.4161468365471424 |
|  |  | 
| TAN(x) | 求正切值(参数是弧度) | SELECT TAN(45); <br />->1.6197751905438615 |



