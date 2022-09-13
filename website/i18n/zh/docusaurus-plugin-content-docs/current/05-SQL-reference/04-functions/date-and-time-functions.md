---
id: date-and-time-functions
sidebar_position: 6.41
---

# 日期与时间函数

| **函数名** | **描述** | **示例** |
| --- | --- | --- |
| ADDDATE(d,n) | 计算起始日期 d 加上 n 天的日期 | SELECT ADDDATE('2022-06-10', INTERVAL 5 DAY);<br />->2022-06-15 |
| ADDTIME(t,n) | n 是一个时间表达式，时间 t 加上时间表达式 n | SELECT ADDTIME('2022-06-10 10:00:00',5);<br />->2022-06-10 10:00:05 |
| CURDATE()<br />CURRENT_DATE() | 返回当前日期 | SELECT CURDATE();<br />->2022-06-10 |
| CURRENT_TIME<br />CURTIME() | 返回当前时间 | SELECT CURRENT_TIME();<br />->17:10:31 |
| CURRENT_TIMESTAMP()<br />LOCALTIME()<br />LOCALTIMESTAMP()<br />NOW()<br />SYSDATE() | 返回当前日期和时间 | SELECT CURRENT_TIMESTAMP();<br />->2022-06-10 17:11:06 |
| DATE() | 从日期或日期时间表达式中提取日期值 | SELECT DATE('2022-06-10');    <br />->2022-06-10 |
| DATEDIFF(d1,d2) | 计算日期 d1 与 d2 之间相隔的天数 | SELECT DATEDIFF('2022-06-10','2021-06-10');<br />->365 |
| DATE_ADD(d，INTERVAL expr type) | 计算起始日期 d 加上一个时间段后的日期，type 可以是second、minute、hour、day、week、month、year 等 | SELECT DATE_ADD('2022-06-10 17:17:21', INTERVAL -3 HOUR); <br />->2022-06-10 14:17:21 |
| DATE_FORMAT(d,f) | 按表达式 f 的要求显示日期 d | SELECT DATE_FORMAT('2022-06-10 17:21:11','%Y-%m-%d %r');<br />->2022-06-10 05:21:11 PM |
| DATE_SUB(date,INTERVAL expr type) | 函数从日期减去指定的时间间隔 | SELECT DATE_SUB(<br />CURRENT_DATE(),INTERVAL 2 DAY);<br />->2022-06-08 |
| DAY(d) | 返回日期值 d 的日期部分 | SELECT DAY('2022-06-10');  <br />->10 |
| DAYNAME(d) | 返回日期 d 是星期几，如 Monday,Tuesday | SELECT DAYNAME('2022-06-10 17:30:30');<br />->Friday |
| DAYOFMONTH(d) | 计算日期 d 是本月的第几天 | SELECT DAYOFMONTH('2022-06-10 17:31:11');<br />->10 |
| DAYOFWEEK(d) | 计算日期 d 是星期几，1 表示星期日，2 表示星期一，以此类推 | SELECT DAYOFWEEK('2022-06-10 17:35:11');<br />->6 |
| DAYOFYEAR(d) | 计算日期 d 是本年的第几天 | SELECT DAYOFYEAR('2022-06-10 18:02:11');<br />->161 |
| EXTRACT(type FROM d) | 从日期 d 中获取指定的值，type 指定返回的值。type 可以是second、minute、hour、day、week、month、year 等 | SELECT EXTRACT(MONTH FROM '2022-06-10 18:02:33') ;<br />->6 |
| HOUR(t) | 返回 t 中的小时值 | SELECT HOUR('18:06:31');<br />->18 |
| LAST_DAY(d) | 返回给定日期的月份的最后一天 | SELECT LAST_DAY("2022-06-10");<br />->2022-06-30 |
| MAKEDATE(year, day-of-year) | 返回给定参数年份 year 和所在年中的天数序号，返回的是一个日期 | SELECT MAKEDATE(2022,161);<br />->2022-06-10 |
| MAKETIME(hour, minute, second) | 组合时间，参数分别为小时、分钟、秒 | SELECT MAKETIME(11,35,4);<br />->11:35:04 |
| MICROSECOND(date) | 返回日期参数所对应的微秒数 | SELECT MICROSECOND('2022-06-10 18:12:00.000023');<br />->23 |
| MINUTE(t) | 返回 t 中的分钟值 | SELECT MINUTE('18:12:31');<br />->12 |
| MONTHNAME(d) | 返回日期当中的月份，如 November | SELECT MONTHNAME('2022-06-10 18:13:19');<br />->June |
| MONTH(d) | 返回日期 d 中的月份值 | SELECT MONTH('2022-06-10 18:14:11');<br />->6 |
| PERIOD_ADD(period, number) | 给 年-月 组合日期添加一个时段 | SELECT PERIOD_ADD(202206,5);   <br />->202211 |
| PERIOD_DIFF(period1, period2) | 返回两个时段之间的月份差值 | SELECT PERIOD_DIFF(202204,202012);<br />->16 |
| QUARTER(d) | 返回日期 d 是第几季节 | SELECT QUARTER('2022-06-10 18:16:29');<br />->2 |
| SECOND(t) | 返回 t 中的秒钟值 | SELECT SECOND('18:17:36');<br />->36 |
| SEC_TO_TIME(s) | 以秒为单位的时间 s 转换为时分秒的格式 | SELECT SEC_TO_TIME(4320);<br />->01:12:00 |
| STR_TO_DATE(string, format_mask) | 将字符串转变为日期 | SELECT STR_TO_DATE('June 10 2022','%M %d %Y');<br />->2022-06-10 |
| SUBDATE(d,n) | 日期 d 减去 n 天后的日期 | SELECT SUBDATE('2022-06-10 18:19:27',15);<br />->2022-05-26 18:19:27 |
| SUBTIME(t,n) | 时间 t 减去 n 秒的时间 | SELECT SUBTIME('2022-06-10 18:21:11',5);<br />->2022-06-10 18:21:06 |
| TIME(expression) | 提取传入表达式的时间部分 | SELECT TIME('18:22:10');<br />->18:22:10 |
| TIME_FORMAT(t,f) | 按表达式 f 的要求显示时间 t | SELECT TIME_FORMAT('18:22:59','%r');<br />06:22:59 PM |
| TIME_TO_SEC(t) | 将时间 t 转换为秒 | SELECT TIME_TO_SEC('18:24:00');<br />->66240 |
| TIMEDIFF(time1, time2) | 计算时间差值 | SELECT TIMEDIFF('18:24:11','13:10:10');<br />->05:14:01 |
| TIMESTAMP(expression, interval) | 单个参数时，返回日期时间表达式，2个参数时，返回日期时间表达式和参数 | SELECT TIMESTAMP('2022-06-10',  '18:25:17');<br />->2022-06-10 18:25:17 |
| TIMESTAMPDIFF(unit,datetime_expr1,datetime_expr2) | 计算时间差，返回 datetime_expr2 − datetime_expr1 的时间差 | SELECT TIMESTAMPDIFF(DAY,'2020-12-23','2022-04-02');<br />->465<br />SELECT TIMESTAMPDIFF(MONTH,'2020-12-23','2022-04-02'); <br /> ->15 |
| TO_DAYS(d) | 计算日期 d 距离 0000 年 1 月 1 日的天数 | SELECT TO_DAYS('2022-06-10 00:00:00');<br />->738681 |
| WEEK(d) | 计算日期 d 是本年的第几个星期，范围是 0 到 53 | SELECT WEEK('2022-06-10 00:00:00');<br />->23 |
| WEEKDAY(d) | 日期 d 是星期几，0 表示星期一，1 表示星期二 | SELECT WEEKDAY('2022-06-10');<br />->4 |
| WEEKOFYEAR(d) | 计算日期 d 是本年的第几个星期，范围是 0 到 53 | SELECT WEEKOFYEAR('2022-06-10 11:11:11');<br />->23 |
| YEAR(d) | 返回年份 | SELECT YEAR('2022-06-10');<br />->2022 |
| YEARWEEK(date, mode) | 返回年份及第几周（0到53），mode 中 0 表示周日，1 表示周一，以此类推 | SELECT YEARWEEK('2022-06-10');<br />->202223 |