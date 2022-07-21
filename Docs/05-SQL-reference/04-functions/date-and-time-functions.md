---
id: date-and-time-functions
sidebar_position: 6.41
---

# Date and Time Functions

This topic describes the date and time functions supported by StoneDB.

This topic describes the date and time functions supported by StoneDB.

| **Function** | **Description** | **Example** |
| --- | --- | --- |
| ADDDATE(d,n) | Adds time value (interval) _n_ to date _d_. | SELECT ADDDATE('2022-06-10', INTERVAL 5 DAY);<br /><br />->2022-06-15 |
| ADDTIME(t,n) | Adds time _n_ to time _t_. | SELECT ADDTIME('2022-06-10 10:00:00',5);<br /><br />->2022-06-10 10:00:05 |
| CURDATE()<br />CURRENT_DATE() | Returns the current date. | SELECT CURDATE();<br /><br />->2022-06-10 |
| CURRENT_TIME<br />CURTIME() | Returns the current time. | SELECT CURRENT_TIME();<br /><br />->17:10:31 |
| CURRENT_TIMESTAMP()<br />LOCALTIME()<br />LOCALTIMESTAMP()<br />NOW()<br />SYSDATE() | Returns the current date and time. | SELECT CURRENT_TIMESTAMP();<br /><br />->2022-06-10 17:11:06 |
| DATE() | Extracts the date part of a date or datetime expression. | SELECT DATE('2022-06-10');    <br /><br />->2022-06-10 |
| DATEDIFF(d1,d2) | Subtracts two dates. _d1_ and _d2_ each specify a date. | SELECT DATEDIFF('2022-06-10','2021-06-10');<br /><br />->365 |
| DATE_ADD(d,INTERVAL expr type) | Add time values (intervals) to date _d_. <br />_type_ can be set to **SECOND**, **MINUTE**, **HOUR**, **DAY**, **WEEK**, **MONTH**, and **YEAR**. | SELECT DATE_ADD('2022-06-10 17:17:21', INTERVAL -3 HOUR); <br /><br />->2022-06-10 14:17:21 |
| DATE_FORMAT(d,f) | Formats date _d_ based on expression _f_. | SELECT DATE_FORMAT('2022-06-10 17:21:11','%Y-%m-%d %r');<br /><br />->2022-06-10 05:21:11 PM |
| DATE_SUB(date,INTERVAL expr type) | Subtracts a time value (interval) from date _date_.<br />_type_ can be set to **SECOND**, **MINUTE**, **HOUR**, **DAY**, **WEEK**, **MONTH**, and **YEAR**. | SELECT DATE_SUB(<br />CURRENT_DATE(),INTERVAL 2 DAY);<br /><br />->2022-06-08 |
| DAY(d) | Returns the day in date _d_. | SELECT DAY('2022-06-10');  <br /><br />->10 |
| DAYNAME(d) | Returns the name of the weekday from date** **_d_, for example, **Monday**. | SELECT DAYNAME('2022-06-10 17:30:30');<br /><br />->Friday |
| DAYOFMONTH(d) | Returns the day of the month from date _d_. | SELECT DAYOFMONTH('2022-06-10 17:31:11');<br /><br />->10 |
| DAYOFWEEK(d) | Returns the weekday index from date _d_. <br />The return value ranges from 1 to 7 and value 1 indicates Sunday. | SELECT DAYOFWEEK('2022-06-10 17:35:11');<br /><br />->6 |
| DAYOFYEAR(d) | Returns the day of the year from date _d_. | SELECT DAYOFYEAR('2022-06-10 18:02:11');<br /><br />->161 |
| EXTRACT(type FROM d) | Extracts part of date _d_. <br />_type_ can be set to **SECOND**, **MINUTE**, **HOUR**, **DAY**, **WEEK**, **MONTH**, and **YEAR**. | SELECT EXTRACT(MONTH FROM '2022-06-10 18:02:33') ;<br /><br />->6 |
| HOUR(t) | Extracts the hour from time _t_. | SELECT HOUR('18:06:31');<br /><br />->18 |
| LAST_DAY(d) | Returns the last day of the month from date _d_. | SELECT LAST_DAY("2022-06-10");<br /><br />->2022-06-30 |
| MAKEDATE(year, day-of-year) | Creates a date based on the given year and day of year. <br />_year_ specifies the year. _day-of-year_ specifies the day of year. | SELECT MAKEDATE(2022,161);<br /><br />->2022-06-10 |
| MAKETIME(hour, minute, second) | Creates time based on the given hour, minute, and second. | SELECT MAKETIME(11,35,4);<br /><br />->11:35:04 |
| MICROSECOND(date) | Returns the microseconds from date _date_. | SELECT MICROSECOND('2022-06-10 18:12:00.000023');<br /><br />->23 |
| MINUTE(t) | Returns the minute from time _t_. | SELECT MINUTE('18:12:31');<br /><br />->12 |
| MONTHNAME(d) | Returns the name of the month from date _d_, such as **November**. | SELECT MONTHNAME('2022-06-10 18:13:19');<br /><br />->June |
| MONTH(d) | Returns the month from date _d_. <br />The return value ranges from 1 to 12. | SELECT MONTH('2022-06-10 18:14:11');<br /><br />->6 |
| PERIOD_ADD(period, number) | Adds a period (expressed in months) to a year-month. <br />_period_ specifies the year-month. _number_ specifies the period to add. | SELECT PERIOD_ADD(202206,5);   <br /><br />->202211 |
| PERIOD_DIFF(period1, period2) | Returns the number of months between periods. | SELECT PERIOD_DIFF(202204,202012);<br /><br />->16 |
| QUARTER(d) | Returns the quarter from date _d_. <br />The return value ranges from 1 to 4. | SELECT QUARTER('2022-06-10 18:16:29');<br /><br />->2 |
| SECOND(t) | Returns the second from time _t_. | SELECT SECOND('18:17:36');<br /><br />->36 |
| SEC_TO_TIME(s) | Converts time _s_ which is expressed in seconds to the hh:mm:ss format. | SELECT SEC_TO_TIME(4320);<br /><br />->01:12:00 |
| STR_TO_DATE(string, format_mask) | Converts a string to a date. | SELECT STR_TO_DATE('June 10 2022','%M %d %Y');<br /><br />->2022-06-10 |
| SUBDATE(d,n) | Subtracts interval _n_ from date _d_. | SELECT SUBDATE('2022-06-10 18:19:27',15);<br /><br />->2022-05-26 18:19:27 |
| SUBTIME(t,n) | Subtracts period_ n_ from time _t_. n is expressed in seconds. | SELECT SUBTIME('2022-06-10 18:21:11',5);<br /><br />->2022-06-10 18:21:06 |
| TIME(expression) | Extracts the time portion of an expression. | SELECT TIME('18:22:10');<br /><br />->18:22:10 |
| TIME_FORMAT(t,f) | Formats time _t_ based on expression _f_. | SELECT TIME_FORMAT('18:22:59','%r');<br /><br />->06:22:59 PM |
| TIME_TO_SEC(t) | Converts time _t_ to seconds. | SELECT TIME_TO_SEC('18:24:00');<br /><br />->66240 |
| TIMEDIFF(time1, time2) | Subtracts two points in time. <br />_time1_ and _time2_ each specify a point in time. | SELECT TIMEDIFF('18:24:11','13:10:10');<br /><br />->05:14:01 |
| TIMESTAMP(expression, interval) | With a single argument, this function returns the date or datetime expression. With two arguments, this function returns the sum of the arguments. | SELECT TIMESTAMP('2022-06-10',  '18:25:17');<br /><br />->2022-06-10 18:25:17 |
| TIMESTAMPDIFF(unit,datetime_expr1,datetime_expr2) | Subtracts two datetime expressions. <br />_datetime_expr1_ and _datetime_expr2_ each specify a datetime expression. _unit_ specifies the unit of the return value. | <br />1. SELECT TIMESTAMPDIFF(DAY,'2020-12-23','2022-04-02');<br />    ->465<br /><br /><br />2. SELECT TIMESTAMPDIFF(MONTH,'2020-12-23','2022-04-02'); <br />   ->15<br /> |
| TO_DAYS(d) | Converts date _d_ to the number of days since date 0000-01-01. | SELECT TO_DAYS('2022-06-10 00:00:00');<br /><br />->738681 |
| WEEK(d) | Returns the week number of date_ d_. <br />The return value ranges from 0 to 53. | SELECT WEEK('2022-06-10 00:00:00');<br /><br />->23 |
| WEEKDAY(d) | Returns the weekday index of date _d_. <br />For example, return value **0** indicates Monday and **1** indicates Tuesday. | SELECT WEEKDAY('2022-06-10');<br /><br />->4 |
| WEEKOFYEAR(d) | Returns the calendar week of date _d_. <br />The return value ranges from 0 to 53. | SELECT WEEKOFYEAR('2022-06-10 11:11:11');<br /><br />->23 |
| YEAR(d) | Returns the year of date _d_. | SELECT YEAR('2022-06-10');<br /><br />->2022 |
| YEARWEEK(date, mode) | Returns the year and week number (value range: 0 to 53). <br />_mode_ is optional and specifies what day a week starts on. For example, return value **0** indicates Sunday and **1** indicates Monday. | SELECT YEARWEEK('2022-06-10');<br /><br />->202223 |

