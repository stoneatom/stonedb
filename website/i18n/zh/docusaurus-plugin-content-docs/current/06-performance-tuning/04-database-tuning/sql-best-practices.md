---
id: sql-best-practices
sidebar_position: 7.41
---

# SQL编写规范

## 一、表设计规范
1. StoneDB的表无需创建索引，但主键是关系型数据库中唯一确定一条记录的依据，建议给表定义主键。
2. 使用自增类型主键，严禁使用uuid类型主键，自增主键能有效提高插入的性能，避免过多的数据页分裂和碎片的增加，提高了空间的使用率，而uuid类型主键不仅无序，而且占用空间更大。
3. 严禁使用外键约束，有外键的表在每次插入、更新和删除后，需要进行完整性的检查，额外的检查工作对性能来说是有损的。
4. 定长字符型字段使用CHAR类型，不定长字符型字段使用VARCHAR类型。
5. 合理定义字段长度，若实际存储长度与定义长度差异过大，不仅占用空间，而且影响访问效率。
6. 字段尽量定义为NOT NULL，并且提供默认值。
7. 表中应包含时间戳的字段，时间戳是获取增量数据的一种方法，可预估一个时间范围内的行数，也可更方便的进行数据清理和归档。
8. 字段尽量不使用大对象类型，如果查询出现大对象类型的字段，就会消耗大量的网络和IO带宽，如有需要，可考虑在外部进行存储。
9. 无论是表名还是字段名，都不能使用数据库的关键字，如desc、order、group、distinct等。
10. 无论是表还是字段，字符集建议一致。
11. 无论是表还是字段，有必要加入注释，有注释更易于维护和易读。
## 二、SQL编写规范
### 2.1 避免`select *`
查询尽量用确定的字段名，不因图方便用*来代替，理由：

1. 将不需要的字段从服务端传输到客户端，额外增加了网络开销；

2. 影响SQL的执行计划，例如：用select *进行查询时，很可能不会使用到覆盖索引，就会造成回表查询。

**反例：**
```sql
select * from test;
```
**正例：**
```sql
select id,name from test;
```
### 2.2 避免在`where`子句中使用`or`
多个字段进行or查询时，建议用union all联结，分成多次查询。

理由：使用or可能会导致索引失效

**反例：**
```sql
select * from test where group_id='40' or user_id='uOrzp9ojhfgqcwRCiume';
```
**正例：**
```sql
select * from test where group_id='40'
union all
select * from test where user_id='uOrzp9ojhfgqcwRCiume';
```
### 2.3 避免对索引列运算
理由：对索引列进行运算会导致索引失效

**反例：**
```sql
select * from test where id-1=99;
```
**正例：**
```sql
select * from test where id=100;
```
### 2.4 避免在索引列上使用函数
理由：在索引列上使用函数会导致索引失效

**反例：**
```sql
select * from test where date_add(create_time,interval 10 minute)=now();
```
**正例：**
```sql
select * from test where create_time=date_add(now(),interval - 10 minute);
```
### 2.5 避免出现索引列自动转换
如果字段的数据类型是字符串，代入变量一定要用引号括起来。

理由：对索引列进行转换会导致索引失效。

**反例：**
```sql
select * from test where group_id=40;
```
**正例：**
```sql
select * from test where group_id='40';
```
### 2.6 避免在索引列上使用`NOT`、`<>`
理由：导致索引失效
### 2.7 避免在索引列上使用`IS NOT NULL`
理由：导致索引失效
### 2.8 尽量少用前置通配符
理由：导致索引失效
### 2.9 大表删除，且没有where条件时，用truncate代替delete
理由：truncate属于ddl操作，不仅时间快，而且会释放空间。
### 2.10 大数据量删除/更新，考虑分批操作
理由：大事务简化成多个小事务，每个小事务的加锁范围小，持锁时间短，能降低系统资源的使用。
### 2.11 大数据量插入，考虑批量插入
理由：批量插入能减少commit次数，提升了性能。
### 2.12 事务尽早commit
理由：减少加锁时间。
### 2.13 尽量少用having过滤数据
理由：having会在最后一步对结果集进行过滤，这个处理需要排序、汇总等操作，如果能通过where过滤就不用having过滤。

**反例：**
```sql
select job,avg(salary) from test group by job having job = 'managent';
```
**正例：**
```sql
select job,avg(salary) from test where job = 'managent' group by job;
```
### 2.14 慎用自定义函数
理由：如果SQL语句中调用函数，则返回多少结果集，函数就会被调用多少次。
### 2.15 慎用标量子查询
理由：主查询返回多少行，标量子查询就需要执行多少次，如果主查询的结果集很大，SQL的性能就会很差。

**反例：**
```sql
select e.empno, e.ename, e.sal,e.deptno,
(select d.dname from dept d where d.deptno = e.deptno) as dname
from emp e;
```
**正例：**
```sql
select e.empno, e.ename, e.sal, e.deptno, d.dname
from emp e
left join dept d
on e.deptno = d.deptno;
```
### 2.16 排序字段顺序最好保持一致
理由：排序保持一致，能充分利用索引的有序性来消除排序带来的CPU开销。

例如：组合索引(a,b)，在反例中，由于字段a要升序，字段b要降序，优化器就不能选择索引来消除排序。

**反例：**
```sql
select a,b from test order by a,b desc;
```
**正例：**
```sql
select a,b from test order by a,b;
select a,b from test order by a desc,b desc;
```
### 2.17 表连接尽量少
理由：表连接越多，编译的时间和开销也就越大，而且优化器容易选错执行计划。
### 2.18 嵌套层次尽量少
理由：容易产生临时表和较差的执行计划。

**反例：**
```sql
select * from t1 a where a.proj_no in
(select b.proj_no from t2 b where b.proj_name = 'xxx' 
and not exists
(select 1 from t3 c where c.mess_id = b.t_project_id))
and a.oper_type <> 'D';
```
### 2.19 避免出现笛卡尔积关联
理由：如果两表进行关联，且没有关联条件，就会发生笛卡尔积关联，如果两表的数据量都很大，这种连接是极其消耗CPU和内存的。

**反例：**
```sql
select * from a,b;
```
**正例：**
```sql
select * from a,b where a.id=b.id;
```
### 2.20 limit分页的偏移量不宜过大
理由：由于先获取的是偏移量的数据，然后获取分页的数据，最后再把偏移量这一段的数据抛弃返回分页的数据，因此当偏移量很大时，SQL的性能就会很差。

**反例：**
```sql
select id,name from test limit 10000,10;
```
**正例：**
```sql
select id,name from test where id>10000 limit 10;
```
### 2.21 使用left join，应确保左边表结果集尽量小
理由：使用left join连接时，大部分情况下左边的表就是驱动表，驱动表返回多少结果集，被驱动表就要执行多少次，如果左边的表的结果集很大，那么被驱动表就要执行很多次，性能就会很差。
### 2.22 合理利用exists&in
理由：exists与in，谁的写法更高效，取决于外层查询和内层查询的结果集大小，如果外层查询结果集大于内层查询结果集，那么in优于exists，反之，exists优于in。
### 2.23 要分清union all与union的区别
区别：union all返回的是两个结果集的集合，而union会在两个结果集的集合上做排序去重操作，最终返回的是排序去重的结果集，如果能用union all就不用union，因为先排序再去重是个消耗资源的操作。
### 2.24 要分清left join与inner join的区别
区别：left join返回的是左表所有行，右表不匹配的行用null来填充，inner join返回的是两表完全匹配的行。
### 2.25 要分清left join中on...and与on...where的区别
区别：
1. `on...and`没有起到过滤作用，不匹配的行还是用`null`来填充，`on...where`有过滤作用；
2. left右边的表谓词条件不管放在on还是where后面，都会对右表先过滤再连接，但是放在where后left join会转换为inner join。
### 2.26 要分清inner join中on...and与on...where的区别
区别：`on...and`与`on...where`是等价的，都有过滤作用。
### 2.27 不必要的排序
理由：用于统计条数，那么排序就是多此一举的。

**反例：**
```sql
select count(*) as totalCount from 
(select * from enquiry e where 1 = 1
AND status = 'closed'
AND is_show = 1
order by id desc, expire_date asc) _xx;
```
**正例：**
```sql
select count(*) from enquiry e where 1 = 1
AND status = 'closed'
AND is_show = 1;
```
### 2.28 不必要的嵌套
理由：能用一条select就满足的查询条件，避免了外层的嵌套select查询。

**反例：**
```sql
select count(*) as totalCount from 
(select * from enquiry e where 1 = 1 
AND status = 'closed'
AND is_show = 1
order by id desc, expire_date asc) _xx;
```
**正例：**
```sql
select count(*) from enquiry e where 1 = 1
AND status = 'closed'
AND is_show = 1;
```
### 2.29 写完SQL要explain SQL
编写完SQL后，需要查看这条SQL的执行计划，需要关注执行计划中的type、rows、extra。
