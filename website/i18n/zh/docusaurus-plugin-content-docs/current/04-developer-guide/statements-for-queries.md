---
id: statements-for-queries
sidebar_position: 5.4
---

# 查询语句

## 常规查询
### union/union all
```sql
select first_name from t_test1
union all
select first_name from t_test2;
```
### distinct
```sql
select distinct first_name from t_test1;
```
### like
```sql
select * from t_test where first_name like 'zhou%';
```
### group by/order by
```sql
select first_name,count(*) from t_test1 group by first_name order by 2;
```
### having
```sql
select e.id, count(e.id), round(avg(e.score), 2)
from t_test1 e
group by e.id
having avg(e.score) > (select avg(score) from t_test1);
```
## 聚合查询
```sql
select first_name,count(*) from t_test group by first_name;
select sum(score) from t_test;
```
## 分页查询
```sql
select * from t_test1 limit 10;
select * from t_test1 limit 10,10;
```
## 关联查询
### 内连接
```sql
select t1.id,t1.first_name,t2.last_name from t_test1 t1,t_test2 t2 where t1.id = t2.id;
```
### 左连接
```sql
select t1.id,t1.first_name,t2.last_name from t_test1 t1 left join t_test2 t2 on t1.id = t2.id and t1.id=100;
```
### 右连接
```sql
select t1.id,t1.first_name,t2.last_name from t_test1 t1 right join t_test2 t2 on t1.id = t2.id and t1.id=100;
```
## 子查询
### 标量子查询
```sql
select e.id,
e.first_name,
(select d.first_name from t_test2 d where d.id = e.id) as first_name
from t_test1 e;
```
### 派生子查询
```sql
select a.first_name, b.last_name
from t_test1 a, (select id,last_name from t_test2) b
where a.id = b.id;
```
### in/not in子查询
```sql
select * from t_test1 where id in(select id from t_test2);
```
### exists/not exists子查询
```sql
select * from t_test1 A where exists (select 1 from t_test2 B where B.id = A.id);
```




