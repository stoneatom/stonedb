---
id: statements-for-queries
sidebar_position: 5.4
---

# Statements for Queries
## **Statements for common queries**
### **UNION/UNION ALL**
```sql
select first_name from t_test1
union all
select first_name from t_test2;
```
### **DISTINCT**
```sql
select distinct first_name from t_test1;
```
### **LIKE**
```sql
select * from t_test where first_name like 'zhou%';
```
### **GROUP BY/ORDER BY**
```sql
select first_name,count(*) from t_test1 group by first_name order by 2;
```
### **HAVING**
```sql
select e.id, count(e.id), round(avg(e.score), 2)
from t_test1 e
group by e.id
having avg(e.score) > (select avg(score) from t_test1);
```
## **Statements used for aggregate queries**
```sql
select first_name,count(*) from t_test group by first_name;
select sum(score) from t_test;
```
## **Statements used for pagination queries**
```sql
select * from t_test1 limit 10;
select * from t_test1 limit 10,10;
```
## **Statements used for join queries**
### **INNER JOIN**
```sql
select t1.id,t1.first_name,t2.last_name from t_test1 t1,t_test2 t2 where t1.id = t2.id;
```
### **LEFT JOIN**
```sql
select t1.id,t1.first_name,t2.last_name from t_test1 t1 left join t_test2 t2 on t1.id = t2.id and t1.id=100;
```
### **RIGHT JOIN**
```sql
select t1.id,t1.first_name,t2.last_name from t_test1 t1 right join t_test2 t2 on t1.id = t2.id and t1.id=100;
```
## **Statements used for subqueries**
### **Statement for scalar subqueries**
```sql
select e.id,
e.first_name,
(select d.first_name from t_test2 d where d.id = e.id) as first_name
from t_test1 e;
```
### **Statement for derived subqueries**
```sql
select a.first_name, b.last_name
from t_test1 a, (select id,last_name from t_test2) b
where a.id = b.id;
```
### IN/NOT IN
```sql
select * from t_test1 where id in(select id from t_test2);
```
## EXISTS/NOT EXISTS
```sql
select * from t_test1 A where exists (select 1 from t_test2 B where B.id = A.id);
```













