---
id: create-and-manage-view
sidebar_position: 5.53
---

# 创建和管理视图

创建视图，例如：要创建一个名为v_s的视图，只查询年龄大于18岁的老师，可以使用以下SQL语句：
```sql
create view v_s as select name from teachers where age>18;
```
查看创建视图语句，可以使用以下SQL语句：
```sql
show create view v_s
```
删除视图，例如：要删除视图v_s，可以使用以下SQL语句：
```sql
drop view v_s;
```
