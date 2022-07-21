---
id: failed-to-connect
sidebar_position: 9.7
---

# 数据库连接问题

## 连接数满
如果连接数据库时发生如下报错，说明连接数已经达到参数max_connections的阈值。需要管理员用户登录，并kill空闲的连接，然后根据业务量重新评估参数max_connections设置是否合理，是否需要增大阈值。
```
ERROR 1040 (HY000): Too many connections
```
## MDL锁等待
如果连接数据库时发生挂起，可能是MDL锁等待，需要管理员用户登录检查是否有大量"Waiting for table metadata lock"等待的返回，找到阻塞者，并kill阻塞者。
```sql
show processlist；
```
## 用户名密码错误或者权限不足
如果连接数据库时发生如下报错，可能是用户名密码错误或者用户的权限不足，需要查询权限表。
```
ERROR 1045 (28000): Access denied for user 'sjj'@'localhost' (using password: YES) 
```
