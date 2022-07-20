---
id: stonedb-crashed
sidebar_position: 9.4
---

# 数据库实例crash

## 系统负载高
系统负载高导致了申请系统资源失败，最后数据库实例crash，常见原因及诊断方法详见[系统资源瓶颈诊断](resource-bottleneck.md)。
## 数据页损坏
如果是硬件故障或者磁盘空间满了，向数据文件写入时，很容易导致数据文件的写corrupt，为了保证数据的一致性，数据库实例会crash。
## Bug
数据库实例发生crash的一个很常见的原因是命中Bug，如内部发生死锁。当数据库实例发生crash，需要收集系统日志、error日志和trace日志，甚至还需要开启core dump来定位。

