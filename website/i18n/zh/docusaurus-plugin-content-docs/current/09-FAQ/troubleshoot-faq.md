---
id: troubleshoot-faq
sidebar_position: 10.3
---

# 故障信息收集FAQ

## 数据库实例crash了该怎么办？
数据库实例crash可能发生的原因：
1）系统负载高
2）硬件故障
3）数据页损坏
4）Bug
需要收集的日志：
1）操作系统的日志，默认路径在/var/log/messages
2）StoneDB的error日志，默认路径在/stonedb/install/log/mysqld.log
3）StoneDB的trace日志，默认路径在/stonedb/install/log/trace.log
如果操作系统日志、error日志和trace日志的信息不足以分析数据库实例crash的原因，需要开启core dump。开启core dump后，当数据库实例再次发生crash时，会生成core dumpfile，用gdb分析生成的core dumpfile。
## 数据库实例挂起了该怎么办？
数据库实例挂起可能发生的原因：

1. 系统负载高

2. 连接数满

3. 磁盘空间满

4. MDL 锁等待

5. Bug

需要收集的日志：

1. 查看连接数是否满了

如果连接数超过参数 max_connections 的阀值，新的连接是无法连接数据库实例的。

2. 查看磁盘空间是否满了

如果磁盘空间满了，DML产生的binlog是无法写入磁盘文件的。

3. 查看是否有锁等待

如果`show processlist`有大量`"Waiting for table metadata lock"`等待的返回，需要找到阻塞者，并kill阻塞者。

4. 收集 mysqld 进程的堆栈
 
如果进程处于假死、死循环状态时，pstack 可以跟踪进程的堆栈。在一段时间内，多执行几次 pstack，如果堆栈总停留在同一个位置，这个位置就特别需要关注，很可能就是出问题的地方。

pstack 在抓取进程的堆栈时会触发一个 SIGSTOP 信号（类似发出 kill -19 命令），实际上 pstack 调用的是 gdb 的命令。gdb  通常会发起 SIGSTOP 信号来停止进程的运行，因此在生产环境执行 pstack 命令的时候一定要格外小心，因为可能会导致应用服务停止运行一小会儿。

输出所有线程堆栈的信息：`pstack mysqld_pid >> mysqld_pid.log`

