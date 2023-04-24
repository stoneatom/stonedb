---
id: resource-bottleneck
sidebar_position: 9.9
---

# 系统资源瓶颈诊断

当操作系统的资源出现瓶颈时，不仅操作系统上的应用服务受到影响，而且在操作系统执行简单的命令可能都无法返回结果。在操作系统尚未完全夯死前，可以使用相关命令对CPU、内存、IO和网络资源的使用情况进行信息的收集，然后分析确认这些资源是否被合理利用，是否存在瓶颈。
## CPU
top、vmstat都可以检查CPU的使用情况，但top的结果显示的更全面。top返回的结果有两层，上层是系统性能的统计信息，下层是进程的统计信息，默认按照CPU的使用率排序。
top返回结果示例如下：
```shell
top - 10:12:21 up 5 days, 22:31,  4 users,  load average: 1.00, 1.00, 0.78
Tasks: 731 total,   1 running, 730 sleeping,   0 stopped,   0 zombie
%Cpu(s):  1.7 us,  0.0 sy,  0.0 ni, 98.3 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st
MiB Mem : 257841.3 total,   1887.5 free,  45581.6 used, 210372.2 buff/cache
MiB Swap:   8192.0 total,   8188.7 free,      3.3 used. 210450.4 avail Mem 

PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND                                                                                                                
908076 mysql     20   0  193.0g  42.4g  44088 S 100.3  16.8 228:10.34 mysqld                                                                                                                 
823137 root      20   0 6187564  83772  51636 S   6.6   0.0   6:36.12 dockerd                                                                                                                
822938 root      20   0 3278696  58500  35420 S   0.7   0.0  38:37.69 containerd                                                                                                             
1483 root      20   0  239280   9260   8136 S   0.3   0.0   0:19.16 accounts-daemon                                                                                                        
928343 root      20   0    9936   4576   3240 R   0.3   0.0   0:00.04 top                                                                                                                    
  ......
```
第一行

- `10:12:21`：当前系统时间

- `up 5 days`：自上一次系统启动后到现在的运行天数

- `4 user`：登录到系统的用户数

- `load average`：过去1分钟、5分钟、15分钟，系统负载的平均值

第二行

- `total`：系统进程总数

- `running`：处于运行状态的进程数

- `sleeping`：处于休眠状态的进程数

- `stopped`：处于被停止状态的进程数

- `zombie`：处于僵尸状态进程数

第三行

- `us`：用户进程占用CPU的百分比

- `sy`：系统进程占用CPU的百分比

- `ni`：优先级被改变过的进程占用CPU的百分比

- `id`：空闲CPU占用的百分比

- `wa`：IO等待占用CPU的百分比

- `hi`：硬件中断占用CPU的百分比

- `si`：软件中断占用CPU的百分比

- `st`：虚拟化环境占用CPU的百分比

需要重点关注CPU的使用率，当us值较高时，说明用户进程消耗CPU时间较多，如果长时间超过50%时，应尽快优化应用服务。当sy值较高时，说明系统进程消耗CPU时间较多，比如可能是操作系统配置不合理或者出现操作系统的Bug。当wa值较高时，说明系统IO等待比较严重，比如可能是发生了大量的随机IO访问，IO带宽出现瓶颈。

第四行

- `total`：物理内存总大小，单位为M

- `free`：空闲的内存大小

- `used`：已使用的内存大小

- `buff/cache`：已缓存的内存大小

第五行

- `total`：Swap大小

- `free`：空闲的Swap大小

- `used`：已使用的Swap大小

- `avail Mem`：已缓存的Swap大小

进程列表

- `PID`：进程的id

- `USER`：进程的拥有者

- `PR`：进程的优先级，值越小越优先执行

- `NI`：进程nice值，正值表示降低进程优先级，负值表示提高进程优先级，nice取值范围为(-20,19)，默认情况下，进程的nice值为0

- `VIRT`：进程占用的虚拟内存大小

- `RES`：进程占用的物理内存大小

- `SHR`：进程占用的共享内存大小

- `S`：进程状态，其中S表示休眠，R表示正在运行，Z表示僵死状态，N表示该进程优先值为负数

- `%CPU`：进程CPU使用率

- `%MEM`：进程内存使用率

- `TIME+`：进程启动后占用CPU的总时间，即占用CPU使用时间的累加值

- `COMMAND`：进程启动命令名称

**出现CPU使用率高诊断方法**：
1）找出调用的函数
```shell
top -H
perf top -p xxx
```
:::info
xxx为top -H返回最消耗CPU的进程。
:::

2）找出消耗CPU的SQL
```shell
pidstat -t -p <mysqld_pid> 1 5
select * from performance_schema.threads where thread_os_id = xxx\G
select * from information_schema.processlist where id = performance_schema.threads.processlist_id\G
```
:::info
xxx为pidstat返回最消耗CPU的线程。
:::
## 内存
top、vmstat、free都可以检查内存的使用情况。

free返回结果示例如下：
```shell
# free -g
total        used        free      shared  buff/cache   available
Mem:            251          44           1           0         205         205
Swap:             7           0           7
```
- `total`：物理内存总大小，total = used + free + buff/cache
- `used`：已使用的内存大小
- `free`：空闲的内存大小
- `shared`：共享内存大小
- `buff/cache`：缓存内存大小
- `available`：可用物理内存大小，available = free + buff/cache

**出现内存使用率高诊断方法**：

1）检查配置是否合理，例如：操作系统物理内存128G，而分配给数据库实例110G，由于操作系统进程和其它应用程序也需要内存，很容易导致内存被消耗光；

2）检查并发连接数是否过高，read_buffer_size、read_rnd_buffer_size、sort_buffer_size、thread_stack、join_buffer_size、binlog_cache_size都是session级别的，连接数越多，需要的内存就越多，因此这些参数也不能设置太大；

3）检查是否有不合理的join，例如：多表关联时，执行计划中驱动表的结果集比较大，需要循环多次执行，容易导致内存泄漏；

4）检查打开文件数是否过多和table_open_cache是否设置合理，访问一个表时，会将该表放入缓存区table_open_cache，目的是下次可以更快的访问，但如果table_open_cache设置太大，且打开的表又多的情况下，是很消耗内存的。
## IO
iostat、dstat、pidstat都可以检查IO的使用情况。

iostat返回结果示例如下：
```shell
# iostat -x 1 1
Linux 3.10.0-957.el7.x86_64 (htap2)     06/13/2022      _x86_64_        (64 CPU)

avg-cpu:  %user   %nice %system %iowait  %steal   %idle
0.06    0.00    0.03    0.01    0.00   99.90

Device:         rrqm/s   wrqm/s     r/s     w/s    rkB/s    wkB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
sda               0.00     0.00    0.00    0.00     0.04     0.00    85.75     0.00    0.25    0.25    0.00   0.15   0.00
sdb               0.06     0.11    7.61    1.10  1849.41    50.81   436.48     0.36   40.93   46.75    0.48   1.56   1.35
dm-0              0.00     0.00    0.28    0.19     8.25    12.05    87.01     0.00    4.81    7.37    0.94   1.61   0.08
```
- `rrqm/s`: 每秒进行merge的读操作数

- `wrqm/s`: 每秒进行merge的写操作数

- `r/s`：每秒读IO次数

- `w/s`：每秒写IO次数

- `rkB/s`：每秒读IO大小，单位为KB

- `wkB/s`：每秒写IO大小，单位为KB

- `avgrq-sz`：平均请求大小，单位为扇区（512B）

- `avgqu-sz`：在驱动请求队列和在设备中活跃的平均请求数

- `await`：平均IO响应时间，包括在驱动请求队列里的等待和设备的IO响应时间

- `r_await`：每次读操作IO响应时间

- `w_await`：每次写操作IO响应时间

- `svctm`：磁盘设备的IO平均响应时间

- `%util`：设备忙处理IO请求的百分比（使用率），磁盘的繁忙程度

- `r/s + w/s`：IOPS

**出现IO使用率高诊断方法**：
1）找出使用率最高的磁盘设备
```shell
iostat -x -m 1
iostat -d /dev/sda -x -m 1
```
2）找出占用IO高的应用程序
```shell
pidstat -d 1
```
3）找出占用IO高的线程
```shell
pidstat -dt -p mysqld_id 1
```
4）找出占用IO高的SQL
```sql
select * from performance_schema.threads where thread_os_id = xxx\G
select * from information_schema.processlist where id = performance_schema.threads.processlist_id\G
```
:::info

xxx为pidstat返回最消耗IO的线程。

:::