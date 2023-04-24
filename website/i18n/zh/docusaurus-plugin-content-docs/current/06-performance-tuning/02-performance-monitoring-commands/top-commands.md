---
id: top-commands
sidebar_position: 7.21
---

# TOP命令

top命令即可以监控操作系统CPU、内存、Swap的使用情况，也可以监控进程的详细信息，默认按照CPU的使用率排序。
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
## 第一行
10:12:21：当前系统时间

up 5 days：自上一次系统启动后到现在的运行天数

4 users：登录到系统的用户数

load average：过去1分钟、5分钟、15分钟，系统负载的平均值
## 第二行
total：系统进程总数

running：处于运行状态的进程数

sleeping：处于休眠状态的进程数

stopped：处于被停止状态的进程数

zombie：处于僵尸状态进程数
## 第三行
us：用户进程占用CPU的百分比

sy：系统进程占用CPU的百分比

ni：优先级被改变过的进程占用CPU的百分比

id：空闲CPU占用的百分比

wa：IO等待占用CPU的百分比

hi：硬件中断占用CPU的百分比

si：软件中断占用CPU的百分比

st：虚拟化环境占用CPU的百分比

需要重点关注CPU的使用率，当us值较高时，说明用户进程消耗CPU时间较多，如果长时间超过50%时，应尽快优化应用服务。当sy值较高时，说明系统进程消耗CPU时间较多，比如可能是操作系统配置不合理或者出现操作系统的Bug。当wa值较高时，说明系统IO等待比较严重，比如可能是发生了大量的随机IO访问，IO带宽出现瓶颈。
## 第四行
total：物理内存总大小，单位为M

free：空闲的内存大小

used：已使用的内存大小

buff/cache：已缓存的内存大小
## 第五行
total：Swap大小

free：空闲的Swap大小

used：已使用的Swap大小

avail Mem：已缓存的Swap大小

## 进程列表

PID：进程的id

USER：进程的拥有者

PR：进程的优先级，值越小越优先执行

NI：进程nice值，正值表示降低进程优先级，负值表示提高进程优先级，nice取值范围为(-20,19)，默认情况下，进程的nice值为0

VIRT：进程占用的虚拟内存大小

RES：进程占用的物理内存大小

SHR：进程占用的共享内存大小

S：进程状态，其中S表示休眠，R表示正在运行，Z表示僵死状态，N表示该进程优先值为负数

%CPU：进程CPU使用率

%MEM：进程内存使用率

TIME+：进程启动后占用CPU的总时间，即占用CPU使用时间的累加值

COMMAND：进程启动命令名称
