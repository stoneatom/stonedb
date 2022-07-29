---
id: resource-bottleneck
sidebar_position: 9.9
---

# Diagnose System Resource Bottlenecks

If an OS resource bottleneck occurs, applications running on the OS are affected and the OS may even fail to respond to simple instructions. Before the OS stops providing services, you can run commands to collect usage information about CPU, memory, I/O, and network resources and then analyze whether these resources are properly used and whether any resource bottlenecks exist.
## CPU
The `top` and `vmstat` commands can be used to check the CPU utilization. The information returned by the `top` command is more comprehensive, which consists the statistics about the system performance and information about processes. The information returned is sorted by CPU utilization.

Example of `top `command output:
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
### Parameter description
Parameters in line 1:

| **Parameter** | **Description** |
| --- | --- |
| 10:12:21 | The current system time. |
| up 5 days | The number of days for which the system runs continuously since last startup. |
| 4 users | The number of the online users. |
| load average | The average system workloads in the past 1 minute, 5 minutes, and 15 minutes. |

Parameters in line 2:

| **Parameter** | **Description** |
| --- | --- |
| total | The number of processes. |
| running | The number of processes that are in the running state. |
| sleeping | The number of processes that are in the sleeping state. |
| stopped | The number of processes that are in the stopped state. |
| zombie | The number of processes that are in the zombie state. |

Parameters in line 3:

| **Parameter** | **Description** |
| --- | --- |
| us | The percentage of CPU time spent in running user space processes. |
| sy | The percentage of CPU time spent in running system processes. |
| ni | The percentage of CPU time spent in running the processes of which priorities are changed. |
| id | The percentage of CPU time spent idle. |
| wa | The percentage of CPU time spent in wait. |
| hi | The percentage of CPU time spent in handling hardware interrupts. |
| si | The percentage of CPU time spent in handling software interrups. |
| st | The percentage of CPU time spent on the hypervisor. |

Pay attention to values in line 3. If the value of **us** is large, user space processes consume much CPU time. If the value of **us** is larger than 50% for a long time, applications must be tuned in time. If the value of **sy** is large, system processes consume much CPU time. This may be caused by improper OS configuration or OS bugs. If the value of **wa** is large, I/O waits are high. This may be caused by high random I/O access or an I/O bottleneck.

Parameters in line 4:

| **Parameter** | **Description** |
| --- | --- |
| total | The amount of memory. |
| free | The amount of free memory. |
| used | The amount of used memory. |
| buff/cache | The amount of memory that is used as buffers and cache. |

Parameters in line 5:

| **Parameter** | **Description** |
| --- | --- |
| total | The size of swap space. |
| free | The size of free swap space. |
| used | The size of used swap space. |
| avail Mem | The size of swap space that has been cached. |

Parameters in the process list:

| **Parameter** | **Description** |
| --- | --- |
| PID | The process ID. |
| USER | The owner of the process. |
| PR | The priority of the process. A smaller value indicates a higher priority. |
| NI | The nice value of the priority. A positive integer indicates that the priority of the process is being downgraded. A negative integer indicates that the priority of the process is being upgraded. The value range is -20 to 19 and the default value is 0. |
| VIRT | The amount of virtual memory occupied by the process. |
| RES | The amount of physical memory occupied by the process. |
| SHR | The amount of shared memory occupied by the process. |
| S | The status of the process. The value can be:<br />- **S**: sleeping<br />- **R**: running<br />- **Z**: zombie<br />- **N**: The nice value of the process is a negative value.<br /> |
| %CPU | The percentage of the CPU time used by the process. |
| %MEM | The percentage of the memory occupied by the process. |
| TIME+ | The total CPU time used by the process. |
| COMMAND | The command that the process is running. |

### Diagnose the cause of high CPU utilization

1. Find the function that is called by the process that consumes the most CPU time.
```shell
top -H
perf top -p xxx
```
_xxx_ indicates the process that consumes the most CPU time.

2. Find the SQL statement that consumes the most CPU time.
```shell
pidstat -t -p <mysqld_pid> 1 5
select * from performance_schema.threads where thread_os_id = xxx\G
select * from information_schema.processlist where id = performance_schema.threads.processlist_id\G
```
_xxx_ indicates the thread that consumes the most CPU time.
## **Memory**
The `top`, `vmstat`, and `free` commands can be used to monitor memory usage.

Example of `free` command output:
```shell
# free -g
total        used        free      shared  buff/cache   available
Mem:            251          44           1           0         205         205
Swap:             7           0           7
```

### Parameter description
| **Parameter** | **Description** |
| --- | --- |
| total | The amount of memory. <br />**total** = **used** + **free** + **buff/cache** |
| used | The amount of used memory. |
| free | The amount of free memory. |
| shared | The amount of shared memory. |
| buff/cache | The amount of memory used as buffers and cache. |
| available | The amount of available memory.<br />**available** = **free** + **buff/cache** |

### Diagnose the cause of high memory usage

1. Check whether memory is properly configured. For example, if the physical memory of the OS is 128 GB and 110 GB is allocated to the database instance, there is a high probability of memory exhaustion, because other OS processes and applications are consuming memory.
2. Check whether too many concurrent connections exist. **read_buffer_size**, **read_rnd_buffer_size**, **sort_buffer_size**, **thread_stack**, **join_buffer_size**, and **binlog_cache_size** are all session-level parameters. More connections indicate more memory consumed. Therefore, we recommend that you set these parameters to small values.
3. Check whether improper JOIN queries exist. Suppose a query that joins multiple tables exists, and the driving table of this query has a large result set. When the query is being executed, the driven table is executed for a large number of times, which may result in memory leakage.
4. Check whether too many files are open and whether **table_open_cache** is properly set. When you access a table, the table is loaded to the cache specified by **table_open_cache** so that next access to this table can be accelerated. However, if **table_open_cache** is too large and too many tables are open, a large amount of memory will be consumed.
## I/O
The `iostat`, `dstat`, and `pidstat` commands are used to monitor the I/O usage.

Example of `iostat` command output:
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
### Parameter description
| **Parameter** | **Description** |
| --- | --- |
| rrqm/s | The number of read requests merged per second. |
| wrqm/s | The number of write requests merged per second. |
| r/s | The number (after merges) of read requests completed per second. |
| w/s | The number (after merges) of write requests completed per second. |
| rkB/s | The number of kilobytes read per second. |
| wkB/s | The number of kilobytes written per second. |
| avgrq-sz | The average size of the requests, expressed in sectors (512 bytes). |
| avgqu-sz | The average queue length of the I/O requests. |
| await | The average time for I/O requests to be served. |
| r_await | The average time for read requests to be served. |
| w_await | The average time for write requests to be served. |
| svctm | The average service time for I/O requests. |
| %util | The percentage of CPU time spent on I/O requests. |


:::info
The sum of **r/s** and **w/s** is the system input/output operations per second (IOPS).
:::
### Diagnose the cause of high I/O usage

1. Find the disk with the highest usage.
```shell
iostat -x -m 1
iostat -d /dev/sda -x -m 1
```

2. Find the application with the highest I/O usage.
```shell
pidstat -d 1
```

3. Find the thread with the highest I/O usage.
```shell
pidstat -dt -p mysqld_id 1
```

4. Find the SQL statement with the highest I/O usage.
```sql
select * from performance_schema.threads where thread_os_id = xxx\G
select * from information_schema.processlist where id = performance_schema.threads.processlist_id\G
```
_xxx_ indicates the thread with the highest I/O usage.
