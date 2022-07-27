---
id: top-commands
sidebar_position: 7.21
---

# The top command

The `top` command can be used to monitor the usage of CPUs, memory, and swap space of the OS. It can also be used to monitor processes. The command output is sorted based on the CPU time.

Command output example:
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

Pay attention to values in this line. If the value of **us** is large, user space processes consume much CPU time. If the value of **us** is larger than 50% for a long time, applications must be tuned in time. If the value of **sy** is large, system processes consume much CPU time. This may be caused by improper OS configuration or OS bugs. If the value of **wa** is large, I/O waits are high. This may be caused by high random I/O access or an I/O bottleneck.

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