---
id: disk-io-monitor
sidebar_position: 7.24
---

# Commands for I/O Monitoring

This topic describes common commands used for I/O monitoring.
## iostat
The `iostat` command is used to monitor the performance of system I/O devices.

Command output example:

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
Parameter description:

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

## dstat

The `dstat` command is used to collect performance statistics about CPUs and I/O devices.

Command output example:

```shell
# dstat 1 5
--total-cpu-usage-- -dsk/total- -net/total- ---paging-- ---system--
usr sys idl wai stl| read  writ| recv  send|  in   out | int   csw 
0   0  99   0   0|9133B 1260k|   0     0 |   0     5B|1059  1009 
2   0  98   0   0|   0   212k| 572B 2258B|   0     0 |1099   761 
2   0  98   0   0|   0     0 | 320B 1456B|   0     0 | 919   674 
2   0  98   0   0|   0     0 | 256B 1448B|   0     0 | 949   665 
2   0  98   0   0|   0     0 |1366B 2190B|   0     0 |1031   812
```

Parameter description:

| **Parameter** | **Description** |
| --- | --- |
| usr | The percentage of CPU time spent in running user space processes. |
| sys | The percentage of CPU time spent in running system processes. |
| idl | The percentage of CPU time spent idle. |
| wai | The percentage of CPU time spent in wait. |
| stl | The percentage of CPU time spent on the hypervisor. |
| read | The number of read requests completed per second. |
| writ | The number of write requests completed per second. |
| recv | The number of packets received per second. |
| send | The number of packets sent per second. |
| in | The number of times information is copied into memory. |
| out | The number of times information is moved out of memory. |
| int | The number of interruptions occurred per second. |
| csw | The number of context switchover per second. |