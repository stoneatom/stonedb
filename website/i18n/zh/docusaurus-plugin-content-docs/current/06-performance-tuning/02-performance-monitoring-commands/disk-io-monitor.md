---
id: disk-io-monitor
sidebar_position: 7.24
---

# 磁盘IO监控

## iostat
iostat命令返回结果示例如下：
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

- **rrqm/s**: 每秒进行IO合并的读操作数
- **wrqm/s**: 每秒进行IO合并的写操作数
- **r/s**：每秒读IO次数
- **w/s**：每秒写IO次数
- **rkB/s**：每秒读K字节数
- **wkB/s**：每秒写K字节数
- **avgrq-sz**：平均每次IO请求大小，单位为扇区（512B）
- **avgqu-sz**：平均IO队列长度
- **await**：平均IO等待的响应时间
- **r_await**：每次读操作IO响应时间
- **w_await**：每次写操作IO响应时间
- **svctm**：平均IO服务的响应时间
- **%util**：IO请求的百分比（CPU使用率）
## dstat
dstat命令返回结果示例如下：
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

- **usr**：用户进程占用CPU的百分比
- **sys**：系统进程占用CPU的百分比
- **idl**：空闲CPU占用的百分比
- **wai**：IO等待占用CPU的百分比
- **stl**：虚拟化环境占用CPU的百分比
- **read**：每秒读IO数
- **writ**：每秒写IO数
- **recv**：每秒网络接收包数
- **send**：每秒网络发送包数
- **in**：每秒换页数（换进）
- **out**：每秒换页数（换出）
- **int**：每秒被中断的次数
- **csw**：每秒上下文的切换次数

