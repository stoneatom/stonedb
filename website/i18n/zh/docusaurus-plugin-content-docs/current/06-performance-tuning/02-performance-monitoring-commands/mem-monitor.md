---
id: mem-monitor
sidebar_position: 7.23
---
# 内存监控

free命令可以监控内存的使用情况，返回结果示例如下：
```shell
# free -g
total        used        free      shared  buff/cache   available
Mem:            251          44           1           0         205         205
Swap:             7           0           7
```
- total：物理内存总大小，total = used + free + buff/cache
- used：已使用的内存大小
- free：空闲的内存大小
- shared：共享内存大小
- buff/cache：缓存内存大小
- available：可用物理内存大小，available = free + buff/cache
