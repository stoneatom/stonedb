---
id: os-tuning
sidebar_position: 7.3
---

# 操作系统优化

操作系统优化仅包含Linux，示例命令仅适用于 CentOS 7.x。
## 关闭SElinux和防火墙
关闭SElinux和防火墙的目的是打开部分网络通信，使得一些服务端口默认是开启的。
```shell
systemctl stop firewalld 
systemctl disable firewalld
vi /etc/selinux/config
#修改SELINUX的值
SELINUX = disabled
```
## I/O调度模式
如果是机械盘，调整为Deadline，目的是提高I/O吞吐量，如果是固态盘，调整为noop。
```shell
dmesg | grep -i scheduler
grubby --update-kernel=ALL --args="elevator=noop"
```
## 尽量不使用swap
如果物理内存不足，不建议使用swap作为缓冲区，因为当swap被使用，说明操作系统已经出现了严重的性能问题，将参数vm.swappiness调整最小值。
```shell
vi /etc/sysctl.conf
#新增vm.swappiness = 0
vm.swappiness = 0
```
## 关闭NUMA
如果NUMA node物理内存使用不均衡，即使物理内存总的空闲量还很多，操作系统也要做内存回收，回收的过程对操作系统是有影响的。关闭NUMA主要是为了更好的分配和使用内存。
```shell
grubby --update-kernel=ALL --args="numa=off"
```
## 关闭透明大页
透明大页是动态分配的，而数据库的内存访问模式是稀疏的，并不是连续。当内存碎片化比较严重时，动态分配透明大页会出现较大的延迟，CPU使用率会出现间接性激增的现象，因此建议关闭透明大页。
```shell
cat /sys/kernel/mm/transparent_hugepage/enabled
vi /etc/default/grub
GRUB_CMDLINE_LINUX="xxx transparent_hugepage=never"
grub2-mkconfig -o /boot/grub2/grub.cfg
```
