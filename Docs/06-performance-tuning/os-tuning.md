---
id: os-tuning
sidebar_position: 7.3
---

# OS Tuning

This topic describes how to tune a Linux OS. Methods to tune other types of OSs are currently not provided. The commands used in the following example suits only to CentOS 7._x_.
## **Disable SELinux and the firewall**
We recommend that you disable SELinux and the firewall to allow access from certain services.
```shell
systemctl stop firewalld 
systemctl disable firewalld
vi /etc/selinux/config
# Modify the value of SELINUX.
SELINUX = disabled
```
## **Change the I/O scheduling mode**
If your disks are hard disk drives (HDDs), change the mode to **Deadline** to improve throughput. If your disks are solid-state drive (SSDs), change the mode to **noop**.
```shell
dmesg | grep -i scheduler
grubby --update-kernel=ALL --args="elevator=noop"
```
## **Do use swap space unless necessary**
If your memory is insufficient, we recommend that you do not use swap space as buffer. This is because the OS will suffer from severe performance problems if the swap space is used. For this reason, set **vm.swappiness** to the smallest value.
```shell
vi /etc/sysctl.conf
# Add parameter setting vm.swappiness = 0
vm.swappiness = 0
```
## **Disable NUMA**
If the memory allocated to non-uniform memory access (NUMA) nodes in a zone is exhausted, the OS will reclaim memory even though the free memory in total is sufficient. This has certain impacts on the OS. We recommend that you disable NUMA to allocate and use memory more efficiently.
```shell
grubby --update-kernel=ALL --args="numa=off"
```
## **Disable transparent hugepages**
Transparent hugepages are dynamically allocated. Databases use sparse memory access. If the system contains a large number of memory fragments, dynamic allocation of transparent hugepages will suffer from high latency and CPU bursts will occur. For this reason, we recommend you to disable transparent hugepages.
```shell
cat /sys/kernel/mm/transparent_hugepage/enabled
vi /etc/default/grub
GRUB_CMDLINE_LINUX="xxx transparent_hugepage=never"
grub2-mkconfig -o /boot/grub2/grub.cfg
```