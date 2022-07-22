---
id: stonedb-crashed
sidebar_position: 9.4
---

# StoneDB Crashed

This topic describes common causes of StoneDB crashes.
# High system workloads
System resources may fail to be applied for due to high system workloads. As a result, StoneDB crashes. In this case, address the issue by referring to [Diagnose system resource bottlenecks](./resource-bottleneck.md).
# **Corrupted data pages**
When the hardware is faulty or the disk space is exhausted, a data file is easy to be corrupted if data is written to the data file. If a data file is corrupted, StoneDB will crash to keep data consistent.
# Bugs
If StoneDB hits a bug, such as a deadlock, it will crash. To address this issue, collect the system log, error log, and trace log, and enable core dumps to locate the fault.

