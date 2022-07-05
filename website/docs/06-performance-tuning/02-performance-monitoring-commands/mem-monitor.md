---
id: mem-monitor
sidebar_position: 7.23
---
# Command for Memory Monitoring

The `free` command can be used to monitor memory usage.

Command output example:

```shell
# free -g
total        used        free      shared  buff/cache   available
Mem:            251          44           1           0         205         205
Swap:             7           0           7
```

Parameter description:

| **Parameter** | **Description** |
| --- | --- |
| total | The amount of memory. <br />**total** = **used** + **free** + **buff/cache** |
| used | The amount of used memory. |
| free | The amount of free memory. |
| shared | The amount of shared memory. |
| buff/cache | The amount of memory used as buffers and cache. |
| available | The amount of available memory.<br />**available** = **free** + **buff/cache** |