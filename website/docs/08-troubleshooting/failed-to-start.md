---
id: failed-to-start
sidebar_position: 9.1
---

# Failed to Start StoneDB

Many issues will cause start failures of StoneDB. If StoneDB cannot be started, we recommend you check whether any error information is recorded in **mysqld.log**. This topic describes common causes of a start failure of StoneDB.
## Improper parameter settings
If the failure is caused by improper parameter settings, check **mysqld.log** to see which parameters are improperly configured. 

The following example indicates that parameter **datadir** is improperly configured.

```bash
[ERROR] failed to set datadir to /stonedb/install/dataxxx/
```
## Denial to access resources
If the port is occupied, the directory owner is incorrect, or the permission on the directory is insufficient, you cannot access the directory.
```bash
Error: unable to create temporary file; errno: 13
```
## Damaged data pages
If a relevant data page is damaged, StoneDB cannot be started. In this case, you must restore the data page from a backup.
