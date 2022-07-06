---
id: excessive-large-directory
sidebar_position: 9.2
---

# Excessively Large Data Directory

The data directory contains data files, binlogs, and error logs. If the data directory is run out of capacity, the database will be suspended and cannot provide services. To prevent this issue, monitoring on capacity usage must be strengthened in routine maintenance. This topic describes common causes of this issue.
## Big transactions
If big transactions exist, a large amount of binlogs are generated. If the binlog cache is insufficient, excessive binlogs will be temporarily stored to temporary files on disks. Big transactions not only occupy too much disk space, but result in long primary/secondary replication latency. Therefore, we recommend that you split each big transactions into multiple small transactions in your production environment.
## CARTESIAN JOIN
When SQL statements do not strictly follow syntaxes, for example, no condition is specified during table association, Cartesian products are generated. If the tables to associate are large, the table space will be used up. Therefore, we recommend that you check the execution plan each time you finish writing an SQL statement. If "Using join buffer (Block Nested Loop)" exists in the execution plan, check the two associated tables to see whether the association condition of the driven table is not indexed or no association condition exists.

:::info
You can start StoneDB to release the temporary table space.
:::
## Subqueries and grouped orderings
Subqueries and grouped orderings use temporary tables to cache intermediate result sets. If the temporary files run out of space, the intermediate result sets will be temporarily stored to temporary files on disks.
