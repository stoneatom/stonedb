---
id: stonedb-faq
sidebar_position: 10.2
---

# StoneDB FAQ

## Is StoneDB compatible with MySQL?
Yes. StoneDB is compatible with the MySQL 5.6 and 5.7 protocols, and the ecosystem, common features, and common syntaxes of MySQL.  However, due to characteristics of column-based storage, StoneDB is incompatible with certain MySQL operations and features.
## Does StoneDB have its own optimizer, other than the MySQL optimizer?
Yes. StoneDB provides its own optimizer, though it still uses the MySQL optimizer to implement query parsing and rewriting.
## Why does StoneDB not support unique constraints?
Column-based storage provides the data compression feature. The compression efficiency is determined by the compression algorithm, data types of columns, and degree of repeatability. If you specify a unique constraint for a column, every data in the column is unique, and thus the compression ratio is low. Suppose 6,000 records of data are inserted into a column that is specified with a unique constraint respectively on InnoDB and StoneDB. After compression, the data volume on InnoDB is more than 16 GB and that on StoneDB is around 5 GB. The compression efficiency of StoneDB is only 3 times of that of InnoDB. Normally, this number is 10 or even higher.
## Do I need to create indexes on StoneDB?
No, you do not need to create indexes. StoneDB uses the knowledge grid technique to locate and decompress only relevant data packs based on metadata, greatly improving query performance. You can still use indexes, but the performance is low if the result sets of queries are large.
## Does StoneDB support transactions?
No. Transactions can be classified into secure transactions and non-secure transactions. Transactions that strictly meet with the atomicity, consistency, isolation, durability (ACID) attributes are identified as non-secure transactions because StoneDB does not provide redo or undo log.
## Can I join a StoneDB table with a table from another storage engine?
By default, StoneDB does not allow JOIN queries of a StoneDB table with a table from another storage engine. You can set **stonedb_ini_allowmysqlquerypath **to **1** to enable this feature.
