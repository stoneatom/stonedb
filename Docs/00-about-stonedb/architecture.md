---
id: architecture
sidebar_position: 1.2
---

# Architecture

![StoneDB_V1.0](./stonedb-architecture-V1.png)

StoneDB is a hybrid transaction/analytical processing (HTAP) database. It provides a column-based storage engine also named StoneDB to handle online analytical processing (OLAP) workloads. The StoneDB storage engine features high performance and high data compression ratio, in addition to common features provided by other storage engines such as InnoDB and MyISAM. The logical architecture of StoneDB consists of three layers: applications, services, and storage engine. When an SQL query is processed by StoneDB, the SQL query is processed through every module in the three layers one after one.

:::info
 In this topic, StoneDB refers to the database, unless otherwise specified.
:::
## Applications layer
### Connection management
When a client sends a connection request to a server, the server assigns a thread from the thread pool to process interactions with the client. If the client disconnects from the server, the thread is assigned to a new connection, instead of being destroyed. This saves time in creating and releasing threads.
### Authentication
After receiving a connection request from a client, the server authenticates the user that initiates the connection based on the username, password, IP address, and port number. If the user fails the authentication, the server rejects the connection request.
### Access control
After the client is connected to the server, the server identifies what operations are allowed for the user based on the permissions granted to the user.
## Services layer
### Management & Utilities
StoneDB provides various database management features, such as backup, recovery, user management, permission management, and database metadata management.
### SQL Interface
SQL Interface is mainly used to receive and process SQL queries and return query results.
### Cache & Buffers
The query cache is used to temporarily store the hash values and result sets of executed SQL statements to enhance execution efficiency. When a query passes through this module, the hash value of the query is used to check if any matching record exists in the query cache. If no, the query is then parsed, optimized, and executed. After it is processed, its hash value and result set are cached. If yes, the result set is directly read from the cache. However, if the query hits the cache but the schema or data of the queried table is modified, the relevant cache is invalid and the query still needs to be further processed to obtain the result set. Therefore, we recommend that you disable the query cache in your production environment. The query cache is removed in MySQL 8.0.
### Parser
The parser is used to parse SQL statements and generate parse trees. The parser performs lexical analysis to check whether the table and columns exist and then syntax analysis to check whether SQL statements are written in correct syntax. If any error is detected, relevant error information will be returned.
### Optimizer
The optimizer chooses the execution plan with the lowest cost for each SQL query based on the tables, indexes, and other statistics information relevant to the SQL query.
### Executor
The executor verifies whether the user that initiates a query has permissions to operate on the relevant tables. If the user has sufficient permissions, the executor calls API operations to read data and returns the query result.
## Storage engine layer
When your data volume reaches tens of or even hundreds of billions of records, executing a statistical or aggregate query on MySQL or another relational database may take several to dozens of minutes. However, to process the same query, StoneDB requires only one tenth of the time or even less. This is because StoneDB uses column-based storage, data compression, and knowledge grid techniques to optimize query processing.
### StoneDB Optimizer
StoneDB Optimizer is a self-developed optimizer provided by StoneDB. It is used to optimize SQL statements by converting expressions, converting subqueries to joins, and then generates a high-efficiency execution plan by using the Knowledge Grid technique.
### StoneDB Executor
StoneDB Executor reads data based on the execution plan.
### Knowledge Grid Manager
#### **Data Pack**
Data Packs are data storage units. Data in each column is sliced into Data Packs every 65,536 rows. A Data Pack is smaller than a column and supports higher data compression ratio, whereas it is larger than a row and supports higher query performance. Data Packs are also the units for which the Knowledge Grid uses to decompress data.

Based on the theory of rough sets, Data Packs can be classified into the following three categories:

- Irrelevant Data Packs: with no data elements relevant for further execution
- Relevant Data Packs: with all data elements relevant for further execution
- Suspect Data Packs: with some data elements relevant for further execution

This classification helps filter out irrelevant Data Packs. StoneDB needs only to read metadata of relevant Data Packs, and decompress suspect Data Packs and then examine the data records to filter relevant data records. The process of handling relevant Data Packs does not consume I/O, since no data is decompressed.
#### **Data Pack Node**
A Data Pack Node stores the following information about a Data Pack:

- The maximum, minimum, average, and sum of the values
- The number of values and the number of non-null values
- The compression method
- The length in bytes

Therefore, Data Pack Node is also called Metadata Node. One Data Pack Node corresponds to one Data Pack.
#### **Knowledge Node**
Knowledge Nodes are at the upper layer of Data Pack Nodes. Knowledge Nodes store a collection of metadata that shows the relations between Data Packs and columns, including the range of value occurrence and the associations between columns. Most data stored in a Knowledge Node is generated when data is being loaded and the rest is generated during queries. 

Knowledge Nodes can be classified into the following types:
##### **Histogram**
Histograms are used to present statistics on columns whose data types are integer, date and time, or floating point. In a histogram, the range between the maximum value and minimum value of a data pack is evenly divided into 1,024 ranges, each of which occupies 1 bit. Ranges within which at least one value falls are marked with 1. Ranges within which no value falls are marked with 0. Histograms are automatically created when data is being loaded.

Suppose values in a Data Pack fall within two ranges: 0‒100 and 102301‒102400, as shown in the following histogram.

| 0‒100 | 101‒200 | 201‒300 | ... | 102301‒102400 |
| --- | --- | --- | --- | --- |
| 1 | 0 | 0 | ... | 1 |

Execute the following SQL statement:
```sql
select * from table where id>199 and id<299;
```
No value in the Data Pack is hit. Therefore, the Data Pack is irrelevant and filtered out.
##### **CMAP**
Character Maps (CMAPs) are binary representation of the occurrence of ASCII characters within the first 64 row positions. If a character exists in a position, the position is marked with 1 for the character. Otherwise, the position is marked with 0 for the character. CMAPs are automatically created when data is being loaded.

In the following example, character A exists in position 1 and position 64.

| Char/Char pos | 1 | 2 | ... | 64 |
| --- | --- | --- | --- | --- |
| A | 1 | 0 | ... | 1 |
| B | 0 | 1 | ... | 0 |
| C | 1 | 1 | ... | 1 |
| ... | ... | ... | ... | ... |

##### **Pack-to-Pack**
Pack-to-Packs are equijoin relations between the pairs of tables. Pack-to-Pack is a binary matrix. If a matching pair is found between two Data Packs, the value is 1. Otherwise, the value is 0. Using Pack-to-Packs can help quickly identify relevant Data Packs, improving join performance. Pack-to-Packs are automatically created when join queries are being executed.

In the following example, the condition for joining tables is `A.C=B.D`. For Data Pack A.C1, only Data Packs B.D2 and B.D5 contain matching values.

|  | B.D1 | B.D2 | B.D3 | B.D4 | B.D5 |
| --- | --- | --- | --- | --- | --- |
| A.C1 | 0 | 1 | 0 | 0 | 1 |
| A.C2 | 1 | 1 | 0 | 0 | 0 |
| A.C3 | 1 | 1 | 0 | 1 | 1 |

#### **Knowledge Grid**
The Knowledge Grid consists of Data Pack Nodes and Knowledge Nodes. Data Packs are compressed for storage and the cost for decompressing Data Packs is high. Therefore, the key to improving read performance is to retrieve as few as Data Packs. The Knowledge Grid can help filter out irrelevant data. With the Knowledge Gid, the data retrieved can be reduced to less than 1% of the total data. In most cases, the data retrieved can be loaded to memory so that the query processing efficiency can be further improved.

For most statistical and aggregate queries, StoneDB can return query results by using only the Knowledge Grid. In this way, the number of Data Packs to be decompressed is greatly reduced, saving I/O resources, minimizing the response time, and improving the network utilization.

Following is an example showing how the Knowledge Grid works.

The following table shows the distribution of values recorded in Data Pack Nodes.

|  | Min. | Max. |
| --- | --- | --- |
| t1.A1 | 1 | 9 |
| t1.A2 | 10 | 30 |
| t1.A3 | 40 | 100 |

The following SQL statement is executed.
```sql
select min(t2.D) from t1,t2 where t1.B=t2.C and t1.A>15;
```
The working process of the Knowledge Grid is as follows:

1. Filter Data Packs based on Data Pack Nodes: data pack t1.A1 is irrelevant, t1.A2 is suspect, and t1.A3 is relevant. Therefore, t1.A1 is filtered out.
|  | t2.C1 | t2.C2 | t2.C3 | t2.C4 | t2.C5 |
| --- | --- | --- | --- | --- | --- |
| t1.B1 | 1 | 1 | 1 | 0 | 1 |
| t1.B2 | 0 | 1 | 0 | 0 | 0 |
| t1.B3 | 1 | 1 | 0 | 0 | 1 |


2. Compare t1.B1 and t2.C1 to check whether matching pairs exist based on pack-to-packs. In this step, Data Packs t2.C2 and t2.C5 contain matching pairs while Data Packs t2.C3 and t2.C4 are filtered out.
|  | Min. | Max. |
| --- | --- | --- |
| t2.D1 | 0 | 500 |
| t2.D2 | 101 | 440 |
| t2.D3 | 300 | 6879 |
| t2.D4 | 1 | 432 |
| t2.D5 | 3 | 100 |

3. Examine Data Packs D2 and D5, after D1, D3, and D4 are filtered out in the previous two steps. Based on the Data Pack Nodes of column D, the maximum value in D5 is 100, which is smaller than the minimum value 101 in D2. Therefore, D2 is filtered out. Now, the system only needs to decompress data pack D5 to obtain the final result.
### StoneDB Loader Parser
StoneDB Loader Parser is a module responsible for data import and export. It processes `LOAD DATA INFILE` and `SELECT … INTO FILE` operations.
### Insert Buffer
The Insert Buffer is used to optimize insert performance. When you insert data to a table, the data to insert is first temporarily stored in Insert Buffer and then flushed from Insert Buffer to disks in batches. This improves system throughput. If the data is directly written into disks, the data is written one row after another because StoneDB does not support transactions. As a result, the system throughput is low and thus the insertion efficiency is low. Insert Buffer is enabled by default. If you want to disable it, set parameter** stonedb_insert_delayed** to **off**.
### Replication Manager
The high-availability structure of StoneDB includes a replication engine called Replication Manager to ensure strong consistency between the primary and secondary databases. Different from binlog replication used by MySQL to replicate original data, Replication Manager can directly replicate compressed data since data stored in StoneDB is compressed, without the need for decompression. This greatly reduces the traffic required for transmitting data.
### Compress
Compress is the module for compressing data and supports more than 20 compression algorithms such as PPM, LZ4, B2, and Delta. In StoneDB, data is stored by column. Since data records stored in a column are of the same data type, StoneDB can dynamically choose the most efficient compression algorithm based on the data type defined for the column. More repeated values in a column indicates higher compression ratio of the column. Compression not only saves storage space but also I/O and memory resources.
### Decompress
Decompress is the module for decompressing data. The unit for compression and decompression is Data Pack. With the Knowledge Grid technique, StoneDB first filters out irrelevant Data Packs, and then decompresses and examines suspect Data Packs, and then obtains the final query result.