---
id: intro
sidebar_position: 1.1
---
# StoneDB Introduction

StoneDB is an open-source hybrid transaction/analytical processing (HTAP) database designed and developed by StoneAtom based on the MySQL kernel. It is the first database of this type launched in China. StoneDB can be seamlessly switched from MySQL. It provides features such as optimal performance and real-time analytics, offering you a one-stop solution to process online transaction processing (OLTP), online analytical processing (OLAP), and HTAP workloads.

StoneDB is fully compatible with the MySQL 5.6 and 5.7 protocols, the MySQL ecosystem, and common MySQL features and syntaxes. You can use tools and clients in the MySQL ecosystem on StoneDB, such as Navicat, Workbench, mysqldump, and mydumper. In addition, all workloads on StoneDB can be run on MySQL.

StoneDB is optimized for OLAP applications. StoneDB that runs on a common server can process complex queries on tens of billions of data records, while ensuring high performance. Compared to databases that use MySQL Community Edition, StoneDB is at least 10 times faster in processing queries.

StoneDB uses the knowledge grid technology and a column-based storage engine. This storage engine is designed for OLAP applications and uses techniques such as column-based storage, knowledge grid-based filtering, and high-efficiency data compression. With such storage engine, StoneDB provides application systems with high-performance and reduces the total cost of ownership (TCO).

## Advantages
### Integration of MySQL
StoneDB is an HTAP database built on MySQL. To enhance analytics capabilities, it integrates a self-developed engine also named StoneDB. (In this topic, StoneDB refers to the database, if not otherwise specified.) For this reason, StoneDB is fully compatible with MySQL. You can use standard interfaces, such as Open Database Connectivity (ODBC) and Java Database Connectivity (JDBC) to connect to StoneDB. In addition, you can create local connections to connect to StoneDB. StoneDB supports APIs written in various programming languages, such as C, C++, C#, Java, PHP, and Perl. StoneDB is fully compatible with views and stored procedures that comply with the ANSI SQL-92 standard and the SQL-99 standard. In this way, your application systems that can run on MySQL can directly run on StoneDB, without the need to modify the code. This allows you to seamlessly switch MySQL to StoneDB.

### Real-time HTAP
StoneDB provides two engines: row-based storage engine InnoDB and column-based storage engine StoneDB. StoneDB uses binlogs to replicate data from the row-based storage engine to the column-based storage engine in real time. This ensures strong data consistency between the two storage engines.

## Key techniques
### Column-based storage engine
A column-based storage engine stores data to disks column by column. When you query data, only the required fields are retrieved, which greatly reduces memory bandwidth traffic and disk I/O. In addition, in a column-based storage engine, columns do not need to be indexed, freeing the database from maintaining such indexes.

### High-efficiency data compression
In a relational database, values in the same column are of the same data type. More duplicate values stored in a column indicate a higher data compression ratio and a smaller data volume. By virtue of this, less data is retrieved for queries, and thus memory bandwidth traffic and disk I/O are reduced. 

StoneDB saves storage space by using column-based storage. The data compression ratio of a column-oriented database is at least 10 times higher than that of a row-oriented database. 
### Knowledge grid
A knowledge grid can filter data packs based on metadata, and then decompress the data packs to obtain the data that meets the query conditions. This greatly reduces I/O, and improves response speed and network utilization.
### Push-based vectorized query execution
When processing a query, StoneDB pushes column-based data packs from one operator to another based on the execution plan. Compared to the execution model used by row-oriented databases, push-based execution prevents in-depth calls of stacks and saves resources.

