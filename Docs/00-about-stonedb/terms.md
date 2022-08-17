---
id: terms
sidebar_position: 1.4
---

# Terms

The following table provides descriptions of common terms.

| **Term** | **Description** |
| --- | --- |
| **row** | A series of data that makes up a record. |
| **column** | Also referred to as field. In a relational database, a field must be associated with a data type when the field is being created. |
| **table** | Consists of rows and columns. Databases use tables to store data. Tables are essential objects in databases. |
| **view** | A virtual table that does not store actual data. It is based on the result set of an SQL statement. |
| **stored procedure** | A collection of one or more SQL statements that are compiled and then stored in a database to execute a specific operation. To execute a stored procedure, you need to specify the name and required parameters of the stored procedure. |
| **database** | A collection of database objects such as tables, views, and stored procedures. |
| **instance** | A collection of databases. |
| **data page** | The basic unit for database management. The default size for a data page is 16 KB. |
| **data file** | Used for storing data. By default, one table corresponds to one data file. |
| **tablespace** | A logical storage unit. By default, one table corresponds to one tablespace. |
| **transaction** | A sequence of DML operations. This sequence satisfies the atomicity, consistency, isolation, and durability (ACID) properties. A transaction must end with a submission or rollback. Implicit submission by using DDL statements are supported. |
| **character set** | A collection of symbols and encodings. |
| **collation** | A collation is a collection of rules for comparing and sorting character strings. |
| **column-based storage** | Stores data by column to disks. |
| **data compression** | A process performed to reduce the size of data files. The data compression ratio is determined by the data type, degree of duplication, and compression algorithm. |
| **OLTP** | The acronym of online transaction processing. OLTP features quick response for interactions and high concurrency of small transactions. Typical applications are transaction systems of banks. |
| **OLAP** | The acronym of online analytical processing. OLAP features complex analytical querying on a large amount of data. Typical applications are data warehouses. |
| **HTAP** | The acronym of hybrid transaction/analytical processing. HTAP is an emerging application architecture built to allow one system for both transactions and analytics. |
| **Data Pack** | Data Packs are data storage units. Data in each column is sliced into Data Packs every 65,536 rows.  |
| **Data Pack Node** | A Data Pack Node stores the following information about a Data Pack:<br />- The maximum, minimum, average, and sum of the values<br />- The number of values and the number of non-null values<br />- The compression method<br />- The length in bytes <br /> |
| **Knowledge Node** | Knowledge Nodes are at the upper layer of Data Pack Nodes. Knowledge Nodes store a collection of metadata that shows the relations between Data Packs and columns, including the range of value occurrence, data characteristics, and certain statistics. Most data stored in a Knowledge Node is generated when data is being loaded and the rest is generated during queries. |
| **Knowledge Grid** | The Knowledge Grid consists of Data Pack Nodes and Knowledge Nodes. Data Packs are compressed for storage and the cost for decompressing Data Packs is high. Therefore, the key to improving read performance is to retrieve as few as Data Packs. Knowledge Grid can help filter out irrelevant data. With Knowledge Grid, the data retrieved can be reduced to less than 1% of the total data. In most cases, the data retrieved can be loaded to memory so that the query processing efficiency can be further improved. |
