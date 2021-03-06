---
id: intro
sidebar_position: 1.1
---

# StoneDB 简介

StoneDB是由石原子科技公司自主设计、研发的国内首款基于MySQL内核打造的开源HTAP（Hybrid Transactional and Analytical Processing）融合型数据库，可实现与MySQL的无缝切换。StoneDB具备超高性能、实时分析等特点，为用户提供一站式HTAP解决方案。

StoneDB是100%兼容MySQL 5.6、5.7协议和MySQL生态等重要特性，支持MySQL常用的功能及语法，支持MySQL生态中的系统工具和客户端，如Navicat、Workbench、mysqldump、mydumper。由于100%兼容MySQL，因此StoneDB的所有工作负载都可以继续使用MySQL数据库体系运行。

StoneDB专门针对OLAP应用程序进行了设计和优化，支持百亿数据场景下进行高性能、多维度字段组合的复杂查询，相对比社区版的MySQL，其查询速度提升了十倍以上。

StoneDB采用基于知识网格技术和列式存储引擎，该存储引擎为海量数据背景下OLAP应用而设计，通过列式存储数据、知识网格过滤、高效数据压缩等技术，为应用系统提供低成本和高性能的数据查询支持。

# 产品优势

- 完全兼容MySQL

StoneDB是在原生的MySQL中加入的存储引擎，最终集成为HTAP数据库，因此是100%兼容MySQL的。支持标准数据库接口，包括ODBC、JDBC和本地连接。支持API接口，包括C、C++、C#、Java、PHP、Perl等。StoneDB全面支持ANSI SQL-92标准和SQL-99扩展标准中视图和存储过程，这种支持使得现有应用程序无需修改应用代码即可使用StoneDB，从而可实现与MySQL的无缝切换。

- 实时HTAP

同时提供行式存储引擎InnoDB和列式存储引擎StoneDB，通过binlog从InnoDB复制数据，保证行式存储引擎InnoDB和列式存储引擎StoneDB之间的数据强一致性。

- 高性能查询

在千万、亿级甚至更多数据量下进行复杂查询时，相比MySQL，其查询速度提升了十倍以上。

- 低存储成本

对数据最高可实现40倍压缩，极大的节省了数据存储空间和企业的成本。

# 核心技术

- 列式存储

列式存储在存储数据时是按照列模式将数据存储到磁盘上的，读取数据时，只需要读取需要的字段，极大减少了网络带宽和磁盘IO的压力。基于列式存储无需为列再创建索引和维护索引。

- 高效的数据压缩比

在关系型数据库中，同一列中的数据属于同一种数据类型，如果列中的重复值越多，则压缩比越高，而压缩比越高，数据量就越小，数据被读取时，对网络带宽和磁盘IO的压力也就越小。由于列式存储比行式存储有着十倍甚至更高的压缩比，StoneDB节省了大量的存储空间，降低了存储成本。

- 知识网格

知识网格根据元数据信息进行粗糙集过滤查询中不符合条件的数据，最后只需要对可疑的数据包进行解压获取符合查询条件的数据，大量减少读取IO操作，提高查询响应时间和网络利用率。

- 基于推送的矢量化查询处理

StoneDB通过执行计划将矢量块（列式数据切片）从一个运算符推送到另一个运算符来处理查询，与基于元组的处理模型相比，基于推送的执行模型避免了深度调用堆栈，并节省了资源。