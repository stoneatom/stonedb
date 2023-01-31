<div align="center">

[![logo](Docs/stonedb_logo.png)](https://stonedb.io/)

<h3 align="center"><strong>一体化实时 HTAP 数据库</strong></h3>

 [English](README.md) | [**中文**](README_zh-hans.md) 

</br>

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/stoneatom/stonedb/pull_requests.yml?branch=stonedb-5.7-dev)](https://github.com/stoneatom/stonedb/actions)
[![codecov](https://codecov.io/gh/stoneatom/stonedb/branch/stonedb-5.7-dev/graph/badge.svg?token=NWJYOOZ2S5)](https://codecov.io/gh/stoneatom/stonedb)
[![Docker Pulls](https://img.shields.io/docker/pulls/stoneatom/stonedb)](https://hub.docker.com/r/stoneatom/stonedb)
[![GitHub license](https://img.shields.io/github/license/stoneatom/stonedb)](https://github.com/stoneatom/stonedb/blob/stonedb-5.6/LICENSE)
[![slack badge](https://img.shields.io/badge/Slack-Join%20StoneDB-blueviolet?logo=slack&amp)](https://stonedb.slack.com/join/shared_invite/zt-1ba2lpvbo-Vqq62DJcxViyxCZmp7Rimw#/shared-invite/email)
[![WeChat badge](https://img.shields.io/badge/Wechat-join-green?logo=wechat&amp)](https://cms.stoneatom.com/assets/8f44fbdf-b987-44fb-8b8d-c65a37da9221.jpg)
[![Twitter Follow](https://img.shields.io/twitter/follow/StoneDataBase?style=social)](https://twitter.com/intent/follow?screen_name=StoneDataBase)

</div>

- [StoneDB 是什么？](#stonedb-是什么)
- [贡献指南](#贡献指南)
- [快速开始](#快速开始)
  - [支持平台](#支持平台)
  - [通过源码编译 StoneDB](#通过源码编译-stonedb)
    - [Ubuntu 20.04 下编译 StoneDB](#ubuntu-2004-下编译-stonedb)
    - [CentOS 7.x 下编译 StoneDB](#centos-7x-下编译-stonedb)
    - [RedHat 7.x 下编译 StoneDB](#redhat-7x-下编译-stonedb)
  - [在 Docker 容器中通过源码编译 StoneDB](#在-docker-容器中通过源码编译-stonedb)
  - [配置 StoneDB](#配置-stonedb)
  - [初始化数据库](#初始化数据库)
  - [启动数据库实例](#启动数据库实例)
  - [用 StoneDB 创建表](#用-stonedb-创建表)
  - [在生产环境中从 MySQL 切换到 StoneDB](#在生产环境中从-mysql-切换到-stonedb)
- [文档](#文档)
- [论坛](#论坛)
- [加入 StoneDB 用户群](#加入-stonedb-用户群)
- [行为准则](#行为准则)

# StoneDB 是什么？

[![logo](Docs/stonedb_github_logo.png)](https://stonedb.io/)


StoneDB 是一个兼容 MySQL 的高性能混合事务/分析处理(HTAP)数据库。它为 MySQL 提供了分析处理(AP)能力。运行中的系统可以无缝地迁移到 StoneDB，而无需更改任何代码。与 InnoDB 相比，StoneDB 提供了 10 倍的查询性能和 10 倍的加载性能。StoneDB 还提供 10:1 到 40:1 的压缩比。

<p align="center"> <b>概览</b> </p>

[![logo](Docs/stonedb_overview.png)](https://stonedb.io/zh/docs/about-stonedb/intro)


<p align="center"> <b>架构图 2.0</b> </p>

[![logo](Docs/stonedb_architecture.png)](https://stonedb.io/zh/docs/about-stonedb/architecture)


想获取更多关于 StoneDB 的信息，可以前往官网：[stonedb.io](https://stonedb.io/zh/)。

# 贡献指南

StoneDB 社区欢迎各种各样的贡献，如为代码库贡献代码，分享您如何使用StoneDB的经验，并在论坛上在社区中提供见解，或贡献项目，使 StoneDB 成为一个更好的开源项目。想获取更多的细节，可以看这里：[贡献指南](https://github.com/stoneatom/stonedb/blob/stonedb-5.7-dev/CONTRIBUTING.md)。

# 快速开始

入门部分提供了关于StoneDB支持的平台，安装(包括创建您的第一个表)，以及从运行的MySQL数据库迁移到StoneDB的信息。

## 支持平台

目前我们支持以下操作系统平台:

- CentOS 7.x 及以上
- Ubuntu 20.04 及以上
- Red Hat Enterprise Linux 7 (RHEL 7.x)

我们用来验证构建的编译器工具集：

- GCC 9.3.0

以下是我们用来验证构建的包:

- Make 3.82 or later
- CMake 3.7.2 or later
- marisa 0.77
- RocksDB 6.12.6
- Boost 1.66

## 通过源码编译 StoneDB

### Ubuntu 20.04 下编译 StoneDB

更多信息，可以查看 [Compile StoneDB on Ubuntu 20.04](https://stonedb.io/zh/docs/developer-guide/compiling-methods/compile-using-ubuntu2004/compile-using-ubuntu20.04-for-57/)。

### CentOS 7.x 下编译 StoneDB

更多信息，可以查看 [Compile StoneDB on CentOS 7](https://stonedb.io/zh/docs/developer-guide/compiling-methods/compile-using-centos7/compile-using-centos7-57)。

### RedHat 7.x 下编译 StoneDB
更多信息，可以查看 [Compile StoneDB on RHEL 7](https://stonedb.io/zh/docs/developer-guide/compiling-methods/compile-using-redhat7/compile-using-redhat7-for-57)。

## 在 Docker 容器中通过源码编译 StoneDB

更多信息，可以查看 [Compile StoneDB in a Docker Container](https://stonedb.io/zh/docs/developer-guide/compiling-methods/compile-using-docker)。

## 配置 StoneDB

StoneDB安装完成后，您至少需要在 `my.cnf` 文件中配置以下参数：

```
#the stonedb configuration options are listed as following.
#for an example.
[mysqld] 
# 如果是5.7及以后的版本，引擎设置为tianmu
default-storage-engine=tianmu
# 如果是5.6版本，引擎要设置为stonedb
# default-storage-engine=stonedb
binlog-format=STATEMENT
```

## 初始化数据库

```bash
# 对于 5.6 版本
cd /path/to/your/path/bin && ./mysql_install_db --basedir=/stonedb/install/ --datadir=/stonedb/install/data/ --user=mysql

# 对于 5.7 及以后的版本
cd /path/to/your/path/bin && ./mysqld --initialize --basedir=/stonedb/install/ --datadir=/stonedb/install/data/ --user=mysql
```

## 启动数据库实例

```bash
mysqld_safe --defaults-file=/path/to/my.cnf --user=mysql & 
```

## 用 StoneDB 创建表

```sql
--The example code for creating a table with 'tianmu' engine.(For version 5.7 or later)
CREATE TABLE `example_table` (
  `id1` bigint(20) NOT NULL DEFAULT '0',
  `id1_type` int(10) NOT NULL DEFAULT '0',
  `id2` bigint(20) NOT NULL DEFAULT '0',
  `id2_type` int(10) NOT NULL DEFAULT '0',
  `data` varchar(255) NOT NULL DEFAULT '',
  `time` bigint(20) NOT NULL DEFAULT '0',
  `version` int(11) NOT NULL DEFAULT '0',
) ENGINE=tianmu;
-- For version 5.6, the engine should be set to 'stonedb'
```

这个例子展示了 StoneDB 中的一些重要特性和限制。有关限制的更多信息，请参见 [StoneDB Limitations](https://stonedb.io/zh/docs/about-stonedb/limits)。

- StoneDB 数据以列格式存储，并持久化到 RocksDB 中，RocksDB 作为磁盘按列存储格式化后的数据。所有数据会被压缩，压缩比为10:1 ~ 40:1。
- 在处理特殊查询时，即使不创建任何索引，StoneDB 也能达到具有竞争力的性能。要了解更多信息，请点击[这里](https://stonedb.io/docs/about-stonedb/architecture)。

--- 

<h3 align="center">
<strong>现在，让我们快速体验 StoneDB</strong>
</h3>
</br>

## 在生产环境中从 MySQL 切换到 StoneDB

如果您想在同一个实例中同时使用 InnoDB 和 Tianmu 来运行连接查询，在文件**my.cnf**中设置**stonedb_ini_allowmysqlquerypath**为**1**。

目前还没有开发在存储引擎之间移动数据的在线迁移工具，但是您显然希望在不停机、不丢失数据或不准确结果的情况下进行迁移。为了实现这一点，您需要从使用 InnoDB 引擎的源 MySQL 服务器上逻辑地移动数据，并将其加载到 Tianmu 中进行分析处理。具体流程如下:

1. 创建一个StoneDB实例和表。

2. 将所有数据库和表模式从源复制到目标。

3. 通过执行 `SELECT…INTO OUTFILE` 将每个表转储到一个文件。

4. 将文件发送到目的地，并使用 `load DATA…INFILE` 加载它们。

# 文档

相关文档可在[https://stonedb.io](https://stonedb.io/zh/docs/about-stonedb/intro) 上找到。该文档为您提供了 StoneDB 基础知识，使用 StoneDB 的广泛示例，以及您在使用 StoneDB 期间可能需要的其他信息。

# 论坛
[GitHub讨论](https://github.com/stoneatom/stonedb/discussions) 是大多数关于 StoneDB 项目的讨论和交流的主页。欢迎您的参与。欢迎并重视您的每一个意见或建议。我们期待 StoneDB 成为一个开放且有影响力的项目。

# 加入 StoneDB 用户群
您可以扫码加入我们的微信用户群：

<img src="Docs/stonedb_wecaht_group.png" width="50%">


# 行为准则
参加StoneDB项目时，请确保您的所有行为都符合[行为准则](https://github.com/stoneatom/stonedb/blob/stonedb-5.7-dev/CODE_OF_CONDUCT.md) 。

