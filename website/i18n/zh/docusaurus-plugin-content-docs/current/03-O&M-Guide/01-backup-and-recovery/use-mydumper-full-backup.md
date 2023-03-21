---
id: use-mydumper-full-backup
sidebar_position: 4.32
---

# MySQL全量数据备份-mydumper

mydumper 项目地址：[https://github.com/mydumper/mydumper](https://github.com/mydumper/mydumper)
## mydumper 介绍
### 什么是 mydumper？
mydumper 是一个 MySQL 逻辑备份工具，它有 2 个工具：

- mydumper 负责导出数据。
- myloader 从 mydumper 读取备份，连接到目标数据库并导入备份，两种工具都可以使用多线程。
### mydumper 优势

- 并行性和性能
- 更易于管理输出，表的单独文件、转储元数据等，易于查看/解析数据
- 一致性，维护所有线程的快照，提供准确的主从日志位置等
- 可管理性，支持 PCRE 以指定数据库和表的包含和排除
### mydumper 主要特性

- 多线程备份，备份后会生成多个备份文件
- 事务性和非事务性表一致的快照（适用于0.2.2以上版本）
- 快速的文件压缩
- 支持导出 binlog
- 多线程恢复（适用于0.2.1以上版本）
- 以守护进程的工作方式，定时快照和连续二进制日志（适用于0.5.0以上版本）
- 开源（GNU GPLv3）
## mydumper使用
### mydumer 参数
```bash
mydumper --help
Usage:
mydumper [OPTION…] multi-threaded MySQL dumping

Help Options:
-?, --help                      Show help options

Application Options:
-B, --database                  Database to dump
-o, --outputdir                 Directory to output files to
-s, --statement-size            Attempted size of INSERT statement in bytes, default 1000000
-r, --rows                      Try to split tables into chunks of this many rows. This option turns off --chunk-filesize
-F, --chunk-filesize            Split tables into chunks of this output file size. This value is in MB
--max-rows                      Limit the number of rows per block after the table is estimated, default 1000000
-c, --compress                  Compress output files
-e, --build-empty-files         Build dump files even if no data available from table
-i, --ignore-engines            Comma delimited list of storage engines to ignore
-N, --insert-ignore             Dump rows with INSERT IGNORE
-m, --no-schemas                Do not dump table schemas with the data and triggers
-M, --table-checksums           Dump table checksums with the data
-d, --no-data                   Do not dump table data
--order-by-primary              Sort the data by Primary Key or Unique key if no primary key exists
-G, --triggers                  Dump triggers. By default, it do not dump triggers
-E, --events                    Dump events. By default, it do not dump events
-R, --routines                  Dump stored procedures and functions. By default, it do not dump stored procedures nor functions
-W, --no-views                  Do not dump VIEWs
-k, --no-locks                  Do not execute the temporary shared read lock.  WARNING: This will cause inconsistent backups
--no-backup-locks               Do not use Percona backup locks
--less-locking                  Minimize locking time on InnoDB tables.
--long-query-retries            Retry checking for long queries, default 0 (do not retry)
--long-query-retry-interval     Time to wait before retrying the long query check in seconds, default 60
-l, --long-query-guard          Set long query timer in seconds, default 60
-K, --kill-long-queries         Kill long running queries (instead of aborting)
-D, --daemon                    Enable daemon mode
-X, --snapshot-count            number of snapshots, default 2
-I, --snapshot-interval         Interval between each dump snapshot (in minutes), requires --daemon, default 60
-L, --logfile                   Log file name to use, by default stdout is used
--tz-utc                        SET TIME_ZONE='+00:00' at top of dump to allow dumping of TIMESTAMP data when a server has data in different time zones or data is being moved between servers with different time zones, defaults to on use --skip-tz-utc to disable.
--skip-tz-utc
--use-savepoints                Use savepoints to reduce metadata locking issues, needs SUPER privilege
--success-on-1146               Not increment error count and Warning instead of Critical in case of table doesn't exist
--lock-all-tables               Use LOCK TABLE for all, instead of FTWRL
-U, --updated-since             Use Update_time to dump only tables updated in the last U days
--trx-consistency-only          Transactional consistency only
--complete-insert               Use complete INSERT statements that include column names
--split-partitions              Dump partitions into separate files. This options overrides the --rows option for partitioned tables.
--set-names                     Sets the names, use it at your own risk, default binary
-z, --tidb-snapshot             Snapshot to use for TiDB
--load-data
--fields-terminated-by
--fields-enclosed-by
--fields-escaped-by             Single character that is going to be used to escape characters in theLOAD DATA stament, default: '\'
--lines-starting-by             Adds the string at the begining of each row. When --load-data is usedit is added to the LOAD DATA statement. Its affects INSERT INTO statementsalso when it is used.
--lines-terminated-by           Adds the string at the end of each row. When --load-data is used it isadded to the LOAD DATA statement. Its affects INSERT INTO statementsalso when it is used.
--statement-terminated-by       This might never be used, unless you know what are you doing
--sync-wait                     WSREP_SYNC_WAIT value to set at SESSION level
--where                         Dump only selected records.
--no-check-generated-fields     Queries related to generated fields are not going to be executed.It will lead to restoration issues if you have generated columns
--disk-limits                   Set the limit to pause and resume if determines there is no enough disk space.Accepts values like: '<resume>:<pause>' in MB.For instance: 100:500 will pause when there is only 100MB free and willresume if 500MB are available
--csv                           Automatically enables --load-data and set variables to export in CSV format.
-t, --threads                   Number of threads to use, default 4
-C, --compress-protocol         Use compression on the MySQL connection
-V, --version                   Show the program version and exit
-v, --verbose                   Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, default 2
--defaults-file                 Use a specific defaults file
--stream                        It will stream over STDOUT once the files has been written
--no-delete                     It will not delete the files after stream has been completed
-O, --omit-from-file            File containing a list of database.table entries to skip, one per line (skips before applying regex option)
-T, --tables-list               Comma delimited table list to dump (does not exclude regex option)
-h, --host                      The host to connect to
-u, --user                      Username with the necessary privileges
-p, --password                  User password
-a, --ask-password              Prompt For User password
-P, --port                      TCP/IP port to connect to
-S, --socket                    UNIX domain socket file to use for connection
  -x, --regex                     Regular expression for 'db.table' matching
```
### myloader参数
```bash
myloader --help
Usage:
  myloader [OPTION…] multi-threaded MySQL loader

Help Options:
  -?, --help                        Show help options

Application Options:
  -d, --directory                   Directory of the dump to import
  -q, --queries-per-transaction     Number of queries per transaction, default 1000
  -o, --overwrite-tables            Drop tables if they already exist
  -B, --database                    An alternative database to restore into
  -s, --source-db                   Database to restore
  -e, --enable-binlog               Enable binary logging of the restore data
  --innodb-optimize-keys            Creates the table without the indexes and it adds them at the end
  --set-names                       Sets the names, use it at your own risk, default binary
  -L, --logfile                     Log file name to use, by default stdout is used
  --purge-mode                      This specify the truncate mode which can be: NONE, DROP, TRUNCATE and DELETE
  --disable-redo-log                Disables the REDO_LOG and enables it after, doesn't check initial status
  -r, --rows                        Split the INSERT statement into this many rows.
  --max-threads-per-table           Maximum number of threads per table to use, default 4
  --skip-triggers                   Do not import triggers. By default, it imports triggers
  --skip-post                       Do not import events, stored procedures and functions. By default, it imports events, stored procedures nor functions
  --no-data                         Do not dump or import table data
  --serialized-table-creation       Table recreation will be executed in serie, one thread at a time
  --resume                          Expect to find resume file in backup dir and will only process those files
  -t, --threads                     Number of threads to use, default 4
  -C, --compress-protocol           Use compression on the MySQL connection
  -V, --version                     Show the program version and exit
  -v, --verbose                     Verbosity of output, 0 = silent, 1 = errors, 2 = warnings, 3 = info, default 2
  --defaults-file                   Use a specific defaults file
  --stream                          It will stream over STDOUT once the files has been written
  --no-delete                       It will not delete the files after stream has been completed
  -O, --omit-from-file              File containing a list of database.table entries to skip, one per line (skips before applying regex option)
  -T, --tables-list                 Comma delimited table list to dump (does not exclude regex option)
  -h, --host                        The host to connect to
  -u, --user                        Username with the necessary privileges
  -p, --password                    User password
  -a, --ask-password                Prompt For User password
  -P, --port                        TCP/IP port to connect to
  -S, --socket                      UNIX domain socket file to use for connection
  -x, --regex                       Regular expression for 'db.table' matching
  --skip-definer                    Removes DEFINER from the CREATE statement. By default, statements are not modified

```
### 安装使用
```bash
# 到项目 github 上下载机器对应的 rpm 包或者源码包，源码包需要进行编译，rpm 包安装简单建议使用，本文以 CentOS 7 系统为例，所以下载 el7 版本
[root@dev tmp]# wget https://github.com/mydumper/mydumper/releases/download/v0.12.1/mydumper-0.12.1-1-zstd.el7.x86_64.rpm
# 由于下载的 mydumper 是 zstd 类型的，所以需要下载 libzstd 依赖
[root@dev tmp]# yum install libzstd.x86_64 -y
[root@dev tmp]# rpm -ivh mydumper-0.12.1-1-zstd.el7.x86_64.rpm
Preparing...                          ################################# [100%]
Updating / installing...
   1:mydumper-0.12.1-1                ################################# [100%]

# 备份库
[root@dev home]# mydumper -u root -p xxx -P 3306 -h 127.0.0.1 -B zz -o /home/dumper/
# 恢复库
[root@dev home]# myloader -u root -p xxx -P 3306 -h 127.0.0.1 -S /stonedb/install/tmp/mysql.sock -B zz -d /home/dumper
```
**备份所生成的文件** 
```bash
[root@dev home]# ll dumper/
total 112
-rw-r--r--. 1 root root   139 Mar 23 14:24 metadata
-rw-r--r--. 1 root root    88 Mar 23 14:24 zz-schema-create.sql
-rw-r--r--. 1 root root 97819 Mar 23 14:24 zz.t_user.00000.sql
-rw-r--r--. 1 root root     4 Mar 23 14:24 zz.t_user-metadata
-rw-r--r--. 1 root root   477 Mar 23 14:24 zz.t_user-schema.sql
[root@dev dumper]# cat metadata
Started dump at: 2022-03-23 15:51:40
SHOW MASTER STATUS:
        Log: mysql-bin.000002
        Pos: 4737113
        GTID:

Finished dump at: 2022-03-23 15:51:40
[root@dev-myos dumper]# cat zz-schema-create.sql
CREATE DATABASE /*!32312 IF NOT EXISTS*/ `zz` /*!40100 DEFAULT CHARACTER SET utf8 */;
[root@dev dumper]# more zz.t_user.00000.sql
/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;
/*!40103 SET TIME_ZONE='+00:00' */;
INSERT INTO `t_user` VALUES(1,"e1195afd-aa7d-11ec-936e-00155d840103","kAMXjvtFJym1S7PAlMJ7",102,62,"2022-03-23 15:50:16")
,(2,"e11a7719-aa7d-11ec-936e-00155d840103","0ufCd3sXffjFdVPbjOWa",698,44,"2022-03-23 15:50:16")
.....#内容过多不全部展示
[root@dev dumper]# cat zz.t_user-metadata
10000
[root@dev-myos dumper]# cat zz.t_user-schema.sql
/*!40101 SET NAMES binary*/;
/*!40014 SET FOREIGN_KEY_CHECKS=0*/;

/*!40103 SET TIME_ZONE='+00:00' */;
CREATE TABLE `t_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `c_user_id` varchar(36) NOT NULL DEFAULT '',
  `c_name` varchar(22) NOT NULL DEFAULT '',
  `c_province_id` int(11) NOT NULL,
  `c_city_id` int(11) NOT NULL,
  `create_time` datetime NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_user_id` (`c_user_id`)
) ENGINE=InnoDB AUTO_INCREMENT=10001 DEFAULT CHARSET=utf8;
```

目录

metadata 文件

   - 记录了备份数据库在备份时间点的二进制日志文件名，日志的写入位置，
   - 如果是在从库进行备份，还会记录备份时同步至主库的二进制日志文件及写入位置 

每个表有两个备份文件：

- database-schema-create 库创建语句文件

- database.table-schema.sql 表结构文件

- database.table.00000.sql 表数据文件

- database.table-metadata 表元数据文件

### 备份原理

   - 主线程 FLUSH TABLES WITH READ LOCK，施加全局只读锁，保证数据的一致性 
   - 读取当前时间点的二进制日志文件名和日志写入的位置并记录在 metadata 文件中，以供全量恢复后追加 binlog 恢复使用 
   - N个（线程数可以指定，默认是4）dump 线程把事务隔离级别改为可重复读 并开启一致性读事务
   - dump non-InnoDB tables, 首先导出非事物引擎的表 
   - 主线程 UNLOCK TABLES 非事物引擎备份完后，释放全局只读锁 
   - dump InnoDB tables，基于事物导出 InnoDB 表 
   - 事物结束

