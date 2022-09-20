---
id: use-mydumper-full-backup
sidebar_position: 4.32
---

# Use Mydumper for Full Backup

## Mydumper introduction
Mydumper is a logical backup tool for MySQL. It consists of two parts:

- mydumper: exports consistent backup files of MySQL databases.
- myloader: reads backups from mydumper, connects to destination databases, and imports backups. 

Both parts require multithreading capacities.
### Benefits

- Parallelism and performance: The tool provides high backup rate. Expensive character set conversion routines are avoided and the overall high efficiency of code is ensured.
- Simplified output management: Separate files used for tables and metadata is dumped, simplifying data view and parse.
- High consistency: The tool maintains snapshots across all threads and provides accurate positions of primary and secondary logs.
- Manageability: Perl Compatible Regular Expressions (PCRE) can be used to specify whether to include or exclude tables or databases.


### Features
- Multi-threaded backup, which generates multiple backup files
- Consistent snapshots for transactional and non-transactional tables

:::info
This feature is supported by versions later than 0.2.2.
:::

- Fast file compression
- Export of binlogs
- Multi-threaded recovery

:::info
This feature is supported by versions later than 0.2.1.
:::

- Function as a daemon to periodically perform snapshots and consistently records binlogs

:::info

This feature is supported by versions later than 0.5.0.

:::

- Open source (license: GNU GPLv3)
## Use Mydumper
### Parameters for mydumer

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
--fields-escaped-by             Single character that is going to be used to escape characters in the LOAD DATA stament, default: '\'
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
### Parameters for myloader
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
### Install and use Mydumper
```bash
# On GitHub, download the RPM package or source code package that corresponds to the machine that you use. We recommend you download the RPM package because it can be directly used while the source code package requires compilation. The OS used in the following example is CentOS 7. Therefore, download an el7 version.
[root@dev tmp]# wget https://github.com/mydumper/mydumper/releases/download/v0.12.1/mydumper-0.12.1-1-zstd.el7.x86_64.rpm
# Because the downloaded package is a ZSTD file, dependency 'libzstd' is required.
[root@dev tmp]# yum install libzstd.x86_64 -y
[root@dev tmp]#rpm -ivh mydumper-0.12.1-1-zstd.el7.x86_64.rpm
Preparing...                          ################################# [100%]
Updating / installing...
   1:mydumper-0.12.1-1                ################################# [100%]
# Backup library
[root@dev home]# mydumper -u root -p ******** -P 3306 -h 127.0.0.1 -B zz -o /home/dumper/
# Recovery library
[root@dev home]# myloader -u root -p ******** -P 3306 -h 127.0.0.1 -S /stonedb/install/tmp/mysql.sock -B zz -d /home/dumper
```
#### Generated backup files 
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
.....# The content is not full displayed since it is too long.
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
The directory contains the following files:
**metadata**: records the name and position of the binlog file of the backup database at the backup point in time.
:::info
If the backup is performed on the standby library, this file also records the name and position of the binlog file that has been synchronized from the active libary when the backup is performed.
:::
Each table has two backup files:
- **database-schema-create**: records the statements for creating the library.
- **database.table-schema.sql**: records the table schemas.
- **database.table.00000.sql**: records table data.
- **database.table-metadata**: records table metadata.
***Extensions***
If you want to import data to StoneDB, you must replace **engine=innodb** with **engine=stonedb** in table schema file **database.table-schema.sql** and check whether the syntax of the table schema is compatible with StoneDB. For example, if the syntax contain keyword **unsigned**, it is incompatible. Following is a schema example after modification:
```
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
  PRIMARY KEY (`id`)
) ENGINE=STONEDB AUTO_INCREMENT=10001 DEFAULT CHARSET=utf8;
```

### Backup principles
1. The main thread executes **FLUSH TABLES WITH READ LOCK** to add a global read-only lock to ensure data consistency.
2. The name and position of the binlog file at the current point in time are obtained and recorded to the **metadata **file to support recovery performed later.
3. Multiple (4 by default, customizable) dump threads change the isolation level for transactions to Repeatable Read and enable read-consistent transactions.
4. Non-InnoDB tables are exported.
5. After data of the non-transaction engine is backed up, the main thread executes **UNLOCK TABLES** to release the global read-only lock.
6. InnoDB tables are exported.