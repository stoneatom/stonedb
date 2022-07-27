---
id: oltp-performance-test-method
sidebar_position: 7.621
---

# OLTP Performance Test Method

## SysBench introduction
SysBench is a modular, cross-platform, and multithreaded benchmark tool for evaluating parameters that are important for a system that runs a database under heavy load. The idea of this benchmark suite is to quickly get an impression about system performance without setting up complex database benchmarks or even without installing a database at all.
## Test description
```sql
CREATE TABLE `sbtest1` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `k` int(11) NOT NULL DEFAULT '0',
  `c` char(120) NOT NULL DEFAULT '',
  `pad` char(60) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `k_1` (`k`)
) ENGINE=StoneDB AUTO_INCREMENT=800001 DEFAULT CHARSET=utf8
```
Percentage proportion of each type of SQL statements:

| **SELECT Type** | **Percentage (%)** | **SQL Statement Example** |
| --- | --- | --- |
| point_selects | 10 | SELECT c FROM sbtest%u WHERE id=? |
| simple_ranges | 1 | SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ? |
| sum_ranges | 1 | SELECT SUM(k) FROM sbtest%u WHERE id BETWEEN ? AND ? |
| order_ranges | 1 | SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c |
| distinct_ranges | 1 | SELECT DISTINCT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c |
| index_updates | 1 | UPDATE sbtest%u SET k=k+1 WHERE id=? |
| non_index_updates | 1 | UPDATE sbtest%u SET c=? WHERE id=? |

:::info

- In this test, operations involved in all SQL statements are read operations.
- StoneDB does not require secondary indexes, so **index_updates** is equivalent to **non_index_updates**.
:::
### Performance metrics

- Transactions Per Second (TPS): the number of transactions committed per second.
- Queries Per Second (QPS): the number of SQL statements executed per second, including INSERT, SELECT, UPDATE, and DELETE statements.
### Additional information

- In the standard OLTP read/write scenario provided by SysBench, a transaction consists of 18 read/write SQL statements. (Because StoneDB does not support DELETE operations, the DELETE statement is removed in this test.)
- In the standard OLTP read-only scenario provided by SysBench, a transaction consists of 14 read SQL statements: 14 primary key statements and 4 range statements.
- In the standard OLTP write-only scenario provided by SysBench, a transaction consists of 4 write SQL statements: 2 UPDATE statements, 1 DELETE statement, and 1 INSERT statement. (Because StoneDB does not support DELETE operations, a DELETE statement and an INSERT statement that is associated with the DELETE statement are removed.)
## Install SysBench
```shell
yum install gcc gcc-c++ autoconf automake make libtool bzr mysql-devel git mysql
git clone https://github.com/akopytov/sysbench.git
## Download SysBench from Git.
cd sysbench
## Open the directory that saves SysBench.
git checkout 1.0.18
## Switch the SysBench version to 1.0.18.
./autogen.sh
## Run autogen.sh.
./configure --prefix=$WROKSPACE/sysbench/ --mandir=/usr/share/man
make
## Compile
make install
```
Statement example for testing:
```shell
cd $WROKSPACE/sysbench/
# Prepare data.
bin/sysbench --db-driver=mysql --mysql-host=xx.xx.xx.xx --mysql-port=3306 --mysql-user=xxx --mysql-password=xxxxxx --mysql-db=sbtest --table_size=800000 --tables=230 --time=600 --mysql_storage_engine=StoneDB --create_secondary=false --test=src/lua/oltp_read_only.lua prepare

# Run workloads.
bin/sysbench --db-driver=mysql --mysql-host=xx.xx.xx.xx --mysql-port=3306 --mysql-user=xxx --mysql-password=xxxxxx  --mysql-db=sbtest --table_size=800000 --tables=230 --events=0 --time=600 --mysql_storage_engine=StoneDB  --threads=8 --percentile=95  --range_selects=0 --skip-trx=1 --report-interval=1 --test=src/lua/oltp_read_only.lua run

# Clear test data.
bin/sysbench --db-driver=mysql --mysql-host=xx.xx.xx.xx --mysql-port=3306 --mysql-user=xxx --mysql-password=xxxxxx  --mysql-db=sbtest --table_size=800000 --tables=230 --events=0 --time=600 --mysql_storage_engine=StoneDB  --threads=8 --percentile=95  --range_selects=0 --skip-trx=1 --report-interval=1 --test=src/lua/oltp_read_only.lua cleanup

```
## SysBench parameter description
| **Parameter** | **Description** |
| --- | --- |
| db-driver | The database driver. |
| mysql-host | The address of the test instance. |
| mysql-port | The port used to connect to the test instance. |
| mysql-user | The username of the test account. |
| mysql-password | The password of the test account. |
| mysql-db | The name of the test database. |
| table_size | The size of the table. |
| tables | The number of tables. |
| events | The number of connections. |
| time | The time that the test lasts. |
| threads | The number of threads. |
| percentile | The percentile to calculate in latency statistics. The default value is 95. |
| report-interval | The interval for generating reports about the test progress, expressed in seconds. Value 0 indicates that no such report will be generated, and only the final report will be generated. |
| skip-trx | Whether to skip transactions.<br />- **1**: yes<br />- **0**: no<br /> |
| mysql-socket | The **.sock** file specified for the instance. This parameter is valid if the instance is a local instance. |
| create_secondary | Whether to create secondary indexes. The default value is **true**. |






