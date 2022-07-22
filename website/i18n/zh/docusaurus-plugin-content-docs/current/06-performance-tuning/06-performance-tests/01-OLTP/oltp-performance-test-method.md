---
id: oltp-performance-test-method
sidebar_position: 7.621
---

# StoneDB性能测试方法（OLTP）

## SysBench工具介绍
SysBench是一个跨平台且支持多线程的模块化基准测试工具，用于评估系统在运行高负载的数据库时相关核心参数的性能表现。可绕过复杂的数据库基准设置，甚至在没有安装数据库的前提下，快速了解数据库系统的性能。
## 测试说明
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
SQL请求比例（只读测试方案下所有的请求都是读操作，不涉及写操作）

| SQL类型 | 执行比例 | SQL语句示例 |
| --- | --- | --- |
| point_selects | 10 | SELECT c FROM sbtest%u WHERE id=? |
| simple_ranges | 1 | SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ? |
| sum_ranges | 1 | SELECT SUM(k) FROM sbtest%u WHERE id BETWEEN ? AND ? |
| order_ranges | 1 | SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c |
| distinct_ranges | 1 | SELECT DISTINCT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c |
| index_updates（StoneDB无需二级索引，所以不用添加二级索引，等同于
non_index_updates） | 1 | UPDATE sbtest%u SET k=k+1 WHERE id=? |
| non_index_updates | 1 | UPDATE sbtest%u SET c=? WHERE id=? |

### 测试指标

- 每秒执行事务数TPS（Transactions Per Second）数据库每秒执行的事务数，以COMMIT成功次数为准。
- 每秒执行请求数QPS（Queries Per Second）数据库每秒执行的SQL数，包含INSERT、SELECT、UPDATE、DETELE等。
- SysBench标准OLTP读写混合场景中一个事务包含18个读写SQL（由于暂不支持delete操作，所以去除一条deleteSQL）。
- SysBench标准OLTP只读场景中一个事务包含14个读SQL（10条主键点查询、4条范围查询）
- SysBench标准OLTP写场景中一个事务包含4个写SQL（2条UPDATE、1条DETELE（去除）、1条INSERT（和delete绑定去除））
## 安装工具-Sysbench
```shell
yum install gcc gcc-c++ autoconf automake make libtool bzr mysql-devel git mysql
git clone https://github.com/akopytov/sysbench.git
##从Git中下载SysBench
cd sysbench
##打开SysBench目录
git checkout 1.0.18
##切换到SysBench 1.0.18版本
./autogen.sh
##运行autogen.sh
./configure --prefix=$WROKSPACE/sysbench/ --mandir=/usr/share/man
make
##编译
make install
```
测试语句示例
```shell
cd $WROKSPACE/sysbench/
# 准备数据
bin/sysbench --db-driver=mysql --mysql-host=xx.xx.xx.xx --mysql-port=3306 --mysql-user=xxx --mysql-password=xxxxxx --mysql-db=sbtest --table_size=800000 --tables=230 --time=600 --mysql_storage_engine=StoneDB --create_secondary=false --test=src/lua/oltp_read_only.lua prepare

# 运行workload
bin/sysbench --db-driver=mysql --mysql-host=xx.xx.xx.xx --mysql-port=3306 --mysql-user=xxx --mysql-password=xxxxxx  --mysql-db=sbtest --table_size=800000 --tables=230 --events=0 --time=600 --mysql_storage_engine=StoneDB  --threads=8 --percentile=95  --range_selects=0 --skip-trx=1 --report-interval=1 --test=src/lua/oltp_read_only.lua run

# 清理压测数据
bin/sysbench --db-driver=mysql --mysql-host=xx.xx.xx.xx --mysql-port=3306 --mysql-user=xxx --mysql-password=xxxxxx  --mysql-db=sbtest --table_size=800000 --tables=230 --events=0 --time=600 --mysql_storage_engine=StoneDB  --threads=8 --percentile=95  --range_selects=0 --skip-trx=1 --report-interval=1 --test=src/lua/oltp_read_only.lua cleanup

```
## SysBench参数说明
| **参数** | **说明** |
| --- | --- |
| db-driver | 数据库驱动。 |
| mysql-host | 测试实例连接地址。 |
| mysql-port | 测试实例连接端口。 |
| mysql-user | 测试实例账号。 |
| mysql-password | 测试实例账号对应的密码。 |
| mysql-db | 测试实例数据库名。 |
| table_size | 测试表大小。 |
| tables | 测试表数量。 |
| events | 测试请求数量。 |
| time | 测试时间。 |
| threads | 测试线程数。 |
| percentile | 需要统计的百分比，默认值为95%，即请求在95%的情况下的执行时间。 |
| report-interval | 表示N秒输出一次测试进度报告，0表示关闭测试进度报告输出，仅输出最终的报告结果。 |
| skip-trx | 是否跳过事务。1：跳过；0：不跳过 |
| mysql-socket | 本地的实例可以指定socket文件 |
| create_secondary | 是否创建二级索引，默认true |



