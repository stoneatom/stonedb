---
id: release-notes
sidebar_position: 11.0
---

# 发版日志


## StoneDB-8.0-v2.1.0 企业版的发行日志 (2023-10-31, GA)

### 一、新特性
1. 支持事务。
2. 支持主从复制。
3. 支持常用的内置函数。
4. 支持存储过程、临时表、视图、触发器等对象。

### 二、Bug 修复
1. 当前线程无法看到同步后的数据，必须退出当前线程后再次登录才能看到同步后的数据。
2. 查询返回"Current transaction is aborted (please ROLLBACK)"报错。
3. 分页查询结果集错误。
4. kill线程后，无法关闭数据库。
5. 当退出MDL元数据锁等待后，再次查询相关表报错，且无法删除该表。
6. 在当前库查询其它库的表发生报错。
7. 子查询返回单行数据，使用in返回正确结果集，但使用=返回错误结果集。

### 三、稳定性提升
1. 主从复制下null值的处理。
2. 修复ifnull、nullif导致实例crash。

## StoneDB-8.0-v2.0.0 企业版的发行日志 (2023-09-25, GA)

### 一、架构介绍
1. StoneDB 是业内首个单机内核开源、行列混存+内存计算架构的一体化 MySQL 实时 HTAP 数据库。
2. 产品对标 Oracle HeatWave，使用 MySQL 的用户，不需要进行数据迁移，通过 StoneDB 可以实现 TP+AP 混合负载，分析性能提升 100 倍以上，也无需与其他 AP 集成，弥补 MySQL 分析领域的空白。
3. 新⼀代企业级实时 HTAP 数据库，100% 兼容 MySQL 协议，100TB 存储，1万并发，70% TP场景，30% AP 场景，通过 AP 增强到自主可控的 TP，瞄准大量 MySQL 信创升级 + 替代市场。

### 二、新特性
1. StoneDB 2.0 提供实时的在线事务支持和数据分析能力。在支持TP事务的同时，支持内置的、对用户透明的AP引擎，百亿级数据join场景下的高性能，相比较MySQL提供100至1000倍的加速。
2. 支持create/alter table/drop table 等DDL语句。
3. 支持insert/update/load data等命令对数据做实时更新。

## StoneDB-5.7-v1.0.4-alpha 的发行日志 (2023-06-30, Alpha)
发布日期： 2023 年 06 月 30 日

### 一、稳定性

1.  修复在导入数据时候，增量数据导致的 crash(**#1805**)

2.  修复在 union all 字句结果集导致的 crash(**#1875**)

3.  修复在大数据量情况下使用聚合函数导致的 crash(**#1855**)

4.  修复主从复制下的内存溢出导致的 crash(**#1549**)

### 二、新特性

#### 2.1  支持 insert/update ignore 语法特性

当更新 Tianmu 时候，对于主键冲突的记录将被跳过，然后执行后续的更新操作。例如：

```sql
CREATE TABLE t1  (id int(11) NOT NULL auto_increment,parent_id int(11) DEFAULT '0' NOT NULL,level tinyint(4)
DEFAULT '0' NOT NULL, PRIMARY KEY (id)) engine=tianmu;
INSERT INTO t1 VALUES (3,1,1),(4,1,1);
```

执行 `update ignore t1 set id=id+1;` 语句会忽略  PK=3  的更新，因为更新后的主键会与  PK=4 冲突。继续执行 PK=4 的更新，更新后 PK=5。
```sql
mysql>  CREATE TABLE t1  (id int(11) NOT NULL auto_increment,  parent_id int(11) DEFAULT '0' NOT NULL,  level tinyint(4)
    ->  DEFAULT '0' NOT NULL, PRIMARY KEY (id)) engine=tianmu;
Query OK, 0 rows affected (0.01 sec)

mysql>  INSERT INTO t1 VALUES (3,1,1),(4,1,1);
Query OK, 2 rows affected (0.01 sec)
Records: 2  Duplicates: 0  Warnings: 0

mysql> update t1 set id=id+1;
ERROR 1062 (23000): Duplicate entry '4' for key 'PRIMARY'
mysql> select * from t1;
+----+-----------+-------+
| id | parent_id | level |
+----+-----------+-------+
|  3 |         1 |     1 |
|  4 |         1 |     1 |
+----+-----------+-------+
2 rows in set (0.00 sec)

mysql> update ignore t1 set id=id+1;
Query OK, 2 rows affected (0.00 sec)
Rows matched: 2  Changed: 2  Warnings: 0

mysql> select * from t1;
+----+-----------+-------+
| id | parent_id | level |
+----+-----------+-------+
|  3 |         1 |     1 |
|  5 |         1 |     1 |
+----+-----------+-------+
2 rows in set (0.00 sec)
```

#### 2.2 ROW 格式支持 Load 语句转换为 write row

当 StoneDB 作为主机时候，Load 语句将以 insert into 的方式被写进 binlog。
```binlong
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;
/*!50003 SET @OLD_COMPLETION_TYPE=@@COMPLETION_TYPE,COMPLETION_TYPE=0*/;
DELIMITER /*!*/;
# at 4
#230630 10:50:51 server id 1  end_log_pos 123 CRC32 0x050a2c27 	Start: binlog v 4, server v 5.7.36-StoneDB-v1.0.1.42e5a3ad4 created 230630 10:50:51 at startup
# Warning: this binlog is either in use or was not closed properly.
ROLLBACK/*!*/;
BINLOG '
C0OeZA8BAAAAdwAAAHsAAAABAAQANS43LjM2LVN0b25lREItdjEuMC4xLjQyZTVhM2FkNAAAAAAA
AAAAAAAAAAAAAAAAAAALQ55kEzgNAAgAEgAEBAQEEgAAXwAEGggAAAAICAgCAAAACgoKKioAEjQA
AScsCgU=
'/*!*/;
# at 123
#230630 10:50:51 server id 1  end_log_pos 154 CRC32 0x3407f97c 	Previous-GTIDs
# [empty]
# at 154
#230630 10:50:51 server id 1  end_log_pos 219 CRC32 0x1631cab7 	Anonymous_GTID	last_committed=0	sequence_number=1	rbr_only=no
SET @@SESSION.GTID_NEXT= 'ANONYMOUS'/*!*/;
# at 219
#230630 10:50:51 server id 1  end_log_pos 334 CRC32 0x1b721a4f 	Query	thread_id=2	exec_time=0	error_code=0
use `test`/*!*/;
SET TIMESTAMP=1688093451/*!*/;
SET @@session.pseudo_thread_id=2/*!*/;
SET @@session.foreign_key_checks=1, @@session.sql_auto_is_null=0, @@session.unique_checks=1, @@session.autocommit=1/*!*/;
SET @@session.sql_mode=1436549152/*!*/;
SET @@session.auto_increment_increment=1, @@session.auto_increment_offset=1/*!*/;
/*!\C latin1 *//*!*/;
SET @@session.character_set_client=8,@@session.collation_connection=8,@@session.collation_server=8/*!*/;
SET @@session.lc_time_names=0/*!*/;
SET @@session.collation_database=DEFAULT/*!*/;
create table t1(id int, name varchar(10))
/*!*/;
# at 334
#230630 10:50:51 server id 1  end_log_pos 399 CRC32 0x092fa235 	Anonymous_GTID	last_committed=1	sequence_number=2	rbr_only=yes
/*!50718 SET TRANSACTION ISOLATION LEVEL READ COMMITTED*//*!*/;
SET @@SESSION.GTID_NEXT= 'ANONYMOUS'/*!*/;
# at 399
#230630 10:50:51 server id 1  end_log_pos 471 CRC32 0x417b2366 	Query	thread_id=2	exec_time=0	error_code=0
SET TIMESTAMP=1688093451/*!*/;
BEGIN
/*!*/;
# at 471
#230630 10:50:51 server id 1  end_log_pos 519 CRC32 0x563c6d07 	Table_map: `test`.`t1` mapped to number 108
# at 519
#230630 10:50:51 server id 1  end_log_pos 580 CRC32 0x99df1dba 	Write_rows: table id 108 flags: STMT_END_F
BINLOG '
C0OeZBMBAAAAMAAAAAcCAAAAAGwAAAAAAAEABHRlc3QAAnQxAAIDDwIKAAMHbTxW
C0OeZB4BAAAAPQAAAEQCAAAAAGwAAAAAAAEAAgAC//wBAAAAB0FBQUFBQUH8AgAAAAdCQkJCQkJC
uh3fmQ==
'/*!*/;
### INSERT INTO `test`.`t1`
### SET
###   @1=1 /* INT meta=0 nullable=1 is_null=0 */
###   @2='AAAAAAA' /* VARSTRING(10) meta=10 nullable=1 is_null=0 */
### INSERT INTO `test`.`t1`
### SET
###   @1=2 /* INT meta=0 nullable=1 is_null=0 */
###   @2='BBBBBBB' /* VARSTRING(10) meta=10 nullable=1 is_null=0 */
# at 580
#230630 10:50:51 server id 1  end_log_pos 611 CRC32 0x8ee952db 	Xid = 3
COMMIT/*!*/;
# at 611
#230630 10:50:51 server id 1  end_log_pos 676 CRC32 0x5d2a5859 	Anonymous_GTID	last_committed=2	sequence_number=3	rbr_only=no
SET @@SESSION.GTID_NEXT= 'ANONYMOUS'/*!*/;
# at 676
#230630 10:50:51 server id 1  end_log_pos 791 CRC32 0x929d7148 	Query	thread_id=2	exec_time=0	error_code=0
SET TIMESTAMP=1688093451/*!*/;
DROP TABLE `t1` /* generated by server */
/*!*/;
SET @@SESSION.GTID_NEXT= 'AUTOMATIC' /* added by mysqlbinlog */ /*!*/;
DELIMITER ;
# End of log file
/*!50003 SET COMPLETION_TYPE=@OLD_COMPLETION_TYPE*/;
/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=0*/;
```

#### 2.3  支持  AggregatorGroupConcat  函数
```sql
mysql> select GROUP_CONCAT(t.id) from sequence t;
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| GROUP_CONCAT(t.id)                                                                                                                                                                         |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 3000000000010000,3000000000010001,3000000000010002,3000000000010003,3000000000010004,3000000000010005,3000000000010006,3000000000010007,3000000000010008,3000000000010009,3000000000010010 |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

#### 2.4  支持  uion/union all  中使用  select 111  或者  select 111 from dual  场景
```sql
mysql>  select id from tt union all select 2222 c1 from dual;
+------+
| id   |
+------+
| 1111 |
| 2222 |
+------+

mysql>  select id from tt union all select 2222 ;
+------+
| id   |
+------+
| 1111 |
| 2222 |
+------+

-- PS：select 111( from dual) 出现位置非第一子句位置
```

### 三、其他  Bug fixed

1. 修复主备场景下，新增列默认值问题(**#1187**)
2. 修复 derived table 中使用 case...when... 结果集不正确问题(**#1784**)
3. 修复类型为 TIME 时候，位操作时由于精度丢失导致结果不正确问题(**#1173**)
4. 修复类型为 BIGINT 时候，由于类型溢出导致查询结果不正确问题(**#1564**)
5. 修复当过滤条件为十六进制时候，结果不正确问题(**#1625**)
6. 修复使用 load 命令导入数据时候，表上缺省值未生效问题(**#1865**)
7. 修改 load 命令默认字段分隔符，使其与 MySQL 行为一致(**#1609**)
8. 修复由于元数据异常导致，查询错误(**#1822**)
9. 修复自动提交被关闭导致数据无法写入的问题(**#1510**)
10. 提升 GitHub 上 MTR 稳定性，wan'sha。

### 四、支持平台

- CentOS 7.6  以上
- Ubuntu 20



### 更新内容
* fix(sql,tianmu):fix when binlog format is row, the load data statement…
* fix(tianmu): add TIME_to_ulonglong_time_round process and fix up preci…
* fix(tianmu): incorrect result when using where expr and args > bigint_max #1564
* fix(tianmu): hotfix corruption in ValueOrNull under multi-thread
* fix(tianmu): To remove unnessary optimization in tianmu
* fix(tianmu): To support union(all) the statement which is without from clause
* fix(tianmu): default value of the field take unaffect in load #1865
* workflow(coverage): Update the lcov running logic
* wokflow(codecov): Filter out excess code files
* docs(intro): update the support for 8.0
* ci(codecov): update the codecov congfig
* fix(tianmu): To suuport ignore option for update statement
* ci(codecov): update the config
* test(mtr): add order by sentence in the mtr case various_join.test
* test(mtr): add more test cases for tianmu(#1196)
* test(tianmu): add order by sentence in the mtr case various_join.test
* fix(tianmu): fix UNION of non-matching columns (column no 0)
* fix(tianmu): Fixup the mem leakage of aggregation function
* docs(developer-guide): update the compiling guide of stonedb 8.0 for c…
* fix(tianmu): To fixup the instance crashed if the result of aggregate …
* fix(tianmu): fix up the `group_concat` function in tianmu (#1852)
* fix(tianmu): fix up mtr test case for delim of load data command (#1854)
* fix(tianmu): revert PR #1841. (#1850)
* feat(tianmu): fixup the default delimeter for load data (#1843)
* feat(tianmu): revert assert() --> debug_assert() #1551
* fix(workflow): nightly build failed #1830
* fix(tianmu): fix up the incorrect meta-info leads unexpected behavior (#1840)
* fix(tianmu): Fix up the unknown exception after instance killed randomly (#1841)
* fix(tianmu): fix up the incompatible type
* docs(website): update the documentation for Compile StoneDB 8.0 in Docker(#1823)
* fix(tinmu): fix tianmu crash when set varchar to num when order by
* fix incorrect result of TIME type by distinguishing the processing of …
* fix: fix storage of DT type
* automatically formatting
* feature: remove DBUG_OFF and repalce DEBUG_ASSERT with assert
* docs:add docker compile guide of stonedb8.0.(#1780)
* doc(develop-guide): modify method for complie stonedb using docker
* fix(tianmu): fix Error result set of the IN subquery with semi join (#1764)
* fit format
* open log for all cmds
* add delete/drop into tianmu log stat
* fix(tianmu):Even if a primary key is defined, duplicate data may be im…
* docs(quickstart): add stonedb-8.0 compiling guide(Chinese) for CentOS 7.x
* add stonedb-8.0 compiling guide for CentOS 7.x
* fix bug and change test case exptected result
* remove unused code block
* fix(tianmu): fix error occurred in the union all query result (#1599)
* fix(tianmu): Insert ignore can insert duplicate values.(#1699)
* fix(tianmu): fix mysqld crash when assigning return values using b…
* feat(tianmu): Test cases that supplement custom variables (#1703)

**完整的更新日志**: https://gitee.com/StoneDB/stonedb/compare/5.7-v1.0.3-GA...5.7-v1.0.4-alpha


## StoneDB-5.7-v1.0.3 的发行日志 (2023-03-20, GA)
发布日期： 2023 年 03 月 20 日
### 主备能力

- Binlog 改造 — 指定 DDL 过滤。
- SQL MODE 中增加参数 NO_KEY_ERROR，SQL 层对不支持 DDL 报错忽略。
   - 语法举例：
```sql
# 全局级别
mysql>set global sql_mode='NO_KEY_ERROR';

# 会话级别
mysql>set session sql_mode='NO_KEY_ERROR';

# my.cnf 配置文件
[mysqld] 
sql_mode='NO_KEY_ERROR'
```

### 生态适配

- StoneDB 版本号展示。
### 性能

- 主从同步性能提升 [#1213](https://github.com/stoneatom/stonedb/issues/1213)。
### 问题修复
修复了如下 Bug：

- 执行 ALTER TABLE 语句添加 TIMESTAMP 字段返回结果不正确。[#1327](https://github.com/stoneatom/stonedb/issues/1327)
- 执行 ALTER TABLE 语句后 Update 表数据不正确。[#1253](https://github.com/stoneatom/stonedb/issues/1253) 
- Bigint unsigned 返回结果不正确。 [#1203](https://github.com/stoneatom/stonedb/issues/1203) 
- Load data 报错。[#1209](https://github.com/stoneatom/stonedb/issues/1209)
- AVG 函数返回结果不正确。[#1125](https://github.com/stoneatom/stonedb/issues/1125)
- ALTER TABLE 更改字段数据类型报错。[#752](https://github.com/stoneatom/stonedb/issues/752)
- 其它 Bug。[#103](https://github.com/stoneatom/stonedb/issues/103) [#1230](https://github.com/stoneatom/stonedb/issues/1230) [#1255](https://github.com/stoneatom/stonedb/issues/1255)[#1188](https://github.com/stoneatom/stonedb/issues/1188) [#1262](https://github.com/stoneatom/stonedb/issues/1262)

### 支持平台

- CentOS 7.6 以上
- Ubuntu 20

更多该版本的详细更新记录，请访问 [Github](https://github.com/stoneatom/stonedb/releases) 和 [Gitee](https://gitee.com/StoneDB/stonedb/releases) 查看。

## StoneDB-5.7-v1.0.2 的发行日志 (2023-01-15, GA)
发布日期： 2023 年 01 月 15 日
### 功能开发

- 支持自定义函数。
- 支持转义功能。
- 支持主键，语法上支持索引。
- 支持修改表/字段的字符集。
- 支持BIT数据类型
   - 建表时允许指定字段类型为 BIT，也允许修改表字段类型为 BIT(需要满足类型转换条件)。
   - BIT 数据类型逻辑运算
- 支持replace into 功能。
- 支持(语法上)支持unsigned 和zerofill。
- SQL MODE 中增加参数 MANDATORY TIANMU，用于指定表的默认存储引擎为 TIANMU。
   - 语法举例：
```sql
# 全局级别
mysql>set global sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU';

# 会话级别
mysql>set session sql_mode='STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU';

# my.cnf 配置文件
[mysqld] 
sql_mode        =  'STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION,MANDATORY_TIANMU'
```

### 易用性

- 安装包自动检测识别能力。
- 快速部署StoneDB为MySQL的从库能力。

### 稳定性

- 做为从库的稳定性增强。

### 问题修复
修复了如下 Bug：

- GROUP_CONCAT() 函数返回错误。[#938](https://github.com/stoneatom/stonedb/issues/938)
- 模糊匹配LIKE 查询问题。[#1162](https://github.com/stoneatom/stonedb/issues/1162) [#1157](https://github.com/stoneatom/stonedb/issues/1157) [#763](https://github.com/stoneatom/stonedb/issues/763)
- 主键AUTO_INCREMENT问题。 [#1144](https://github.com/stoneatom/stonedb/pull/1144) [#1142](https://github.com/stoneatom/stonedb/issues/1142)
- 增加NUMERIC 类型字段返回错误。[#1140](https://github.com/stoneatom/stonedb/issues/1140)
- CI/CD 执行clang-format失败。[#973](https://github.com/stoneatom/stonedb/issues/973)
- INSERT INTO 兼容性。[#965](https://github.com/stoneatom/stonedb/issues/965)
- UNION ALL 返回错误。[#854](https://github.com/stoneatom/stonedb/issues/854)
- EXTRACT() 函数返回错误。[#845](https://github.com/stoneatom/stonedb/issues/845)
- DATE类型select 显示错误。[#829](https://github.com/stoneatom/stonedb/issues/829)
- UPDATE 更改多列不生效。[#781](https://github.com/stoneatom/stonedb/issues/781)
- 子查询场景。[#732](https://github.com/stoneatom/stonedb/issues/732)
- MTR binlog.binlog_unsafe Crash。[#341](https://github.com/stoneatom/stonedb/issues/341)
- 其它BUG。[#682](https://github.com/stoneatom/stonedb/issues/682) [#553](https://github.com/stoneatom/stonedb/issues/553) [#508](https://github.com/stoneatom/stonedb/issues/508)

### 行为变更
使用快速部署StoneDB为MySQL的从库Shell脚本，默认 sql_mode 开启强制 Tianmu 引擎参数：MANDATORY_TIANMU。

### 支持平台

- CentOS 7.6以上。
- Ubuntu 20。

### 其他

- 添加一些MTR测试用例。


## StoneDB-5.7-v1.0.1 的发行日志 (2022-10-21, RC; 2022-10-24, GA)
- 功能添加或改变
- 编译相关改动
- 文档变更
- BUG修复

### 功能添加或改变
- 关键项：Tianmu 引擎增加 delete 功能
  - 增加 delete all 单表功能；
  - 增加 delete all 多表功能；
  - 增加 delete where 单表功能；
  - 增加 delete where 多表功能；
- 关键项：Tianmu 引擎增加 alter table 功能
- 关键项：Tianmu 引擎 binlog 日志增加 ROW 格式
- 关键项：Tianmu 引擎增加临时表功能
- 关键项：Tianmu 引擎增加触发器功能
- 关键项：Tianmu 引擎增加 Create table AS...union... 功能
- 关键项：Tianmu 引擎提升了子查询的性能
- 关键项：Tianmu引擎增加 gtest 模块
- 关键项：添加一些 mtr 测试用例
### 编译相关改动
- cmake 增加 -DWITH_MARISA  -DWITH_ROCKSDB 参数
### 文档变更
- 用户手册、编译手册等相关文档发生了变更. ( # address)
### BUG修复
- 修复若干 MySQL 原生 MTR 用例
- 修复及完善 Tianmu 功能: 
>修复 issue 有： [#282](https://github.com/stoneatom/stonedb/issues/282),[#274](about:blank),[#270](https://github.com/stoneatom/stonedb/issues/270),[#663](https://github.com/stoneatom/stonedb/issues/663),[#669](https://github.com/stoneatom/stonedb/issues/669),[#670](https://github.com/stoneatom/stonedb/issues/670),[#675](https://github.com/stoneatom/stonedb/issues/675),[#678](https://github.com/stoneatom/stonedb/issues/678),[#682](https://github.com/stoneatom/stonedb/issues/682),[#487](https://github.com/stoneatom/stonedb/issues/487),[#426](https://github.com/stoneatom/stonedb/issues/426),[#250](https://github.com/stoneatom/stonedb/issues/250),[#247](https://github.com/stoneatom/stonedb/issues/247),[#569](https://github.com/stoneatom/stonedb/issues/569),[#566](https://github.com/stoneatom/stonedb/issues/566),[#290](https://github.com/stoneatom/stonedb/issues/290),[#736](https://github.com/stoneatom/stonedb/issues/736),[#567](https://github.com/stoneatom/stonedb/issues/567),[#500](https://github.com/stoneatom/stonedb/issues/500),[#300](https://github.com/stoneatom/stonedb/issues/300),[#289](https://github.com/stoneatom/stonedb/issues/289),[#566](https://github.com/stoneatom/stonedb/issues/566),[#279](https://github.com/stoneatom/stonedb/issues/279),[#570](https://github.com/stoneatom/stonedb/issues/570)[,#571](https://github.com/stoneatom/stonedb/issues/571),[#580](https://github.com/stoneatom/stonedb/issues/580),[#581](https://github.com/stoneatom/stonedb/issues/581),[#586](https://github.com/stoneatom/stonedb/issues/586),[#589](https://github.com/stoneatom/stonedb/issues/589),[#674](https://github.com/stoneatom/stonedb/issues/674),[#646](https://github.com/stoneatom/stonedb/issues/646),[#280](https://github.com/stoneatom/stonedb/issues/280),[#301](https://github.com/stoneatom/stonedb/issues/301),[#733](https://github.com/stoneatom/stonedb/issues/733) 等。

## StoneDB-5.7-v1.0.0 的发行日志 (2022-08-31, GA)
- 支持MySQL 5.7
- 功能添加或改变
- 编译相关改动
- 配置相关改动
- 文档变更
- BUG修复

### 支持MySQL 5.7

- **关键项：** StoneDB 数据库支持MySQL 5.7 协议，基线版本：MySQL 5.7.36
### 功能添加或改变

- **关键项：** StoneDB 数据库的列式存储引擎从 StoneDB 变更为 Tianmu
```bash
mysql> show engines;
+----------------+---------+--------------------------+--------------+------+------------+
| Engine         | Support | Comment                  | Transactions | XA   | Savepoints |
+----------------+---------+--------------------------+--------------+------+------------+
| TIANMU         | DEFAULT | Tianmu storage engine    | YES          | NO   | NO         |
+----------------+---------+--------------------------+--------------+------+------------+
```

- **Tianmu:** 提升了 Tianmu 引擎对 decimal 数据类型的聚合能力；
- **Tianmu: **提高了代码的可读性。有些代码在逻辑上没有分开或者变量名称不能体现真实含义。例如：修改 int DoGetSomething(); to int GetSomethingXXX();, int GetNoNulls() to int GetNumOfNulls()。
- **Tianmu: **优化了视图对日期函数的调用能力，日期函数能够被视图正常调用使用 (例如：DATE_ADD, DATE_SUB等函数) ；([BUG #342](https://github.com/stoneatom/stonedb/issues/342))
### 编译相关改动

- Boost 依赖库的版本变更为 1.66.0；
- Rocksdb 满足 StoneDB 数据库构建的版本变更为 6.12.6；
### 配置相关改动

- **关键项：**StoneDB 数据库默认配置文件从 stonedb.cnf 变更为 my.cnf；([feature #182](https://github.com/stoneatom/stonedb/issues/182))
- **关键项：**StoneDB 数据库的默认存储引擎从 Innodb 变更为 Tianmu。([feature #255](https://github.com/stoneatom/stonedb/issues/255))
### 文档变更

- 用户手册、编译手册等相关文档发生了变更. ( [# address](https://stonedb.io/))
### BUG修复

- **修复 mtr 用例: **[BUG #78](https://github.com/stoneatom/stonedb/issues/78), [BUG #73](https://github.com/stoneatom/stonedb/issues/73),[ BUG #170](https://github.com/stoneatom/stonedb/issues/170), [BUG #192](https://github.com/stoneatom/stonedb/issues/192), [BUG #191](https://github.com/stoneatom/stonedb/issues/191), [BUG #227](https://github.com/stoneatom/stonedb/issues/227),  [BUG #245](https://github.com/stoneatom/stonedb/issues/245), [BUG  #263](https://github.com/stoneatom/stonedb/issues/263)
- **修复 Tianmu 缺陷: **[BUG #338](https://github.com/stoneatom/stonedb/issues/388),[ BUG #327](https://github.com/stoneatom/stonedb/issues/327), [BUG #212](https://github.com/stoneatom/stonedb/issues/212), [BUG #142](https://github.com/stoneatom/stonedb/issues/142)

## StoneDB-5.6-v1.0.0

发布时间: 2022 年 6 月 30 号

全面兼容 MySQL5.6 版本。

