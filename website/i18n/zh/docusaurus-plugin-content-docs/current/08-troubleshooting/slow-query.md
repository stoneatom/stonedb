---
id: slow-query
sidebar_position: 9.8
---

# 慢SQL诊断

慢查询日志用来记录SQL的执行时间超过long_query_time的阀值，慢查询日志支持将日志写入文件，也支持将日志写入数据库的表。慢查询日志能帮助我们捕捉到有性能问题的SQL，**相关慢查询日志参数如下**：

- slow_query_log：慢查询日志是否开启
- slow_query_log_file：慢查询日志记录的文件
- long_query_time：SQL实际执行时间超过该参数阀值就会被记录
- log_queries_not_using_indexes：未使用索引就会被记录
- log_slow_admin_statements：是否记录管理语句，如alter、create、drop

:::info
long_query_time 记录的是 SQL 实际执行时间，不包含锁等待时间，虽然有的 SQL 执行时间很长，但可能是由于锁等待引起的，实际上这种SQL不会被记录到慢查询日志。
:::

**慢查询日志输出解释**：

- Query_time：SQL总执行时间，即实际执行时间 + 锁等待时间
- Lock_time：锁等待时间
- Rows_sent：发送给客户端的行数
- Rows_examined：表示执行过程中扫描了多少行，这个值是在执行器每次调用存储引擎获取数据行的时候累加的，是在Server层统计的

:::info
在有些场景下，执行器调用一次，在存储引擎内部则扫描了多行，因此存储引擎扫描行数和rows_examined并不是完全相同的。
:::
## mysqldumpslow
mysqldumpslow常用于对慢查询日志进行汇总分类。

1. 查看慢查询日志中TOP 10（以平均执行时间降序排列）
```shell
mysqldumpslow -t 10 /var/lib/mysql/mysql-slow.log | more
```
2. 查看慢查询日志中返回总记录数最多的TOP 10（以返回总记录数降序排列）
```shell
mysqldumpslow -s r -t 10 /var/lib/mysql/mysql-slow.log | more
```
3. 查看慢查询日志中总执行次数最多的TOP 10（以总执行次数降序排列）
```shell
mysqldumpslow -s c -t 10 /var/lib/mysql/mysql-slow.log | more
```
4. 查看慢查询日志中按照总执行时间排序，前10条包含left join的SQL
```shell
mysqldumpslow -s t -t 10 -g "left join" /var/lib/mysql/mysql-slow.log | more
```
相关参数解释：
- s：降序排序
- al：平均锁定时间排序
- ar：平均返回记录数排序
- at：平均执行时间排序，默认
- c：总执行次数排序
- l：总锁定时间排序
- r：总返回记录数排序
- t：总执行时间排序
- t NUM：返回top n
- g：使用正则表达式
- h：根据主机名选择
- i：根据实例名选择
- l：不要从总时间减去锁定时间
## profiling
profiling分析工具可以获得一条SQL在整个执行过程中的每一步状态，以及每一步状态的资源消耗情况，如CPU、IO、Swap，同时还能获得每一步状态的函数调用，以及该函数所在的源文件和出现的位置。
用法示例如下：

开启当前线程的profiling：
```sql
set profiling=on;
```
查询当前线程执行的所有SQL：
```sql
show profiles;
```
查询某个SQL在执行过程中每个状态的CPU、IO消耗：
```sql
show profile cpu,block io for query query_id;
```
查询某个SQL在执行过程中每个状态的所有消耗：
```sql
show profile all for query query_id;
```
每个状态的所有消耗返回结果示例如下：
```sql
+----------------------+----------+----------+------------+-------------------+---------------------+--------------+---------------+---------------+-------------------+-------------------+-------------------+-------+-----------------------+----------------------+-------------+
| Status               | Duration | CPU_user | CPU_system | Context_voluntary | Context_involuntary | Block_ops_in | Block_ops_out | Messages_sent | Messages_received | Page_faults_major | Page_faults_minor | Swaps | Source_function       | Source_file          | Source_line |
+----------------------+----------+----------+------------+-------------------+---------------------+--------------+---------------+---------------+-------------------+-------------------+-------------------+-------+-----------------------+----------------------+-------------+
| starting             | 0.000363 | 0.000239 |   0.000025 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 0 |     0 | NULL                  | NULL                 |        NULL |
| checking permissions | 0.000055 | 0.000040 |   0.000004 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 0 |     0 | check_access          | sql_authorization.cc |         809 |
| checking permissions | 0.000045 | 0.000000 |   0.000047 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 0 |     0 | check_access          | sql_authorization.cc |         809 |
| Opening tables       | 0.000315 | 0.000000 |   0.000307 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 7 |     0 | open_tables           | sql_base.cc          |        5815 |
| System lock          | 0.000057 | 0.000000 |   0.000056 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 1 |     0 | mysql_lock_tables     | lock.cc              |         330 |
| init                 | 0.000047 | 0.000000 |   0.000048 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 0 |     0 | optimize_select       | Engine_execute.cpp   |         330 |
| optimizing           | 0.000195 | 0.000000 |   0.000203 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 1 |     0 | optimize              | sql_optimizer.cc     |         175 |
| update multi-index   | 0.000488 | 0.000216 |   0.000272 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 6 |     0 | UpdateMultiIndex      | ParameterizedFilter. |         981 |
| join                 | 8.127252 | 6.948224 |   1.168786 |               236 |                  18 |            0 |           168 |             0 |                 0 |                 0 |            304466 |     0 | UpdateJoinCondition   | ParameterizedFilter. |         603 |
| aggregation          | 0.021095 | 0.000957 |   0.000102 |                 3 |                   0 |           72 |             8 |             0 |                 0 |                 0 |                18 |     0 | Aggregate             | AggregationAlgorithm |          26 |
| query end            | 0.000185 | 0.000092 |   0.000009 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 7 |     0 | mysql_execute_command | sql_parse.cc         |        4972 |
| closing tables       | 0.000156 | 0.000141 |   0.000015 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                11 |     0 | mysql_execute_command | sql_parse.cc         |        5031 |
| freeing items        | 0.000152 | 0.000136 |   0.000015 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                 1 |     0 | mysql_parse           | sql_parse.cc         |        5659 |
| logging slow query   | 0.006152 | 0.000401 |   0.000000 |                 1 |                   0 |            8 |             8 |             0 |                 0 |                 0 |                 6 |     0 | log_slow_do           | log.cc               |        1718 |
| cleaning up          | 0.000245 | 0.000154 |   0.000000 |                 0 |                   0 |            0 |             0 |             0 |                 0 |                 0 |                13 |     0 | dispatch_command      | sql_parse.cc         |        1935 |
+----------------------+----------+----------+------------+-------------------+---------------------+--------------+---------------+---------------+-------------------+-------------------+-------------------+-------+-----------------------+----------------------+-------------+
```
一个SQL需要经历如下的Status过程：

1. starting：语法语义解析，生成解析树
2. checking permissions：根据解析后的解析树，对需要访问的表进行鉴权
3. opening tables：打开访问的表
4. System lock：确认是由哪种锁引起的
5. optimizing/statistics/preparing：这3个状态处于语句的物理和逻辑优化阶段，之后生成执行计划
6. Sending data：服务层与存储引擎层正在进行数据交互
7. Update：insert语句，如果遇到行锁会处于这个状态下
8. Updating：delete/update语句，如果遇到行锁会处于这个状态下
9. query end：提交过程，如果遇到大事务会处于这个状态下
10. closing tables：关闭表的访问，与opening tables对应
11. freeing items：释放解析树
12. 如果Status长时间处于如下几种状态，说明SQL存在性能问题或者等待。
13. Sending data：服务层与存储引擎层正在进行数据交互
14. converting HEAP to MyISAM：查询结果集太大，内存临时表不够用，需要暂存到磁盘临时文件
15. Creating tmp table：创建临时表，然后拷贝数据到临时表，用完之后删除临时表，通常是有group by、distinct、子查询操作
16. Copying to tmp table on disk：把内存中的临时表拷贝到磁盘
17. Creating sort index：在做排序操作，通常是order by
18. locked：被阻塞
19. Waiting for table metadata lock：发生元数据锁等待
## optimizer trace
有时候我们不仅想知道SQL的执行计划，还想知道优化器为什么选择这个执行计划。optimizer trace功能可以让我们知道优化器生成执行计划的过程，这个功能是由系统变量optimizer_trace来决定的。
```sql
mysql> show variables like 'optimizer_trace';
+-----------------+--------------------------+
| Variable_name   | Value                    |
+-----------------+--------------------------+
| optimizer_trace | enabled=off,one_line=off |
+-----------------+--------------------------+
```
enabled=off表示optimizer trace默认是关闭的，如果要开启，需要设置enabled=on，而且一般只作为当前线程开启；one_line=off表示输出格式，默认off表示换行输出。
使用optimizer trace功能的完整步骤如下：

1. 设置optimizer_trace缓存大小，目的是保存完整的trace，不要被截断
```sql
set optimizer_trace_max_mem_size = 100*1024;
```
2. 开启optimizer_trace
```sql
set optimizer_trace='enabled=on';
```
3. 执行具体的SQL

:::tip
如果SQL执行时间很长，而又不想等SQL执行结束才生成trace，使用explain SQL也是可以生成trace的。
:::

4. 从optimizer_trace表中查看上一个SQL的优化过程
```sql
select * from information_schema.optimizer_trace;
```
5. 关闭optimizer_trace
```sql
set optimizer_trace='enabled=off';
```
optimizer trace的输出分3个阶段，分别是：
1. join_preparation
2. join_optimization
3. join_execution


其中join_optimization阶段是基于成本计算的。对于单表查询来说，主要关注的是row_estimation过程，这个过程分析了全表扫描成本和各个索引扫描的成本。对于多表关联查询来说，主要关注的是considered_execution_plans过程，这个过程分析了不同表关联顺序的成本。

### join_optimization 阶段需要关注的部分：

- condition_processing：优化器在这个过程会做查询条件处理，如等值传递转换、常量传递转换、消除没用的条件。例如：原始的查询条件是："a=1 and b=a"，经过转换后是："a=1 and b=1"
- rows_estimation：单表查询的行预估
- table_scan：全表扫描统计
- rows：全表扫描预估行数
- cost：全表扫描的成本
- potential_range_indexes：可能使用的索引
- "index": "PRIMARY",   --->表示主键索引不可用
- "usable": false,
- "cause": "not_applicable"
- "index": "idx_xxx",   --->表示idx_xxx索引可能被使用
- "usable": true,
- "key_parts": [
  "column1",
  "column2",
  "id"]
- analyzing_range_alternatives：分析各种可能使用的索引的成本
- "index": "idx_xxx",
- "ranges": [
  "2 < = column1 < = 2 AND 0 < = column2 < = 0"
  ],
- "index_dives_for_eq_ranges": true,   --->使用index dive
- "rowid_ordered": true,   --->使用该索引获取的记录是否按照主键排序
- "using_mrr": false,   --->是否使用mrr
- "index_only": false,   --->是否使用索引覆盖
- "rows": xxx, --->使用该索引预估行数
- "cost": xxx,   --->使用该索引的成本
- "chosen": true   --->是否选择该索引
- chosen_range_access_summary：最终选择的执行计划
- considered_execution_plans：分析各种可能的执行计划，常用于多表关联查询
- attaching_conditions_to_tables：尝试给查询添加一些其他的查询条件
- reconsidering_access_paths_for_index_ordering：是否因排序导致执行计划改变
