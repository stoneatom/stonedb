---
id: slow-query
sidebar_position: 9.8
---

# Diagnose Slow SQL Queries

The slog query log is used to record SQL queries whose execution time is longer than the threshold specified by parameter **long_query_time**. The slow query log can be recorded in a file or in a StoneDB table. It helps identify SQL statements that may affect database performance. 

The following table describes parameters for configuring slow query log.

| **Parameter** | **Description** |
| --- | --- |
| `slow_query_log` | Whether to enable slog query log. |
| `slow_query_log_file` | The file that stores slow query log records. |
| `long_query_time` | The execution time threshold. If the execution time of an SQL query exceeds this threshold, the SQL query will be recorded in the slow query log. info **long_query_time** is used to limit the actual execution time of each SQL query, excluding the lock wait time. Therefore, if an SQL query has long total execution time but its actual execution time does not exceed the this threshold, the SQL query will not be recorded in the slow query log. |
| `log_queries_not_using_indexes` | Whether to record the queries that do not use indexes. |
| `log_slow_admin_statements` | Whether to record management statements, including `ALTER`, `CREATE`, and `DROP`. |

The following table describes the parameters in the slow query log:

| **Parameter** | **Description** |
| --- | --- |
| Query_time | The total execution time of an SQL query, including the lock wait time. |
| Lock_time | The lock wait time. |
| Rows_sent | The number of rows sent to the client. |
| Rows_examined | The number of rows that have been scanned during the execution. This number is accumulatively counted each time the storage engine is called by the executor to obtain data records. This value is obtained at the server level. |

# mysqldumpslow
mysqldumpslow is used to classify slow query log records. Following are some examples.

- Check the top 10 SQL statements sorted by average execution time in descending order.
```shell
mysqldumpslow -t 10 /var/lib/mysql/mysql-slow.log | more
```

- Check the top 10 SQL statements sorted by the number of returned records in descending order.
```shell
mysqldumpslow -s r -t 10 /var/lib/mysql/mysql-slow.log | more
```

- Check the top 10 SQL statements sorted by count in descending order. 
```shell
mysqldumpslow -s c -t 10 /var/lib/mysql/mysql-slow.log | more
```

- Check the top 10 SQL statements that include LEFT JOIN sorted by total execution time in descending order.
```shell
mysqldumpslow -s t -t 10 -g "left join" /var/lib/mysql/mysql-slow.log | more
```
The following table describes the relevant parameters.

| **Parameter** | **Description** |
| --- | --- |
| -s | The sorting type. The value can be:<br />- **al**: Sort by average lock time<br />- **ar**: Sort by average number of returned records<br />- **at**: Sort by average execution time<br />- **c**: Sort by count<br />- **l**: Sort by total lock time<br />- **r**: Sort by total number of returned records<br />- **t**: Sort by total execution time<br />
If no value is specified, **at** is used by default. |
| -t NUM | Specifies a number _n_. Only the first _n_ statements will be returned in the output. |
| -g | Specifies a string. Only statements that contain the string are considered. |
| -h | The host name. |
| -i | The name of the instance. |
| -l | Do not subtrack lock wait time from total execution time. |

# profiling
The profiling variable can be used to records detailed information about an SQL statement in each state during the whole execution process. The recorded information includes CPU utilization, I/O usage, swap space usage, and the name, source file, and position of each function that is called.<br />The following example shows how to use profiling:

1. Enable profiling for the current thread.
```sql
set profiling=on;
```

2. Query the statements executed during the course of the current thread.
```sql
show profiles;
```

3. Query the CPU and I/O overhead of an SQL statement in each state during the execution.
```sql
show profile cpu,block io for query query_id;
```

4. Check the total overhead of a SQL statement in each state during the execution.
```sql
show profile all for query query_id;
```
Output example:
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
The following table describes each status that an SQL statement may pass through:

| **Status** | **Description** |
| --- | --- |
| starting | Performing lexical analysis and syntax analysis on the SQL statement to generate a parse tree. |
| checking permissions | Checking whether the server has the required privileges to execute the statement. |
| opening tables | Opening the tables and adding metadata locks. |
| System lock | Identifying which type of locks causes the system lock. |
| optimizing | Optimizing the statement to generate an execution plan. |
| statistics |  |
| preparing |  |
| Sending data | Transmitting data between the server and the storage engine. |
| Update | If the SQL statement is an INSERT statement, it will be in this state when a row lock occurs. |
| Updating | If the SQL statement is a DELETE or UPDATE statement, it will be in this state when a row lock occurs. |
| query end | Submitting the SQL statement. If a large transaction occurs, the statement will be in this state. |
| closing tables | Flushing the changed table data to disk and closing the used tables or rolling the statement back. |
| freeing items | Releasing the parse tree. |
| Creating tmp table | Creating a temporary table to save copied data. The temporary table is deleted after use. If the statement enters this state, it normally includes GROUP BY, DINSTICT, or subqueries. |
| Copying to tmp table on disk | Copying temporary files from memory to disks. |
| Creating sort index | Sorting data. If the statement enters this state, it normally includes ORDER BY. |
| locked | Encountering a congestion. |
| Waiting for table metadata lock | Waiting for a metadata lock. |

### info
If the statement remains in any of the following states for a long time, the SQL statement has performance issues or is waiting for process:
- **Sending data**
- **Creating tmp table**
- **Copying to tmp table on disk**
- **Creating sort index**
- **locked**
- **Waiting for table metadata lock**

# Optimizer trace
The optimizer trace feature helps you understand the process of generating an execution plan. This feature is controlled by system variable **optimizer_trace**.
```sql
mysql> show variables like 'optimizer_trace';
+-----------------+--------------------------+
| Variable_name   | Value                    |
+-----------------+--------------------------+
| optimizer_trace | enabled=off,one_line=off |
+-----------------+--------------------------+
```
Variable **enabled** specifies whether the feature is enabled. **enabled=off** indicates that the optimizer trace feature is disabled by default. To enable the optimizer trace feature, set **enabled** to **on**. Variable **one_line** specifies whether the output is displayed in one line. **one_line=off** indicates that line breaks are used in the output by default.

To use the optimizer trace feature, perform the following steps:

1. Configure the cache size for optimizer trace to ensure that traces are not truncated.
```sql
set optimizer_trace_max_mem_size = 100*1024;
```

2. Enable the optimizer trace feature.
```sql
set optimizer_trace='enabled=on';
```

3. Execute an SQL statement.
### info
If the execution time is too long and you want traces generated during the execution of the SQL statement, you can execute an EXPLAIN statement to explain the statement, instead.


4. Check the optimization process of the previous SQL statement in the **optimizer_trace** table.
```sql
select * from information_schema.optimizer_trace;
```

5. Disable the optimizer trace feature.
```sql
set optimizer_trace='enabled=off';
```
The output of optimizer trace is generated through three steps, namely:

1. join_preparation
1. join_optimization
1. join_execution

In the join_optimization step, the statement is optimized based on cost. For a one-table query, the row_estimation sub-step is important, which analyzes the cost of full table scans and index scans. For a multi-table query, the considered_execution_plans sub-step is important, which analyzes the cost of every possible join order.

Key sub-steps in the join_optimization step:

condition_processing: In this sub-step, the optimizer processes the query conditions, such as passing the value of an argument to an equivalent argument and removing unnecessary conditions. For example, the source condition "a=1 and b=a" is converted to "a=1 and b=1".

rows_estimation: estimates the relevant rows for a one-table query.

- table_scan: full table scan statistics

- rows: the number of rows estimated to scan

- cost: the cost of full table scans

- potential_range_indexes: the indexes that may be used

- "index": "PRIMARY",   # The primary key index cannot be used.

- "usable": false,

- "cause": "not_applicable"

- "index": "idx_xxx", # The **id**`_x_xxx_` index may be used.

- "usable": true,

- "key_parts": [<br/>"column1",<br/>"column2",<br/>"id"<br/>analyzing_range_alternatives: analyzes the cost of each index that may be used.<br/>  "index": "idx_xxx",<br/>"ranges": [<br/>"2 `<=` column1 `<=` 2 AND 0 `<=` column2 `<=` 0"<br/>],

- "index_dives_for_eq_ranges": true,  # Index dives are used.

- "rowid_ordered": true,   # Whether the records obtained by using the index are sorted by primary key.
 
- "using_mrr": false, # Whether Multi-Range Read (MRR) is used.
 
- "index_only": false,   # Whether to enable covering indexes.
 
- "rows": xxx, # Whether the index is used to estimate the number of relevant rows.
 
- "cost": xxx,   # The cost of using the index.
  
- "chosen": true   # Whether the index is used.
  
- chosen_range_access_summary: the execution plan that is being selected.

- considered_execution_plans: analyzes every possible execution plan. This parameter is often used for JOIN queries.
 
- attaching_conditions_to_tables: adds other query conditions to the query.
 
- reconsidering_access_paths_for_index_ordering: whether the execution plan is changed due to sorting.
