---
id: read_write-splitting
sidebar_position: 7.51
---

# 读写分离

## 一、ProxySQL介绍
ProxySQL是一款开源的中间件产品，是一个灵活的代理层，可以实现读写分离，支持Query路由功能，支持动态指定某个SQL进行缓存，支持动态加载（无需重启ProxySQL服务），故障切换和一些SQL的过滤功能。 
相关ProxySQL的网站
[https://www.proxysql.com/](https://www.proxysql.com/)
[https://github.com/sysown/proxysql/wiki](https://github.com/sysown/proxysql/wiki)
## 二、ProxySQL架构
![](ProxySQL.png)


## 三、ProxySQL系统表及配置详解  
### 3.1 ProxySQL端口说明
ProxySQL安装部署完成后有以下两个端口：
6032是管理端口，可以使用admin/admin账号密码登录修改系统配置，admin账号只允许本地登录，远程登录需要在配置文件中添加远程登录账号，配置远程登录账号最好在部署启动前配置，过后配置需要清空数据目录下的所有数据文件，重新初始化。
6033是客户端接口端口，登录账号密码由后端数据库设置后，再使用6032管理端口登录配置mysql_users后，再刷新账户配置。
### 3.2 ProxySQL管理端配置数据库
ProxySQL有两种配置后端数据库的方法，一种是使用配置文件进行配置，配置后启动初始化ProxySQL自动生成配置，不建议使用这种方法进行配置，另一种是通过登录管理端进行在线修改配置，修改后通过加载运行和刷盘配置到磁盘的命令做持久化保存，这种方法无需重启，配置后加载运行即可生效。
### 3.3 ProxySQL管理端数据库说明
我们通过管理端口6032登录到管理端后执行show databases可以看到如下结果：
```sql
ProxySQL proxy>show databases;
+-----+---------------+-------------------------------------+
| seq | name          | file                                |
+-----+---------------+-------------------------------------+
| 0   | main          |                                     |
| 2   | disk          | /var/lib/proxysql/proxysql.db       |
| 3   | stats         |                                     |
| 4   | monitor       |                                     |
| 5   | stats_history | /var/lib/proxysql/proxysql_stats.db |
+-----+---------------+-------------------------------------+
5 rows in set (0.00 sec)

```
库说明：
main 库内置配置数据库，表存放后端db实例、用户验证、路由规则等信息，表名以runtime 开头的表示ProxySQL当前运行的配置内容，不能通过dml语句修改，只能修改对应的不以 runtime 开头的（在内存）里的表，然后 LOAD 使其生效， SAVE 使其存到硬盘以供下次重启加载。
disk 是持久化到硬盘的配置，sqlite数据文件，避免ProxySQL重启导致配置文件丢失
stats 是ProxySQL运行抓取的统计信息，包括到后端各命令的执行次数、流量、processlist、查询种类汇总/执行时间等等。
monitor 库存储 monitor 模块收集的信息，主要是对后端db的健康/延迟检查。
stats_history 统计信息历史库
```sql
ProxySQL proxy>show tables from main;
+----------------------------------------------------+
| tables                                             |
+----------------------------------------------------+
| global_variables                                   |# ProxySQL的基本配置参数，类似与MySQL
| mysql_aws_aurora_hostgroups                        |
| mysql_collations                                   |# 配置对MySQL字符集的支持
| mysql_firewall_whitelist_rules                     |
| mysql_firewall_whitelist_sqli_fingerprints         |
| mysql_firewall_whitelist_users                     |
| mysql_galera_hostgroups                            |
| mysql_group_replication_hostgroups                 | # MGR相关的表，用于实例的读写组自动分配
| mysql_query_rules                                  | # 路由表
| mysql_query_rules_fast_routing                     | # 主从复制相关的表，用于实例的读写组自动分配
| mysql_replication_hostgroups                       | # 存储MySQL实例的信息
| mysql_servers                                      | # 现阶段存储MySQL用户，当然以后有前后端账号分离的设想
| mysql_users                                        | # 存储ProxySQL的信息，用于ProxySQL Cluster同步
| proxysql_servers                                   | 
| restapi_routes                                     |
| runtime_checksums_values                           |
| runtime_global_variables                           |
| runtime_mysql_aws_aurora_hostgroups                |
| runtime_mysql_firewall_whitelist_rules             |
| runtime_mysql_firewall_whitelist_sqli_fingerprints |
| runtime_mysql_firewall_whitelist_users             |
| runtime_mysql_galera_hostgroups                    |
| runtime_mysql_group_replication_hostgroups         | # 与上面对应，但是运行环境正在使用的配置
| runtime_mysql_query_rules                          |
| runtime_mysql_query_rules_fast_routing             |
| runtime_mysql_replication_hostgroups               |
| runtime_mysql_servers                              |
| runtime_mysql_users                                |
| runtime_proxysql_servers                           |
| runtime_restapi_routes                             |
| runtime_scheduler                                  |
| scheduler                                          |
+----------------------------------------------------+
32 rows in set (0.00 sec)

# runtime_开头的是运行时的配置，这些是不能修改的。要修改ProxySQL的配置，需要修改了非runtime_表，修改后必须执行LOAD ... 
# TO RUNTIME才能加载到RUNTIME生效，执行save ... to disk才能将配置持久化保存到磁盘。

```
## 四、安装
### 4.1 下载ProxySQL
官网下载rpm包安装，下载后执行安装（yum安装可以解决依赖问题
ProxySQL官网:[https://proxysql.com/documentation/installing-proxysql/](https://proxysql.com/documentation/installing-proxysql/)
```shell
[root@Proxysql ~]# wget https://github.com/sysown/proxysql/releases/download/v2.4.1/proxysql-2.4.1-1-centos7.x86_64.rpm
[root@Proxysql ~]# yum install proxysql-2.4.1-1-centos7.x86_64.rpm -y


[root@Proxysql ~]# systemctl start proxysql

[root@Proxysql ~]# systemctl enable proxysql

[root@Proxysql ~]# ss -lntup|grep proxy
```
### 4.2 修改proxysql.cnf 配置
```bash
[root@Proxysql ~]# vim /etc/proxysql.cnf
admin_variables=
{
#       admin_credentials="radmin:radmin"
        admin_credentials="admin:admin;radmin:radmin"
#主要修改以上配置，其他配置后面启动proxysql 后登录管理端再修改更方便
```
### 4.3 创建StoneDB账号
创建ProxySQL监控StoneDB账户，还有业务服务访问账户（StoneDB做了主从架构，所以只需要在主库上执行创建账号即可）
```bash
# proxysql监控账号
mysql>  GRANT ALL ON * . * TO  'proxysql'@'%' identified by 'xxxxxxx'; #用于proxysql监控StoneDB权限可以缩小点，后期测试下补充下缩小的权限
# 创建业务使用账号
mysql> GRANT ALL ON * . * TO  'zz'@'%' identified by 'xxxxxxx';
```
### 4.4 配置ProxySQL
登录ProxySQL 6032端口进行配置：
```bash
[root@docker ~]# mysql -h192.168.46.30 -uradmin -pradmin -P16032

mysql> select * from global_variables;
+----------------------------------------------------------------------+-----------------------------+
| variable_name                                                        | variable_value              |
+----------------------------------------------------------------------+-----------------------------+
| mysql-default_charset                                                | utf8                        |
| mysql-default_collation_connection                                   | utf8_general_ci             |
| mysql-shun_on_failures                                               | 5                           |
| mysql-shun_recovery_time_sec                                         | 10                          |
| mysql-query_retries_on_failure                                       | 1                           |
| mysql-client_multi_statements                                        | true                        |
| mysql-client_host_cache_size                                         | 0                           |
| mysql-client_host_error_counts                                       | 0                           |
| mysql-connect_retries_delay                                          | 1                           |
| mysql-connection_delay_multiplex_ms                                  | 0                           |
| mysql-connection_max_age_ms                                          | 0                           |
| mysql-connect_timeout_client                                         | 10000                       |
| mysql-connect_timeout_server_max                                     | 10000                       |
| mysql-enable_client_deprecate_eof                                    | true                        |
| mysql-enable_server_deprecate_eof                                    | true                        |
| mysql-enable_load_data_local_infile                                  | false                       |
| mysql-eventslog_filename                                             |                             |
| mysql-eventslog_filesize                                             | 104857600                   |
| mysql-eventslog_default_log                                          | 0                           |
| mysql-eventslog_format                                               | 1                           |
| mysql-auditlog_filename                                              |                             |
| mysql-auditlog_filesize                                              | 104857600                   |
| mysql-handle_unknown_charset                                         | 1                           |
| mysql-free_connections_pct                                           | 10                          |
| mysql-connection_warming                                             | false                       |
| mysql-session_idle_ms                                                | 1                           |
| mysql-have_ssl                                                       | false                       |
| mysql-client_found_rows                                              | true                        |
| mysql-log_mysql_warnings_enabled                                     | false                       |
| mysql-monitor_enabled                                                | true                        |
| mysql-monitor_connect_timeout                                        | 600                         |
| mysql-monitor_ping_max_failures                                      | 3                           |
| mysql-monitor_ping_timeout                                           | 1000                        |
| mysql-monitor_read_only_max_timeout_count                            | 3                           |
| mysql-monitor_replication_lag_interval                               | 10000                       |
| mysql-monitor_replication_lag_timeout                                | 1000                        |
| mysql-monitor_replication_lag_count                                  | 1                           |
| mysql-monitor_groupreplication_healthcheck_interval                  | 5000                        |
| mysql-monitor_groupreplication_healthcheck_timeout                   | 800                         |
| mysql-monitor_groupreplication_healthcheck_max_timeout_count         | 3                           |
| mysql-monitor_groupreplication_max_transactions_behind_count         | 3                           |
| mysql-monitor_groupreplication_max_transactions_behind_for_read_only | 1                           |
| mysql-monitor_galera_healthcheck_interval                            | 5000                        |
| mysql-monitor_galera_healthcheck_timeout                             | 800                         |
| mysql-monitor_galera_healthcheck_max_timeout_count                   | 3                           |
| mysql-monitor_replication_lag_use_percona_heartbeat                  |                             |
| mysql-monitor_query_interval                                         | 60000                       |
| mysql-monitor_query_timeout                                          | 100                         |
| mysql-monitor_slave_lag_when_null                                    | 60                          |
| mysql-monitor_threads_min                                            | 8                           |
| mysql-monitor_threads_max                                            | 128                         |
| mysql-monitor_threads_queue_maxsize                                  | 128                         |
| mysql-monitor_wait_timeout                                           | true                        |
| mysql-monitor_writer_is_also_reader                                  | true                        |
| mysql-max_allowed_packet                                             | 67108864                    |
| mysql-tcp_keepalive_time                                             | 0                           |
| mysql-use_tcp_keepalive                                              | false                       |
| mysql-automatic_detect_sqli                                          | false                       |
| mysql-firewall_whitelist_enabled                                     | false                       |
| mysql-firewall_whitelist_errormsg                                    | Firewall blocked this query |
| mysql-throttle_connections_per_sec_to_hostgroup                      | 1000000                     |
| mysql-max_transaction_idle_time                                      | 14400000                    |
| mysql-max_transaction_time                                           | 14400000                    |
| mysql-multiplexing                                                   | true                        |
| mysql-log_unhealthy_connections                                      | true                        |
| mysql-enforce_autocommit_on_reads                                    | false                       |
| mysql-autocommit_false_not_reusable                                  | false                       |
| mysql-autocommit_false_is_transaction                                | false                       |
| mysql-verbose_query_error                                            | false                       |
| mysql-hostgroup_manager_verbose                                      | 1                           |
| mysql-binlog_reader_connect_retry_msec                               | 3000                        |
| mysql-threshold_query_length                                         | 524288                      |
| mysql-threshold_resultset_size                                       | 4194304                     |
| mysql-query_digests_max_digest_length                                | 2048                        |
| mysql-query_digests_max_query_length                                 | 65000                       |
| mysql-query_digests_grouping_limit                                   | 3                           |
| mysql-wait_timeout                                                   | 28800000                    |
| mysql-throttle_max_bytes_per_second_to_client                        | 0                           |
| mysql-throttle_ratio_server_to_client                                | 0                           |
| mysql-max_stmts_per_connection                                       | 20                          |
| mysql-max_stmts_cache                                                | 10000                       |
| mysql-mirror_max_concurrency                                         | 16                          |
| mysql-mirror_max_queue_length                                        | 32000                       |
| mysql-default_max_latency_ms                                         | 1000                        |
| mysql-query_processor_iterations                                     | 0                           |
| mysql-query_processor_regex                                          | 1                           |
| mysql-set_query_lock_on_hostgroup                                    | 1                           |
| mysql-reset_connection_algorithm                                     | 2                           |
| mysql-auto_increment_delay_multiplex                                 | 5                           |
| mysql-long_query_time                                                | 1000                        |
| mysql-query_cache_size_MB                                            | 256                         |
| mysql-poll_timeout_on_failure                                        | 100                         |
| mysql-keep_multiplexing_variables                                    | tx_isolation,version        |
| mysql-kill_backend_connection_when_disconnect                        | true                        |
| mysql-client_session_track_gtid                                      | true                        |
| mysql-session_idle_show_processlist                                  | true                        |
| mysql-show_processlist_extended                                      | 0                           |
| mysql-query_digests                                                  | true                        |
| mysql-query_digests_lowercase                                        | false                       |
| mysql-query_digests_replace_null                                     | false                       |
| mysql-query_digests_no_digits                                        | false                       |
| mysql-query_digests_normalize_digest_text                            | false                       |
| mysql-query_digests_track_hostname                                   | false                       |
| mysql-servers_stats                                                  | true                        |
| mysql-default_reconnect                                              | true                        |
| mysql-ssl_p2s_ca                                                     |                             |
| mysql-ssl_p2s_capath                                                 |                             |
| mysql-ssl_p2s_cert                                                   |                             |
| mysql-ssl_p2s_key                                                    |                             |
| mysql-ssl_p2s_cipher                                                 |                             |
| mysql-ssl_p2s_crl                                                    |                             |
| mysql-ssl_p2s_crlpath                                                |                             |
| mysql-init_connect                                                   |                             |
| mysql-ldap_user_variable                                             |                             |
| mysql-add_ldap_user_comment                                          |                             |
| mysql-default_tx_isolation                                           | READ-COMMITTED              |
| mysql-default_session_track_gtids                                    | OFF                         |
| mysql-connpoll_reset_queue_length                                    | 50                          |
| mysql-min_num_servers_lantency_awareness                             | 1000                        |
| mysql-aurora_max_lag_ms_only_read_from_replicas                      | 2                           |
| mysql-stats_time_backend_query                                       | false                       |
| mysql-stats_time_query_processor                                     | false                       |
| mysql-query_cache_stores_empty_result                                | true                        |
| admin-stats_credentials                                              | stats:stats                 |
| admin-stats_mysql_connections                                        | 60                          |
| admin-stats_mysql_connection_pool                                    | 60                          |
| admin-stats_mysql_query_cache                                        | 60                          |
| admin-stats_mysql_query_digest_to_disk                               | 0                           |
| admin-stats_system_cpu                                               | 60                          |
| admin-stats_system_memory                                            | 60                          |
| admin-telnet_admin_ifaces                                            | (null)                      |
| admin-telnet_stats_ifaces                                            | (null)                      |
| admin-refresh_interval                                               | 2000                        |
| admin-read_only                                                      | false                       |
| admin-hash_passwords                                                 | true                        |
| admin-vacuum_stats                                                   | true                        |
| admin-version                                                        | 2.3.2-10-g8cd66cf           |
| admin-cluster_username                                               |                             |
| admin-cluster_password                                               |                             |
| admin-cluster_check_interval_ms                                      | 1000                        |
| admin-cluster_check_status_frequency                                 | 10                          |
| admin-cluster_mysql_query_rules_diffs_before_sync                    | 3                           |
| admin-cluster_mysql_servers_diffs_before_sync                        | 3                           |
| admin-cluster_mysql_users_diffs_before_sync                          | 3                           |
| admin-cluster_proxysql_servers_diffs_before_sync                     | 3                           |
| admin-cluster_mysql_variables_diffs_before_sync                      | 3                           |
| admin-cluster_admin_variables_diffs_before_sync                      | 3                           |
| admin-cluster_ldap_variables_diffs_before_sync                       | 3                           |
| admin-cluster_mysql_query_rules_save_to_disk                         | true                        |
| admin-cluster_mysql_servers_save_to_disk                             | true                        |
| admin-cluster_mysql_users_save_to_disk                               | true                        |
| admin-cluster_proxysql_servers_save_to_disk                          | true                        |
| admin-cluster_mysql_variables_save_to_disk                           | true                        |
| admin-cluster_admin_variables_save_to_disk                           | true                        |
| admin-cluster_ldap_variables_save_to_disk                            | true                        |
| admin-checksum_mysql_query_rules                                     | true                        |
| admin-checksum_mysql_servers                                         | true                        |
| admin-checksum_mysql_users                                           | true                        |
| admin-checksum_mysql_variables                                       | true                        |
| admin-checksum_admin_variables                                       | true                        |
| admin-checksum_ldap_variables                                        | true                        |
| admin-restapi_enabled                                                | false                       |
| admin-restapi_port                                                   | 6070                        |
| admin-web_enabled                                                    | false                       |
| admin-web_port                                                       | 6080                        |
| admin-web_verbosity                                                  | 0                           |
| admin-prometheus_memory_metrics_interval                             | 61                          |
| admin-admin_credentials                                              | admin:admin;radmin:radmin   |
| admin-mysql_ifaces                                                   | 0.0.0.0:6032                |
| mysql-threads                                                        | 4                           |
| mysql-max_connections                                                | 2048                        |
| mysql-default_query_delay                                            | 0                           |
| mysql-default_query_timeout                                          | 36000000                    |
| mysql-have_compress                                                  | true                        |
| mysql-poll_timeout                                                   | 2000                        |
| mysql-interfaces                                                     | 0.0.0.0:6033                |
| mysql-default_schema                                                 | information_schema          |
| mysql-stacksize                                                      | 1048576                     |
| mysql-server_version                                                 | 5.5.30                      |
| mysql-connect_timeout_server                                         | 3000                        |
| mysql-monitor_username                                               | root                        |
| mysql-monitor_password                                               | 110022                      |
| mysql-monitor_history                                                | 600000                      |
| mysql-monitor_connect_interval                                       | 60000                       |
| mysql-monitor_ping_interval                                          | 10000                       |
| mysql-monitor_read_only_interval                                     | 1500                        |
| mysql-monitor_read_only_timeout                                      | 500                         |
| mysql-ping_interval_server_msec                                      | 120000                      |
| mysql-ping_timeout_server                                            | 500                         |
| mysql-commands_stats                                                 | true                        |
| mysql-sessions_sort                                                  | true                        |
| mysql-connect_retries_on_failure                                     | 10                          |
| mysql-server_capabilities                                            | 569899                      |
+----------------------------------------------------------------------+-----------------------------+

#参数很多，本次搭建主要改mysql-monitor_username，mysql-monitor_password
mysql> UPDATE global_variables SET variable_value='proxysql' where variable_name='mysql-monitor_username';
Query OK, 1 row affected (0.01 sec)

mysql> UPDATE global_variables SET variable_value='proxysql' where variable_name='mysql-monitor_password';
Query OK, 1 row affected (0.00 sec)

#修改完后并不是立即生效的，需要手动加载生效，并持久化到磁盘
MySQL [(none)]> LOAD MYSQL VARIABLES TO RUNTIME;
 
MySQL [(none)]> SAVE MYSQL VARIABLES TO DISK;

#配置业务登录账号，业务登录账号需要mysql上也有这个账号，配置账号可以通过上面myql步骤进行设置
#也可以设置密码加密。加密插入语句可以参考

#StoneDB执行
SELECT
   CONCAT(
       "insert into mysql_users(username,password,active,default_hostgroup,transaction_persistent) values('",`User`,"','",password," ',1,100,1);")
   FROM
       mysql.`user` WHERE `User`='xxxx'
       

mysql> INSERT INTO MySQL_users(username,password,default_hostgroup) VALUES ('zz','xxxxx',100);
Query OK, 1 row affected (0.00 sec)

mysql> LOAD MYSQL SERVERS TO RUNTIME;
Query OK, 0 rows affected (0.00 sec)

mysql> SAVE MYSQL SERVERS TO DISK;
Query OK, 0 rows affected (0.00 sec)

MySQL [(none)]> LOAD MYSQL USERS TO RUNTIME;
 
MySQL [(none)]> SAVE MYSQL USERS TO DISK;

```
配置读写分离组，设置group 100为写组，200为读组
```bash
mysql> insert into mysql_servers (hostgroup_id, hostname, port) values(100,'192.168.46.10',3306);
mysql> insert into mysql_servers (hostgroup_id, hostname, port) values(200,'192.168.46.20',3306);
mysql> LOAD MYSQL SERVERS TO RUNTIME;
mysql> SAVE MYSQL SERVERS TO DISK;
mysql> select * from  mysql_servers ;
+--------------+----------------+-------+-----------+--------+--------+-------------+-----------------+---------------------+---------+----------------+---------+
| hostgroup_id | hostname       | port  | gtid_port | status | weight | compression | max_connections | max_replication_lag | use_ssl | max_latency_ms | comment |
+--------------+----------------+-------+-----------+--------+--------+-------------+-----------------+---------------------+---------+----------------+---------+
| 100          | 192.168.46.10  | 3306  | 0         | ONLINE | 1      | 0           | 1000            | 0                   | 0       | 0              |         |
| 200          | 192.168.46.20  | 3306  | 0         | ONLINE | 1      | 0           | 1000            | 0                   | 0       | 0              |         |
+--------------+----------------+-------+-----------+--------+--------+-------------+-----------------+---------------------+---------+----------------+---------+
2 rows in set (0.00 sec)

```
设置读写分离规则
```bash
mysql> insert into mysql_query_rules (active, match_pattern, destination_hostgroup, apply) values (1,"^SELECT",200,1);
mysql> insert into mysql_query_rules(active,match_pattern,destination_hostgroup,apply) values(1,'^SELECT.*FOR UPDATE$',100,1);
mysql> LOAD MYSQL QUERY RULES TO RUNTIME;
mysql> SAVE MYSQL QUERY RULES TO DISK;
```
### 4.5测试读写分离规则
```bash
[root@docker ~]# mysql -hxxx.xxx.xxx.xxx -uzz -pxxxxxxx -P6033 -e "select @@hostname"
Warning: Using a password on the command line interface can be insecure.
+--------------+
| @@hostname   |
+--------------+
| mysql2       |
+--------------+
```
## 五、扩展知识
查看读写分离SQL执行记录
```bash
select hostgroup,username,digest_text,count_star from stats_mysql_query_digest
```
清理SQL执行记录
```bash
select * from stats_mysql_query_digest_reset;
```
设置高可用读写分离切换
```bash
mysql> insert into  mysql_replication_hostgroups (writer_hostgroup,reader_hostgroup,comment)values(100,200,'测试我的读写分离高可用');
mysql> load mysql servers to runtime;
Query OK, 0 rows affected (0.01 sec)
mysql> save mysql servers to disk;
Query OK, 0 rows affected (0.03 sec)
mysql> select * from runtime_mysql_replication_hostgroups;
+------------------+------------------+-----------------------------------+
| writer_hostgroup | reader_hostgroup | comment                           |
+------------------+------------------+-----------------------------------+
| 100              | 1000             | 测试我的读写分离高可用            |
+------------------+------------------+-----------------------------------+
1 row in set (0.00 sec)
```
