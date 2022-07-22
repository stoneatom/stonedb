---
id: mdl-wait
sidebar_position: 9.3
---

# MDL锁等待

虽然StoneDB不支持事务，但如果显示开启事务，并且没有及时提交或者回滚事务，这时候申请的MDL读锁是没有释放的。如果另一个线程执行对这张表的DDL操作就会被阻塞（这是因为DDL操作要申请MDL写锁，写锁与读锁互斥），此时刚好对这张表有并发的查询和更新，那么这些线程都会被阻塞，如果不及时找到阻塞者，很容易将连接数打满。
如果开启了performance_schema，可以使用以下SQL找到阻塞者。
```sql
select locked_schema,
locked_table,
locked_type,
waiting_processlist_id,
waiting_age,
waiting_query,
waiting_state,
blocking_processlist_id,
blocking_age,
substring_index(sql_text, "transaction_begin;", -1) AS blocking_query,
sql_kill_blocking_connection
from (select b.owner_thread_id as granted_thread_id,
      a.object_schema as locked_schema,
      a.object_name as locked_table,
      "Metadata Lock" AS locked_type,
      c.processlist_id as waiting_processlist_id,
      c.processlist_time as waiting_age,
      c.processlist_info as waiting_query,
      c.processlist_state as waiting_state,
      d.processlist_id as blocking_processlist_id,
      d.processlist_time as blocking_age,
      d.processlist_info as blocking_query,
      concat('kill ', d.processlist_id) as sql_kill_blocking_connection
      from performance_schema.metadata_locks a
      join performance_schema.metadata_locks b
      on a.object_schema = b.object_schema
      and a.object_name = b.object_name
      and a.lock_status = 'PENDING'
      and b.lock_status = 'GRANTED'
      and a.owner_thread_id <> b.owner_thread_id
      and a.lock_type = 'EXCLUSIVE'
      join performance_schema.threads c
      on a.owner_thread_id = c.thread_id
      join performance_schema.threads d
      on b.owner_thread_id = d.thread_id) t1,
      (select thread_id,
       group_concat(case
                    when event_name = 'statement/sql/begin' then
                    "transaction_begin"
                    else
                    sql_text
                    end order by event_id separator ";") as sql_text
       from performance_schema.events_statements_history
       group by thread_id) t2
 where t1.granted_thread_id = t2.thread_id;
```
如果没有开启performance_schema，且是5.7版本，可以使用以下SQL找到阻塞者。
```sql
select * from sys.schema_table_lock_waits where blocking_lock_type <> 'SHARED_UPGRADABLE'\G
```
