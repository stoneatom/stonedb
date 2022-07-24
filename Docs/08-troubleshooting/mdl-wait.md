---
id: mdl-wait
sidebar_position: 9.3
---

# Metadata Lock Waits

Although StoneDB does not support transactions, metadata locks for which you apply are not released if transactions are enabled and the transactions are not submitted or rolled back in time. In this case, if another thread executes a DDL operation on this table, a thread congestion will occur and then any threads perform queries or updates on this table are congested. This is because the write lock applied for by the DDL operation conflicts with the read lock. You need to locate and terminate the thread that causes the congestion in time. Otherwise, the maximum number of connections will be reached in a short period of time.

If **performance_schema** is enabled, you can execute the following statement to locate the thread that causes the congestion.
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
If **performance_schema** is disabled, execute the following statement to locate the thread.
```sql
select * from sys.schema_table_lock_waits where blocking_lock_type <> 'SHARED_UPGRADABLE'\G
```
