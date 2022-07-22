---
id: failed-to-connect
sidebar_position: 9.7
---

# Failed to Connect to StoneDB

# **Too many connections**
If the following error is returned when you connect to StoneDB, the maximum number of connections specified by parameter **max_connections** has been reached. You must log in as user admin, release idle connections, and determine whether to set **max_connections** to a larger value based on your service requirements.
```
ERROR 1040 (HY000): Too many connections
```
# **Metadata lock waits**
If your request for connecting to StoneDB is suspended, a metadata lock wait may occur. You need to log in as user admin and check whether the command output contains a large number of "Waiting for table metadata lock" messages. If yes, locate and terminate the thread that cause the congestion.
```sql
show processlist；
```
# **Incorrect username or password, or insufficient permissions**
If the following error is returned when you try to connect to StoneDB, the username or password is incorrect, or you are not granted with the permissions to access StoneDB.
```
ERROR 1045 (28000): Access denied for user 'sjj'@'localhost' (using password: YES) 
```
