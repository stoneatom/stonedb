---
id: configuration-parameters
sidebar_position: 5.71
---

# Configure Parameters

By default, all parameters of the StoneDB storage engine are saved in **/stonedb/install/stonedb.cnf**. Parameters of other storage engines can also be saved in file **stonedb.cnf**. If you want to modify parameter settings of the StoneDB storage engine, you must modify them in file **stonedb.cnf**, and then restart the StoneDB instance to make the modification take effect. This is because the StoneDB storage engine supports only static modification of parameter settings, which is different from other storage engines.

You can configure parameters based on your environment requirements. The following examples show how to configure parameters respectively in a dynamic and static manner.

# Example 1: Change the storage engine type

Parameter **default_storage_engine** specifies the storage engine type. You can dynamically set this parameter at the session level or the global level. However, If the database is restarted, the value of this parameter is restored to the default value. If you want to make a permanent modification, change the value of this parameter in file **stonedb.cnf** and restart the StoneDB instance.

Code example of changing the default storage engine:

```shell
# mysql -uroot -p -P3308
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 926
Server version: 5.7.36-StoneDB-log build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
No entry for terminal type "xterm";
using dumb terminal settings.
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show variables like 'default_storage_engine';
+------------------------+--------+
| Variable_name          | Value  |
+------------------------+--------+
| default_storage_engine | MyISAM |
+------------------------+--------+
1 row in set (0.00 sec)

mysql> set global default_storage_engine=StoneDB;
Query OK, 0 rows affected (0.00 sec)

mysql> exit
Bye
# mysql -uroot -p -P3308
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 927
Server version: 5.7.36-StoneDB-log build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
No entry for terminal type "xterm";
using dumb terminal settings.
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show variables like 'default_storage_engine';
+------------------------+---------+
| Variable_name          | Value   |
+------------------------+---------+
| default_storage_engine | STONEDB |
+------------------------+---------+
1 row in set (0.00 sec)
```

The default storage engine of the database is modified from **MyISAM** to **STONEDB** at a global level.

Code example of restarting the StoneDB instance:

```sql
mysql> shutdown;
Query OK, 0 rows affected (0.00 sec)

mysql> exit
Bye
# mysql -uroot -p -P3308
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 2
Server version: 5.7.36-StoneDB-log build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
No entry for terminal type "xterm";
using dumb terminal settings.
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show variables like 'default_storage_engine';
+------------------------+--------+
| Variable_name          | Value  |
+------------------------+--------+
| default_storage_engine | MyISAM |
+------------------------+--------+
1 row in set (0.00 sec)
```

After the StoneDB instance is restarted, the value of **default_storage_engine** is restored to **MyISAM**. If you want to make your change persistently effective, edit file **stonedb.cnf** to modify the parameter setting and then restart the StoneDB instance.

# Example 2: Change the insert buffer size
The parameters of the StoneDB storage engine support only static modification. After the parameter settings are modified, restart the StoneDB instance to make the modification take effect.

Code example:
```sql
mysql> show variables like 'stonedb_insert_buffer_size';
+----------------------------+-------+
| Variable_name              | Value |
+----------------------------+-------+
| stonedb_insert_buffer_size | 512   |
+----------------------------+-------+
1 row in set (0.00 sec)

# vi /stonedb/install/stonedb.cnf
stonedb_insert_buffer_size = 1024

mysql> shutdown;
Query OK, 0 rows affected (0.00 sec)

# /stonedb/install//bin/mysqld_safe --datadir=/stonedb/install/data/ --user=mysql &

# mysql -uroot -p -P3308
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 2
Server version: 5.7.36-StoneDB-log build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
No entry for terminal type "xterm";
using dumb terminal settings.
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show variables like 'stonedb_insert_buffer_size';
+----------------------------+-------+
| Variable_name              | Value |
+----------------------------+-------+
| stonedb_insert_buffer_size | 1024  |
+----------------------------+-------+
1 row in set (0.00 sec)
```

**stonedb_insert_buffer_size** is set to 1024 MB.
