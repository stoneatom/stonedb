---
id: configuration-parameters
sidebar_position: 5.62
---

# 设置参数

StoneDB的所有参数默认存放在/stonedb/install/stonedb.cnf，这个参数文件也可以包含其他存储引擎的参数。StoneDB的参数和其他存储引擎的参数在修改上是有区别的，StoneDB的参数只能静态修改，即只能编辑参数文件，然后重启StoneDB。
用户可根据自己的环境要求进行设置参数，以下是设置参数的演示。
## 指定存储引擎类型
存储引擎类型由参数default_storage_engine决定，这个参数可以在会话级和全局动态修改，但如果数据库实例重启，参数会失效，即参数变为原来的值。如果想参数永久生效，需要编辑参数文件，然后重启StoneDB。
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
以上可知，数据库的默认存储引擎为MyISAM，在全局级修改后，数据库的默认存储引擎为STONEDB。
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
以上可知，数据库实例重启后，参数default_storage_engine变回MyISAM，如果想参数永久生效，需要编辑参数文件，然后重启StoneDB。以下是演示介绍静态修改参数。
## 指定insert buffer大小
StoneDB的参数只支持静态修改，修改完成后，需要重启StoneDB。
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
以上可知，参数stonedb_insert_buffer_size已经修改为1024MB。
