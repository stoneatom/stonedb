---
id: configuration-parameters
sidebar_position: 5.71
---

# 设置参数

StoneDB 的所有参数存放在 my.cnf，这个参数文件也可以包含其他存储引擎的参数。StoneDB 的参数和其他存储引擎的参数在修改上是一样的，即能静态修改，也能动态修改，不过动态修改的参数，重启 StoneDB 后会失效。

:::tip
注：StoneDB 5.6 的参数以 stonedb 前缀，5.7 的参数以 tianmu 前缀。
:::
# 指定默认存储引擎
存储引擎类型由参数 default_storage_engine 决定，这个参数可以在会话级或全局动态修改，但如果数据库实例重启，参数会失效，即参数变为原来的值。如果想参数永久生效，需要编辑参数文件，然后重启 StoneDB。
```shell
mysql> show variables like 'default_storage_engine';
+------------------------+--------+
| Variable_name          | Value  |
+------------------------+--------+
| default_storage_engine | MyISAM |
+------------------------+--------+
1 row in set (0.00 sec)

mysql> set global default_storage_engine=tianmu;
Query OK, 0 rows affected (0.00 sec)

mysql> exit

...

mysql> show variables like 'default_storage_engine';
+------------------------+---------+
| Variable_name          | Value   |
+------------------------+---------+
| default_storage_engine | TIANMU |
+------------------------+---------+
1 row in set (0.00 sec)
```
以上可知，数据库的默认存储引擎为 MyISAM，在全局级修改后，数据库的默认存储引擎为 TIANMU。
```sql
mysql> shutdown;
Query OK, 0 rows affected (0.00 sec)

mysql> exit
Bye

...

mysql> show variables like 'default_storage_engine';
+------------------------+--------+
| Variable_name          | Value  |
+------------------------+--------+
| default_storage_engine | MyISAM |
+------------------------+--------+
1 row in set (0.00 sec)
```
以上可知，数据库实例重启后，参数 default_storage_engine 变回 MyISAM，如果想参数永久生效，需要编辑参数文件，然后重启 StoneDB。
# 指定 insert buffer 大小
```sql
mysql> show variables like 'tianmu_insert_buffer_size';
+----------------------------+-------+
| Variable_name              | Value |
+----------------------------+-------+
| tianmu_insert_buffer_size | 512   |
+----------------------------+-------+
1 row in set (0.00 sec)

# vi /stonedb/install/my.cnf
tianmu_insert_buffer_size = 1024

mysql> shutdown;
Query OK, 0 rows affected (0.00 sec)

...

mysql> show variables like 'tianmu_insert_buffer_size';
+----------------------------+-------+
| Variable_name              | Value |
+----------------------------+-------+
| tianmu_insert_buffer_size | 1024  |
+----------------------------+-------+
1 row in set (0.00 sec)

```
以上可知，参数 tianmu_insert_buffer_size 已经修改为 1024MB。