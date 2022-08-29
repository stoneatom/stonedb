---
id: use-mysql-client
sidebar_position: 5.21
---

# 通过MySQL客户端连接StoneDB

本文主要介绍如何使用 MySQL 客户端连接 StoneDB，包含前提条件、连接示例和参数说明。
## 前提条件
本地已安装 MySQL 客户端，StoneDB 当前版本支持的 MySQL 客户端版本包括 V5.5、V5.6 和 V5.7。
## 连接示例
输入连接参数，格式请参见如下示例。
```shell
/stonedb/install/bin/mysql -uroot -p -S /stonedb/install/tmp/mysql.sock
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 5
Server version: 5.7.36-StoneDB-log build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
No entry for terminal type "xterm";
using dumb terminal settings.
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> 
```
## 参数说明
主要参数含义如下：

- -h：连接 StoneDB 数据库的IP
- -u：连接 StoneDB 数据库的用户
- -p：用户的密码，为了安全可以不提供，可在提示符下输入，密码文本不可见
- -P：StoneDB 数据库的端口，默认是 3306，可以自定义
- -A：指定连接的数据库名
- -S：使用 sock 文件连接 StoneDB

如果要退出 StoneDB 命令行，可以输入 exit 后按回车键。

:::tip
更多连接参数详见 "mysql --help" 的返回结果。
:::
