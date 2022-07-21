---
id: use-mysql-client
sidebar_position: 5.21
---

# 通过MySQL客户端连接StoneDB

本文主要介绍如何使用MySQL客户端连接StoneDB，包含前提条件、连接示例和参数说明。
## 前提条件

- 本地已安装MySQL客户端，StoneDB当前版本支持的MySQL客户端版本包括 V5.5、V5.6 和V5.7；
- 环境变量PATH包含了MySQL客户端命令所在目录。
## 连接示例
1）打开命令行终端；
2）输入连接参数，格式请参见如下示例。
```shell
mysql -u -p -h -P -S -A
```
## 参数说明
主要参数含义如下：

- -h：连接StoneDB数据库的IP
- -u：连接StoneDB数据库的用户
- -p：用户的密码，为了安全可以不提供，可在提示符下输入，密码文本不可见
- -P：StoneDB 数据库的端口，默认是3306，可以自定义
- -A：指定连接的数据库名
- -S：使用sock文件连接StoneDB

如果要退出 StoneDB 命令行，可以输入exit后按回车键。
注：更多连接参数详见"mysql --help"的返回结果。
