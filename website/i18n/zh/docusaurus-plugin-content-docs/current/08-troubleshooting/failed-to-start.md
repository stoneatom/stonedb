---
id: failed-to-start
sidebar_position: 9.1
---

# StoneDB启动失败

实例启动失败的原因有很多，主要还是从mysqld.log查找相关报错信息，以下是实例启动失败的常见原因。
## 参数设置不合理
如果参数设置不合理，mysqld.log有明确提示是哪个参数，如下所示是参数datadir的路径设置不正确。
```
[ERROR] failed to set datadir to /stonedb/install/dataxxx/
```
## 无法访问资源
无法访问资源包括端口被占用，目录的属主或者权限错误，导致无法访问目录。
```
Error: unable to create temporary file; errno: 13
```
## 数据页被损坏
如果数据页被损坏，实例是无法启动的，这种情况只能通过备份来还原。

