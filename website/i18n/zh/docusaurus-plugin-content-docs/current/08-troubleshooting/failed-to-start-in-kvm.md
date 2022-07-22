---
id: failed-to-start-in-kvm
sidebar_position: 9.6
---

# KVM环境下StoneDB启动失败

StoneDB在KVM环境下启动时，有如下报错。
```shell
# /stonedb/install/bin/mysql_server start
Starting stonedbbasedir::: /stonedb/install/
bindir::: /stonedb/install//bin
datadir::: /stonedb/install/data
mysqld_pid::: /stonedb/install/data/mysqld.pid
...220307 02:14:15 mysqld_safe Logging to '/stonedb/install/log/mysqld.log'.
.220307 02:14:15 mysqld_safe Starting mysqld daemon with databases from /stonedb/install/data
/stonedb/install//bin/mysqld_safe: line 166: 22159 Illegal instruction     nohup /stonedb/install/bin/mysqld --basedir=/stonedb/install/ --datadir=/stonedb/install/data --plugin-dir=/stonedb/install/lib/plugin --user=mysql --log-error=/stonedb/install/log/mysqld.log --open-files-limit=65535 --pid-file=/stonedb/install/data/mysqld.pid --socket=/stonedb/install//tmp/mysql.sock --port=3306 < /dev/null >> /stonedb/install/log/mysqld.log 2>&1:q
220307 02:14:15 mysqld_safe mysqld from pid file /stonedb/install/data/mysqld.pid ended
./mysql_server: line 264: kill: (20941) - No such process
 ERROR!
```
其中重要的错误信息是：**22159 Illegal instruction**
该报错是因为无法识别指令集，通过gdb分析core dumpfile，其对应的指令集为AVX。修改支持AVX指令集后，可以正确启动StoneDB。 
检查AVX指令集是否开启的命令如下：
```shell
cat /proc/cpuinfo | grep avx
```
