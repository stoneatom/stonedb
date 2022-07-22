---
id: failed-to-start-in-kvm
sidebar_position: 9.6
---

# Failed to Start StoneDB in a KVM

An error is returned when StoneDB is started in a kernel-based virtual machine (KVM).

The following code provides an example.
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
The status code and error message are **22159 Illegal instruction**.

This error occurs when the system cannot identify the instruction set. After GDB is used to analyze the core dump files, the cause is located: Advanced Vector Extensions (AVX) is disabled. Enable AVX and then you can start StoneDB.

To check whether AVX is enabled, run the following command:
```shell
cat /proc/cpuinfo | grep avx
```
