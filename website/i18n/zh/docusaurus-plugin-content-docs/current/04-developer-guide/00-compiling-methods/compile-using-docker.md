---
id: compile-using-docker
sidebar_position: 5.15
---

# Docker 下编译使用 StoneDB
## 环境简介
由于编译环境搭建第三方库较为繁琐，且Fedora，Ubuntu等环境编译存在大量依赖缺失，需要补充安装依赖，搭建麻烦，所以搭建一个Docker  Centos 编译环境容器，可以通过Docker 容器快速编译StoneDB，解决编译环境搭建繁琐问题，也可以通过Docker 容器编译后直接启动StoneDB进行调试使用。

## Docker编译环境搭建使用步骤
本搭建文档需要提前安装好Docker，Docker 安装请参考Docker官方文档[https://docs.docker.com/engine/install/ubuntu/](https://docs.docker.com/engine/install/ubuntu/)。

### 下载StoneDB源码、启动docker_buildenv 镜像
Docker hub镜像地址：[stoneatom/stonedb_buildenv](https://hub.docker.com/r/stoneatom/stonedb_buildenv)
#### 通过docker hub 获取
```bash
$ docker pull stoneatom/stonedb_buildenv
Using default tag: latest
latest: Pulling from stoneatom/stonedb_buildenv
2d473b07cdd5: Pull complete
6d19c47c65b7: Pull complete
e34f2dc49119: Pull complete
d9db1ee08930: Pull complete
da45ca64a9a0: Pull complete
9496927f6ea6: Pull complete
a75697ca3a7a: Pull complete
644ae91df978: Pull complete
bd28f0e05bf1: Pull complete
6f7060f9ac74: Pull complete
fe70789b39f3: Pull complete
bad0be27952c: Pull complete
Digest: sha256:2a96e743759263267ae5cfbab6abee3c84e31f84d0399d326c9432568123de87
Status: Downloaded newer image for stoneatom/stonedb_buildenv:latest
docker.io/stoneatom/stonedb_buildenv:latest

#查看docker 镜像
$ docker images
REPOSITORY                   TAG       IMAGE ID       CREATED       SIZE
stoneatom/stonedb_buildenv   latest    4409a857e962   3 weeks ago   2.29GB
```
### 获取StoneDB源码
```bash
[root@testOS src]# cd /home/src/
[root@testOS src]# git clone https://github.com/stoneatom/stonedb.git
Cloning into 'stonedb'...
remote: Enumerating objects: 84350, done.
remote: Counting objects: 100% (84350/84350), done.
remote: Total 84350 (delta 19707), reused 83550 (delta 19707), pack-reused 0
Receiving objects: 100% (84350/84350), 402.19 MiB | 13.50 MiB/s, done.
Resolving deltas: 100% (19707/19707), done.

```


### 进入容器编译StoneDB
StoneDB56和StoneDB57的编译都可以参考以下编译，不同的是需要切换对应分支
```bash
# docker run 参数说明
# -v 目录挂载，前面是宿主机目录，后面是容器内目录,宿主机目录为stonedb源码父目录路径，本文档以/home/src路径为示例
# -p 端口映射，前面是宿主机端口，后面是容器端口,
#    这里设置端口映射是为了后面容器内可以直接运行试用StoneDB，如果不需要在容器中试用可以忽略该配置
# docker run 可以参考上面docker build 成功后的参考命令
[root@testOS docker]# docker run -d -p 23306:3306 -v /home/src:/home/ stoneatom/stonedb_buildenv
06f1f385d3b35c86c4ed324731a13785b2a66f8ef2c3423c9b4711c56de1910f
[root@testOS docker]# docker ps
CONTAINER ID        IMAGE                   COMMAND             CREATED             STATUS              PORTS                     NAMES
06f1f385d3b3        stoneatom/stonedb_buildenv   "/usr/sbin/init"    18 seconds ago      Up 17 seconds       0.0.0.0:23306->3306/tcp   confident_tesla

#进入Docker 容器内部进行编译StoneDB（以StoneDB57为例）
[root@testOS docker]# docker exec -it 06f1f385d3b3 bash
[root@06f1f385d3b3 home]# cd /home/stonedb/
[root@06f1f385d3b3 stonedb]# git branch -a
* stonedb-5.7
  remotes/origin/HEAD -> origin/stonedb-5.7
  remotes/origin/stonedb-5.6
  remotes/origin/stonedb-5.7

[root@06f1f385d3b3 stonedb]# mkdir build

[root@06f1f385d3b3 stonedb]# cd build/

[root@06f1f385d3b3 build]# cmake ../ \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_INSTALL_PREFIX=/stonedb57/install \
-DMYSQL_DATADIR=/stonedb57/install/data \
-DSYSCONFDIR=/stonedb57/install \
-DMYSQL_UNIX_ADDR=/stonedb57/install/tmp/mysql.sock \
-DWITH_EMBEDDED_SERVER=OFF \
-DWITH_STONEDB_STORAGE_ENGINE=1 \
-DWITH_MYISAM_STORAGE_ENGINE=1 \
-DWITH_INNOBASE_STORAGE_ENGINE=1 \
-DWITH_PARTITION_STORAGE_ENGINE=1 \
-DMYSQL_TCP_PORT=3306 \
-DENABLED_LOCAL_INFILE=1 \
-DEXTRA_CHARSETS=all \
-DDEFAULT_CHARSET=utf8 \
-DDEFAULT_COLLATION=utf8_general_ci \
-DDOWNLOAD_BOOST=0 \
-DWITH_BOOST=/usr/local/stonedb-boost/ \
-DDOWNLOAD_ROCKSDB=0 \
-DWITH_ROCKSDB=/usr/local/stonedb-gcc-rocksdb/ \
-DWITH_MARISA=/usr/local/stonedb-marisa/

#等待cmake 结束，然后执行make和make install
[root@06f1f385d3b3 build]# make -j`nproc`
[root@06f1f385d3b3 build]# make -j`nproc` install
```

## （可选）后续步骤
编译make 成功后，可以将编译文件打包成tar拷贝出容器解包部署，或者直接在容器中部署运行。
### 1、tar打包导出到物理机安装部署
```bash
#/home目录挂载到容器外，所以直接tar打包到挂载目录即可直接打包到容器外（以stonedb57为例）
[root@06f1f385d3b3 build]# tar -zcPvf /home/stonedb57.tar.gz /stonedb57/
```
### 2、容器中直接部署试用StoneDB
可以参考：[StoneDB快速部署手册](https://stonedb.io/zh/docs/getting-started/quick-deployment)，
或者在容器中参考以下方法快速部署进行试用。
```bash
#进入编译安装的StoneDB install目录，以stonedb57为例
[root@06f1f385d3b3 build]# cd /stonedb57/install/

[root@06f1f385d3b3 install]# groupadd mysql

[root@06f1f385d3b3 install]# useradd -g mysql mysql

[root@06f1f385d3b3 install]# ll
total 180
-rw-r--r--.  1 root root  17987 Jun  8 03:41 COPYING
-rw-r--r--.  1 root root 102986 Jun  8 03:41 INSTALL-BINARY
-rw-r--r--.  1 root root   2615 Jun  8 03:41 README
drwxr-xr-x.  2 root root   4096 Jun  8 06:16 bin
drwxr-xr-x.  3 root root     18 Jun  8 06:16 data
drwxr-xr-x.  2 root root     55 Jun  8 06:16 docs
drwxr-xr-x.  3 root root   4096 Jun  8 06:16 include
-rwxr-xr-x.  1 root root    267 Jun  8 03:41 install.sh
drwxr-xr-x.  3 root root    272 Jun  8 06:16 lib
drwxr-xr-x.  4 root root     30 Jun  8 06:16 man
drwxr-xr-x. 10 root root   4096 Jun  8 06:16 mysql-test
-rwxr-xr-x.  1 root root  12516 Jun  8 03:41 mysql_server
-rwxr-xr-x.  1 root root    764 Jun  8 03:41 reinstall.sh
drwxr-xr-x.  2 root root     57 Jun  8 06:16 scripts
drwxr-xr-x. 28 root root   4096 Jun  8 06:16 share
drwxr-xr-x.  4 root root   4096 Jun  8 06:16 sql-bench
-rw-r--r--.  1 root root   5526 Jun  8 03:41 stonedb.cnf
drwxr-xr-x.  2 root root    136 Jun  8 06:16 support-files

[root@06f1f385d3b3 install]# ll {data,binlog,log,tmp,redolog,undolog}
[root@06f1f385d3b3 install]# mkdir -p ./{data/innodb,binlog,log,tmp,redolog,undolog}

[root@06f1f385d3b3 install]# chown -R mysql:mysql *
```
StoneDB56 和StoneDB57安装部署的区别在于初始化数据和重置密码上，在初始化过程中如果遇到问题建议先自行排查log/mysqld.log ,是否有报错信息，如无法自行解决，请提供StoneDB版本信息，Docker image 版本和报错日志，StoneDB版本信息到我们社区或者github 提交issues。
### StoneDB56参考
```bash
[root@06f1f385d3b3 install]# ./scripts/mysql_install_db --defaults-file=./my.cnf --user=mysql --basedir=/stonedb56/install --datadir=/stonedb56/install/data
To do so, start the server, then issue the following commands:
# 日志太多了，为了简洁只展示主要日志信息
  /stonedb56/install/bin/mysqladmin -u root password 'new-password'
  /stonedb56/install/bin/mysqladmin -u root -h 06f1f385d3b3 password 'new-password'

Alternatively you can run:

  /stonedb56/install/bin/mysql_secure_installation


# 启动StoneDB服务
[root@06f1f385d3b3 install]# ./support-files/mysql.server start
Starting MySQL..                                           [  OK  ]

# 修改密码
[root@06f1f385d3b3 install]# /stonedb56/install/bin/mysqladmin -u root password "********"
Warning: Using a password on the command line interface can be insecure.

#去除skip-grant-tables参数并重启StoneDB服务
[root@06f1f385d3b3 install]# sed -i '/skip-grant-tables/d' my.cnf
[root@06f1f385d3b3 install]# ./support-files/mysql.server restart
Shutting down MySQL.....                                   [  OK  ]
Starting MySQL.                                            [  OK  ]

# 使用新密码登录并创建远程登录账号
[root@06f1f385d3b3 install]# ./bin/mysql -uroot -p
Enter password:
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 1
Server version: 5.6.24-StoneDB build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> grant all ON *.* to root@'%' identified by '********';
Query OK, 0 rows affected (0.00 sec)

mysql> flush privileges;
Query OK, 0 rows affected (0.00 sec)


```
### StoneDB57参考
```bash

[root@06f1f385d3b3 install]# ./bin/mysqld --defaults-file=./my.cnf --initialize --user=mysql

# 启动StoneDB
[root@06f1f385d3b3 install]# ./support-files/mysql.server start
Starting MySQL.                                            [  OK  ]

# 查看初始化生成的临时登录密码
[root@06f1f385d3b3 install]# cat log/mysqld.log |grep password
2022-08-10T08:53:43.682406Z 1 [Note] A temporary password is generated for root@localhost: 30eb,hLq?x#a

# 登录StoneDB 修改root@localhost 密码
[root@06f1f385d3b3 install]# ./bin/mysql -uroot -p
Enter password:
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 2
Server version: 5.7.36-StoneDB-log

Copyright (c) 2021, 2022 StoneAtom Group Holding Limited
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> alter user root@'localhost' identified by '********';
Query OK, 0 rows affected (0.00 sec)

# 创建远程链接账号密码
mysql> grant all ON *.* to root@'%' identified by '********';
Query OK, 0 rows affected (0.00 sec)

mysql> flush privileges;
Query OK, 0 rows affected (0.00 sec)

```
容器内启动StoneDB后，可以在容器内登录使用，也可以通过docker run -p挂载映射的端口进行访问。

