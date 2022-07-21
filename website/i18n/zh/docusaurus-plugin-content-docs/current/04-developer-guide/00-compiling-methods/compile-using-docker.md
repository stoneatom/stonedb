---
id: compile-using-docker
sidebar_position: 5.15
---

# Docker 编译环境搭建和使用
## 环境简介
由于编译环境搭建第三方库较为繁琐，且Fedora，Ubuntu等环境编译存在大量依赖缺失，需要补充安装依赖，搭建麻烦，所以搭建一个Docker  Centos 编译环境容器，可以通过Docker 容器快速编译StoneDB，解决编译环境搭建繁琐问题，也可以通过Docker 容器编译后直接启动StoneDB进行调试使用。

## Docker编译环境搭建使用步骤
本搭建文档需要提前安装好Docker，Docker 安装请参考Docker官方文档[https://docs.docker.com/engine/install/ubuntu/](https://docs.docker.com/engine/install/ubuntu/)。

### 下载StoneDB源码、启动docker_buildenv 镜像
docker_buildenv镜像获取有两种方式：
#### 通过docker hub 获取
```bash
docker pull stoneatom/stonedb_buildenv
```
#### 使用docker build 创建docker 镜像
##### 下载docker.zip文件，保存到StoneDB源码根目录下解压，可参考以下步骤。
[docker_buildenv_v1.0.1.zip](https://stoneatom.yuque.com/attachments/yuque/0/2022/zip/26909006/1657765779060-80a9bc71-5205-475f-aaa1-b6b2302853cd.zip?_lake_card=%7B%22src%22%3A%22https%3A%2F%2Fstoneatom.yuque.com%2Fattachments%2Fyuque%2F0%2F2022%2Fzip%2F26909006%2F1657765779060-80a9bc71-5205-475f-aaa1-b6b2302853cd.zip%22%2C%22name%22%3A%22docker_buildenv_v1.0.1.zip%22%2C%22size%22%3A139304521%2C%22type%22%3A%22application%2Fx-zip-compressed%22%2C%22ext%22%3A%22zip%22%2C%22source%22%3A%22%22%2C%22status%22%3A%22done%22%2C%22mode%22%3A%22title%22%2C%22download%22%3Atrue%2C%22taskId%22%3A%22ua1e9f4fc-afd2-4851-8e7a-496ba3b57a2%22%2C%22taskType%22%3A%22upload%22%2C%22__spacing%22%3A%22both%22%2C%22id%22%3A%22hRoda%22%2C%22margin%22%3A%7B%22top%22%3Atrue%2C%22bottom%22%3Atrue%7D%2C%22card%22%3A%22file%22%7D)

```bash
[root@testOS src]# cd /home/src/
[root@testOS src]# git clone https://github.com/stoneatom/stonedb.git
Cloning into 'stonedb'...
remote: Enumerating objects: 84350, done.
remote: Counting objects: 100% (84350/84350), done.
remote: Total 84350 (delta 19707), reused 83550 (delta 19707), pack-reused 0
Receiving objects: 100% (84350/84350), 402.19 MiB | 13.50 MiB/s, done.
Resolving deltas: 100% (19707/19707), done.

[root@testOS src]# cd stonedb

#使用ftp工具上传docker.zip到本目录下解压
[root@testOS stonedb]# unzip docker_buildenv.zip
[root@testOS stonedb]# tree docker_buildenv
docker_buildenv
├── cmake.tar.gz
├── docker_build.sh
├── Dockerfile
├── README.md
├── stonedb-boost1.66.tar.gz
├── stonedb-gcc-rocksdb6.12.6.tar.gz
└── stonedb-marisa.tar.gz


0 directories, 7 files

```
##### Docker build

```bash
[root@testOS stonedb]# cd docker
[root@testOS docker]# chmod u+x docker_build.sh
#之前环境内build过镜像，使用缓存较快，第一次需要进行镜像依赖安装，会久一点
#脚本使用方法：./docker_build.sh tag  tag为打的镜像tag号
#例如：./docker_build.sh 0.1
[root@testOS docker]# ./docker_build.sh v0.1
/home/src
Sending build context to Docker daemon  99.41MB
Step 1/14 : FROM centos:7
 ---> eeb6ee3f44bd
Step 2/14 : ENV container docker
 ---> Using cache
 ---> dc33c0e29f61
Step 3/14 : RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == systemd-tmpfiles-setup.service ] || rm -f $i; done); rm -f /lib/systemd/system/multi-user.target.wants/*;rm -f /etc/systemd/system/*.wants/*;rm -f /lib/systemd/system/local-fs.target.wants/*; rm -f /lib/systemd/system/sockets.target.wants/*udev*; rm -f /lib/systemd/system/sockets.target.wants/*initctl*; rm -f /lib/systemd/system/basic.target.wants/*;rm -f /lib/systemd/system/anaconda.target.wants/*;
 ---> Using cache
 ---> 12ca4ee4c8b0
Step 4/14 : RUN yum install -y epel-release.noarch
 ---> Using cache
 ---> 8b9a0d9cb423
Step 5/14 : RUN  yum install -y snappy-devel lz4-devel bzip2-devel libzstd-devel.x86_64 ncurses-devel make bison libaio ncurses-devel perl perl-DBI perl-DBD-MySQL perl-Time-HiRes readline-devel numactl zlib-devel curldevel openssl-devel redhat-lsb-core.x86_64 git
 ---> Using cache
 ---> c7cf717b95c4
Step 6/14 : RUN yum install -y centos-release-scl && yum install devtoolset-7-gcc* -y
 ---> Using cache
 ---> e512aca12c7e
Step 7/14 : RUN ln -s /opt/rh/devtoolset-7/root/bin/gcc /usr/bin/gcc    && ln -s /opt/rh/devtoolset-7/root/bin/g++ /usr/bin/g++    && ln -s /opt/rh/devtoolset-7/root/bin/c++ /usr/bin/c++
 ---> Using cache
 ---> 39cb9ada4812
Step 8/14 : RUN yum clean all
 ---> Using cache
 ---> 1370d1dc1a8e
Step 9/14 : ADD cmake.tar.gz /usr/local/
 ---> Using cache
 ---> f93823785ade
Step 10/14 : RUN ln -s /usr/local/cmake/bin/cmake /usr/bin/
 ---> Using cache
 ---> f5f9d2b3c35b
Step 11/14 : ADD stonedb-marisa.tar.gz /usr/local/
 ---> Using cache
 ---> e787d2341307
Step 12/14 : ADD stonedb-boost1.66.tar.gz /usr/local/
 ---> Using cache
 ---> 5d115e2ddb34
Step 13/14 : ADD stonedb-gcc-rocksdb.tar.gz /usr/local/
 ---> Using cache
 ---> a338f6756d71
Step 14/14 : CMD ["/usr/sbin/init"]
 ---> Using cache
 ---> 38381cd2bf3d
Successfully built 38381cd2bf3d
Successfully tagged stonedb_buildenv:v0.1
Docker build success!you can run it:
        docker run -d -p 23306:3306 -v /home/src:/home/ stonedb_buildenv:v0.1



```

### 进入容器编译StoneDB
```bash
# docker run 参数说明
# -v 目录挂载，前面是宿主机目录，后面是容器内目录,宿主机目录为stonedb源码父目录路径，本文档以/home/src路径为示例
# -p 端口映射，前面是宿主机端口，后面是容器端口,
#    这里设置端口映射是为了后面容器内可以直接运行试用StoneDB，如果不需要在容器中试用可以忽略该配置
# docker run 可以参考上面docker build 成功后的参考命令
[root@testOS docker]# docker run -d -p 23306:3306 -v /home/src:/home/ stonedb_buildenv:v0.1
06f1f385d3b35c86c4ed324731a13785b2a66f8ef2c3423c9b4711c56de1910f
[root@testOS docker]# docker ps
CONTAINER ID        IMAGE                   COMMAND             CREATED             STATUS              PORTS                     NAMES
06f1f385d3b3        stonedb_buildenv:v0.1   "/usr/sbin/init"    18 seconds ago      Up 17 seconds       0.0.0.0:23306->3306/tcp   confident_tesla

#进入Docker 容器内部进行编译StoneDB
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
-DCMAKE_BUILD_TYPE=RelWithDebInfo \
-DCMAKE_INSTALL_PREFIX=/stonedb56/install \
-DMYSQL_DATADIR=/stonedb56/install/data \
-DSYSCONFDIR=/stonedb56/install \
-DMYSQL_UNIX_ADDR=/stonedb56/install/tmp/mysql.sock \
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
-DWITH_BOOST=/usr/local/stonedb-boost/

#等待cmake 结束，然后执行make和make install
[root@06f1f385d3b3 build]# make 
[root@06f1f385d3b3 build]# make install
```

## （可选）后续步骤
编译make 成功后，可以将编译文件打包成tar拷贝出容器，或者直接在容器中运行。
### tar打包导出
```bash
#/home目录挂载到容器外，所以直接tar打包到挂载目录即可直接打包到容器外
[root@06f1f385d3b3 build]# tar -zcPvf /home/stonedb56.tar.gz /stonedb56/
```
### 容器中直接运行试用StoneDB
可以参考：[StoneDB快速部署手册](https://stoneatom.yuque.com/staff-ft8n1u/dghuxr/cumqaz)，
或者在容器中参考以下方法快速部署进行试用。
```bash
[root@06f1f385d3b3 build]# cd /stonedb56/install/

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
[root@06f1f385d3b3 install]# ./reinstall.sh

...

#出现以下信息即启动成功
+ log_success_msg
+ /etc/redhat-lsb/lsb_log_message success
/etc/redhat-lsb/lsb_log_message: line 3: /etc/init.d/functions: No such file or directory
/etc/redhat-lsb/lsb_log_message: line 11: success: command not found

+ return 0
+ return_value=0
+ test -w /var/lock/subsys
+ touch /var/lock/subsys/mysql
+ exit 0


# 修改本地root 密码
[root@06f1f385d3b3 install]# /stonedb56/install/bin/mysqladmin flush-privileges -u root password "*******"
Warning: Using a password on the command line interface can be insecure.
# 创建远程链接账号密码
[root@06f1f385d3b3 install]# /stonedb56/install/bin/mysql -uroot -p*******
Warning: Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 5
Server version: 5.6.24-StoneDB-log build-

Copyright (c) 2000, 2022 StoneAtom Group Holding Limited
Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> grant all ON *.* to root@'%' identified by '********';
Query OK, 0 rows affected (0.00 sec)

mysql> flush privileges;
Query OK, 0 rows affected (0.00 sec)

```
容器内启动StoneDB后，可以在容器内登录使用，也可以通过docker run -p挂载映射的端口进行访问。

