---
id: compile-using-docker
sidebar_position: 5.15
---

# Compile StoneDB in a Docker Container
## Introduction
Compiling StoneDB on a physical server requires installation of third-party repositories, which is complicated. In addition, if the OS in your environment is Fedora or Ubuntu, you also need to install many dependencies. We recommend that you compile StoneDB in a Docker container. After StoneDB is compiled, you can directly run StoneDB in the container or copy the compilation files to your environment.
## Prerequisites
Docker has been installed. For information about how to install Docker, visit [https://docs.docker.com/engine/install/ubuntu/](https://docs.docker.com/engine/install/ubuntu/).
## Procedure
### Step 1. Download the source code of StoneDB and start the docker_buildenv image
The **docker_buildenv** image can be obtained by using two ways:

- Pull it from Docker Hub
```bash
docker pull stoneatom/stonedb_buildenv
```

- Run the docker build command to create the Docker image
1. Download the [docker.zip](https://static.stoneatom.com/stonedb_docker_220706.zip) file, save it to the root directory of the source code of StoneDB, and then decompress it. 
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

#Use an FTP tool to upload 'docker.zip' to this directory for decompression.
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

2. Build the Docker image.
```bash
[root@testOS stonedb]# cd docker
[root@testOS docker]# chmod u+x docker_build.sh
# If an image has been created in your environment, you can use the cache. If this is the first image that is to be created in your environment, you must install dependencies. This may take a longer period of time.
# Run the './docker_build.sh <tag>' command to call the script. <tag> specifies the tag of the image.
# Example './docker_build.sh 0.1'
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
### Step 2. Enter the container and compile StoneDB
```bash
# docker run parameter description
# -v Directory mounting. Specify the directory on the host first and then the directory in the container.
# -p Port mapping. Specify the port on the host first and then the port in the container.
#    After configuring the port mapping, you can directly start StoneDB in the container. If you do not need the trial, you can skip this parameter.
# docker run You can refer to the commands used in 'Step 2. Build a Docker image'.
[root@testOS docker]# docker run -d -p 23306:3306 -v /home/src:/home/ stonedb_buildenv:v0.1
06f1f385d3b35c86c4ed324731a13785b2a66f8ef2c3423c9b4711c56de1910f
[root@testOS docker]# docker ps
CONTAINER ID        IMAGE                   COMMAND             CREATED             STATUS              PORTS                     NAMES
06f1f385d3b3        stonedb_buildenv:v0.1   "/usr/sbin/init"    18 seconds ago      Up 17 seconds       0.0.0.0:23306->3306/tcp   confident_tesla

# Enter the Docker container and compile StoneDB.
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

#After the 'cmake' command is completed, run the 'make' and 'make install' commands.
[root@06f1f385d3b3 build]# make 
[root@06f1f385d3b3 build]# make install
```

## (Optional) Follow-up operations
After the `make` commands are successful, you can choose either to compress the compilation files to a TAR file and copy the TAR file from the container or to directly run it in the container.
### Compress compilation files to a TAR file
```bash
# Compress the 'home' folder to a TAR file and mount the TAR file to a directory outside the container.
[root@06f1f385d3b3 build]# tar -zcPvf /home/stonedb56.tar.gz /stonedb56/
```
### Directly use StoneDB in the container
You can refer to [Quick Deployment](.../../../../02-getting-started/quick-deployment/quick-deployment-56.md) or the following code to deploy and use StoneDB in the container.
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

# If the following information is returned, StoneDB is started.
+ log_success_msg
+ /etc/redhat-lsb/lsb_log_message success
/etc/redhat-lsb/lsb_log_message: line 3: /etc/init.d/functions: No such file or directory
/etc/redhat-lsb/lsb_log_message: line 11: success: command not found

+ return 0
+ return_value=0
+ test -w /var/lock/subsys
+ touch /var/lock/subsys/mysql
+ exit 0


# Reset the password of local user 'root'.
[root@06f1f385d3b3 install]# /stonedb56/install/bin/mysqladmin flush-privileges -u root password "*******"
Warning: Using a password on the command line interface can be insecure.
# Create a username and password for remote connection.
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
After you start StoneDB in the container, you can log in to and use StoneDB or run the `docker run -p <port mapping>` command to connect to StoneDB.
