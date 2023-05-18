---
id: compile-using-docker-for-80
sidebar_position: 5.162
---

# Compile StoneDB 8.0 in a Docker Container
## Introduction
Compiling StoneDB on a physical server requires installation of third-party repositories, which is complicated. In addition, if the OS in your environment is Fedora or Ubuntu, you also need to install many dependencies. We recommend that you compile StoneDB in a Docker container. After StoneDB is compiled, you can directly run StoneDB in the container or copy the compilation files to your environment.

## Prerequisites
Docker has been installed. For information about how to install Docker, visit [https://docs.docker.com/engine/install/ubuntu/](https://docs.docker.com/engine/install/ubuntu/).

## Procedure
The url of our image:[stoneatom/stonedb80_buildenv](https://hub.docker.com/r/stoneatom/stonedb80_buildenv)

### Pull image from dockerhub
```bash
$ docker pull stoneatom/stonedb80_buildenv
# show all images
$ docker images
REPOSITORY                     TAG       IMAGE ID       CREATED       SIZE
stoneatom/stonedb80_buildenv   latest    cc644347ffed   7 months ago  771MB
```

### Launch a container & Get into it
```bash
# docker run parameter description
# -v Directory mounting. Specify the directory on the host first and then the directory in the container.
# -p Port mapping. Specify the port on the host first and then the port in the container.It can allow connection 
#    from outside the container.
$ docker run -v /home/src/:/home/ -p 23306:3306 -it stoneatom/stonedb80_buildenv /bin/bash
```

### Download source code & Compile and Install
```bash
root@71a1384e5ee3:/home# git clone -b stonedb-8.0-dev https://github.com/stoneatom/stonedb.git

root@fb0bf0c54de0:/home# cd stonedb
root@fb0bf0c54de0:/home/stonedb# mkdir build
root@fb0bf0c54de0:/home/stonedb# cd build
# The intall directory here is /stonedb8/install，you can change it by yourself
root@fb0bf0c54de0:/home/stonedb/build# cmake .. \
-DCMAKE_BUILD_TYPE=Release \
-DCMAKE_INSTALL_PREFIX=/stonedb8/install \
-DMYSQL_DATADIR=/stonedb8/install/data \
-DSYSCONFDIR=/stonedb8/install \
-DMYSQL_UNIX_ADDR=/stonedb8/install/tmp/mysql.sock \
-DWITH_BOOST=/usr/local/stonedb-boost \
-DWITH_MARISA=/usr/local/stonedb-marisa \
-DWITH_ROCKSDB=/usr/local/stonedb-gcc-rocksdb

root@fb0bf0c54de0:/home/stonedb/build# make -j `nproc` && make install -j`nproc`
```

### Initialize & Start StoneDB
```bash
root@fb0bf0c54de0:/home/stonedb/build# cd /stonedb8/install
# Create directories
root@fb0bf0c54de0:/stonedb8/install# mkdir data binlog log tmp redolog undolog
# Configure parameters in my.cnf
root@fb0bf0c54de0:/stonedb8/install# cp /home/stonedb/scripts/my.cnf.sample my.cnf
root@fb0bf0c54de0:/stonedb8/install# sed -i "s|YOUR_ABS_PATH|$(pwd)|g" my.cnf
# Initialize StoneDB.
root@fb0bf0c54de0:/stonedb8/install# ./bin/mysqld --defaults-file=./my.cnf --initialize-insecure
# Start StoneDB
root@fb0bf0c54de0:/stonedb8/install# ./bin/mysqld --user=root &
```

### Login StoneDB
```bash
root@fb0bf0c54de0:/stonedb8/install# ./bin/mysql -uroot
# Set the password of user root to stonedb123
mysql> alter user 'root'@'localhost' identified by 'stonedb123';
# Allow remote access
mysql> use mysql;
mysql> update user set host='%' where user='root';
```