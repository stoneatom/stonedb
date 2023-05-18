---
id: compile-using-docker-for-80
sidebar_position: 5.162
---

# Docker 下编译使用 StoneDB 8.0
## 环境简介
由于编译环境搭建第三方库较为繁琐，且Fedora，Ubuntu等环境编译存在大量依赖缺失，需要补充安装依赖，搭建麻烦，所以搭建一个Docker  Centos 编译环境容器，可以通过Docker 容器快速编译StoneDB，解决编译环境搭建繁琐问题，也可以通过Docker 容器编译后直接启动StoneDB进行调试使用。

## Docker 编译环境搭建使用步骤
本搭建文档需要提前安装好Docker，Docker 安装请参考Docker官方文档[https://docs.docker.com/engine/install/ubuntu/](https://docs.docker.com/engine/install/ubuntu/)。

### 获取镜像
Docker hub镜像地址：[stoneatom/stonedb80_buildenv](https://hub.docker.com/r/stoneatom/stonedb80_buildenv)
```bash
# 从 dockerhub 获取镜像
$ docker pull stoneatom/stonedb80_buildenv
# 查看docker 镜像
$ docker images
REPOSITORY                     TAG       IMAGE ID       CREATED       SIZE
stoneatom/stonedb80_buildenv   latest    cc644347ffed   7 months ago  771MB
```

### 启动一个容器并进入
```bash
# docker run 参数说明
# -v 目录挂载，前面是宿主机目录，后面是容器内目录,宿主机目录为stonedb源码父目录路径，本文档以/home/src路径为示例
# -p 端口映射，前面是宿主机端口，后面是容器端口,设置端口映射使得可以在容器外连接容器内的数据库服务端
$ docker run -v /home/src/:/home/ -p 23306:3306 -it stoneatom/stonedb80_buildenv /bin/bash
```

### 获取 StoneDB 8.0 源码，编译安装
```bash
root@71a1384e5ee3:/home# git clone -b stonedb-8.0-dev https://github.com/stoneatom/stonedb.git

root@fb0bf0c54de0:/home# cd stonedb
root@fb0bf0c54de0:/home/stonedb# mkdir build
root@fb0bf0c54de0:/home/stonedb# cd build
# 本文档安装路径以/stonedb8/install为例，你可以另外指定安装路径
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

### 初始化并启动StoneDB
```bash
root@fb0bf0c54de0:/home/stonedb/build# cd /stonedb8/install
# 新建目录
root@fb0bf0c54de0:/stonedb8/install# mkdir data binlog log tmp redolog undolog
# 配置 my.cnf
root@fb0bf0c54de0:/stonedb8/install# cp /home/stonedb/scripts/my.cnf.sample my.cnf
root@fb0bf0c54de0:/stonedb8/install# sed -i "s|YOUR_ABS_PATH|$(pwd)|g" my.cnf
# 初始化
root@fb0bf0c54de0:/stonedb8/install# ./bin/mysqld --defaults-file=./my.cnf --initialize-insecure
# 启动
root@fb0bf0c54de0:/stonedb8/install# ./bin/mysqld --user=root &
```

### 登录 StoneDB
```bash
root@fb0bf0c54de0:/stonedb8/install# ./bin/mysql -uroot
# 设置用户 root 的密码为 'stonedb123'
mysql> alter user 'root'@'localhost' identified by 'stonedb123';
# 允许远程访问
mysql> use mysql;
mysql> update user set host='%' where user='root';
```