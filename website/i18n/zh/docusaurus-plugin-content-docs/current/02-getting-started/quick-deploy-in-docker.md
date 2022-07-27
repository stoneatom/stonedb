---
id: quick-deploy-in-docker
sidebar_position: 3.2
---

# Docker快速部署StoneDB

## StoneDB Docker Hub地址
[Docker Hub](https://hub.docker.com/r/stoneatom/stonedb)
## 使用方法
默认登录账号密码为root，stonedb123
### 1、docker pull
```bash
docker pull stoneatom/stonedb:v0.1
```
### 2、docker run
参数说明：
-p：端口映射，把容器端口映射到宿主机端口上，：前面是宿主机端口，后面是容器端口
-v：目录挂载，如果没有挂载的话，容器重启会进行初始化，：前面是宿主机映射路径，后面是容器映射路径
-i：交互式操作
-t：终端
-d：启动不进入容器，想要进入容器需要使用指令 docker exec
```bash
docker run -p 13306:3306 -v $stonedb_volumn_dir/data/:/stonedb56/install/data/ -it -d stoneatom/stonedb:v0.1 /bin/bash
```
or 
```bash
docker run -p 13306:3306 -it -d stoneatom/stonedb:v0.1 /bin/bash
```
### 3、登录容器内使用stonedb
```bash
#获取docker 容器ID
docker ps
#通过docker ps获取容器ID 进入docker容器
docker exec -it 容器ID bash
容器ID$ /stonedb56/install/bin/mysql -uroot -pstonedb123
```
### 4、容器外登录stonedb
使用mysql client 登录，其他第三方工具（例如Navicat、DBEAVER）登录方式等类似
```shell
mysql -h宿主机IP -uroot -pstonedb123 -P宿主机映射端口
```
