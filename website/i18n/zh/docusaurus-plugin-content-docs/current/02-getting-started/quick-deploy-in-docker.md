---
id: quick-deploy-in-docker
sidebar_position: 3.2
---

# Docker快速部署StoneDB
## StoneDB Docker Hub地址

建议您阅读Docker hub的最新文档。
[Docker Hub](https://hub.docker.com/r/stoneatom/stonedb)

## 使用方法
默认登录账号密码为 root，密码会随机生成，也可以自定义。

确保你的 CPU 支持 AVX

```bash
cat /proc/cpuinfo |grep avx
```
### 1、docker pull
```bash
docker pull stoneatom/stonedb:v1.0.3
```
### 2、docker run
参数说明：

-p：端口映射，把容器端口映射到宿主机端口上，前面是宿主机端口，后面是容器端口

-v：目录挂载，如果没有挂载的话，容器重启会进行初始化，前面是宿主机映射路径，后面是容器映射路径

-i：交互式操作

-t：终端

-d：启动不进入容器，想要进入容器需要使用指令 docker exec

```bash
docker run -p 3306:3306 -itd -v $YOU_DATA_DIR:/opt -e MYSQL_ROOT_PASSWORD='$YOU_PASSWORD' stoneatom/stonedb:v1.0.3
```
or 
```bash
docker run -p 3306:3306 -itd  -e MYSQL_ROOT_PASSWORD='$YOU_PASSWORD'  stoneatom/stonedb:v1.0.3
```
### 3、登录容器内使用 StoneDB
```bash
#获取 docker ID
$ docker ps

#通过 docker ps 获取 docker ID，进入容器
docker exec -it <Container ID> bash
#如果$YOU_PASSWORD不正确，请去log中查看默认生成的密码
<Container ID>$ /opt/stonedb57/install/bin/mysql -uroot -p$YOU_PASSWORD
```
### 4、容器外登录StoneDB
使用客户端登录，其他第三方工具，如 Navicat、DBeaver 登录方式类似
```shell
mysql -h<Host IP address> -uroot -p$YOU_PASSWORD -P<Mapped port of the host>
```