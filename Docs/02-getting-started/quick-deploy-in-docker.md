---
id: quick-deploy-in-docker
sidebar_position: 3.2
---

# Quick Deployment in a Docker Container
## Prerequisites
The image of StoneDB is downloaded from Docker Hub.You can read the Dockerhub documentation, which must be up to date.
[Docker Hub](https://hub.docker.com/r/stoneatom/stonedb)

## Procedure
The username and password for login are **root** and **stonedb123**.

Make sure your CPU supports AVX

```bash
cat /proc/cpuinfo |grep avx
```
### 1. Pull the image
Run the following command:
```bash
docker pull stoneatom/stonedb:v1.0.3
```
### 2. Run the image
Run the following command:
```bash
docker run -p 3306:3306 -itd -v $YOU_DATA_DIR:/opt -e MYSQL_ROOT_PASSWORD='$YOU_PASSWORD' stoneatom/stonedb:v1.0.3
```
Altenatively, run the following command:
```bash
docker run -p 3306:3306 -itd  -e MYSQL_ROOT_PASSWORD='$YOU_PASSWORD'  stoneatom/stonedb:v1.0.3
```
Parameter description:

- **-p**: maps ports in the _Port of the host_:_Port of the container_ format.
- **-v**: mounts directories in the _Path in the host_:_Path in the container_ format. If no directories ae mounted, the container will be initialized.
- **-i**: the interaction.
- **-t**: the terminal.
- **-d**: Do not enter the container upon startup. If you want to enter the container upon startup, run the  docker exec command.
### 3. Log in to StoneDB in the container
```bash
# Obtain the Docker container ID.
docker ps
# Use the "cocker ps" command to obtain the container ID and enter the Docker container.
docker exec -it <Container ID> bash

<Container ID>$ /opt/stonedb57/install/bin/mysql -uroot -p$YOU_PASSWORD
```
### **4. Log in to StoneDB using a third-party tool**
You can log in to StoneDB by using third-party tools such as mysql, Navicat, and DBeaver. The following code uses mysql as an example.
```shell
mysql -h<Host IP address> -uroot -p$YOU_PASSWORD -P<Mapped port of the host>
```