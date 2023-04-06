---
id: quick-deployment-of-stonedb-as-the-secondary-database
sidebar_position: 3.5
---

# 快速部署 StoneDB 为从库

## 前提条件
- 当前环境中存在两台服务器，且两台服务器上分别安装了 MySQL 和 StoneDB。关于如何安装 StoneDB，请参考[快速部署 StoneDB-5.7](https://stonedb.io/docs/quick-deployment)。
- StoneDB 和 MySQL 之间的网络已联通。
- StoneDB 的备份目录磁盘空间充足。
- StoneDB 和 MySQL 均能正常启动。
- StoneDB 和 MySQL 均开启了 GTID 模式。
- MySQL 上已创建复制用户。


:::info
当前脚本只支持 StoneDB-5.7 版本。
:::

## 操作步骤

1. 下载 Shell 脚本。您可以通过以下两种方式进行下载：
   - GitHub 下载
```sql
wget https://github.com/stoneatom/stonedb/releases/download/5.7-v1.0.3-GA/shell.for.deploy.stonedb.as.replic.tar.gz
```

   - Gitee 下载
```sql
wget  https://gitee.com/StoneDB/stonedb/releases/download/5.7-v1.0.3-GA/shell.for.deploy.stonedb.as.replic.tar.gz
```
## 解压 Shell 脚本安装包。
```sql
tar zxvf shell.for.deploy.stonedb.as.replic.tar.gz
```

3. 执行 Shell 脚本。

执行之前请先阅读解压目录下的 README.txt 了解注意事项。
```sql
cd shell.for.deploy.stonedb.as.replic

# 执行之前请先阅读 README.txt。
sh stonedb_slave_save_log.sh
```

4. 输入主从信息参数。
```sql

# sh stonedb_slave_save_log.sh

启动该脚本前需要检查以下环境：
1.主从库网络是否联通
2.从库备份目录磁盘空间是否充足
3.主从库是否正常启动
4.主从库是否开启GTID模式
5.主库是否创建复制用户
以上条件是否满足？ 输入y，继续，输入n，退出.:y
+ read -p 1、请输入StoneDB从库的安装目录: INSTALL_PATH
1、请输入StoneDB从库的安装目录:/opt/stonedb57/install
+ read -p 2、请输入主库IP: MASTER_IP
2、请输入主库IP:192.168.64.2
+ read -p 3、请输入主库数据库端口: MASTER_PORT
3、请输入主库数据库端口:3306
+ read -p 4、请输入主库用户: MASTER_USER
4、请输入主库用户:root
+ read -p 5、请输入主库用户密码: MASTER_USER_PASSWORD
5、请输入主库用户密码:root
+ read -p 6、请输入StoneDB从库用户: SLAVE_USER
6、请输入StoneDB从库用户:root
+ read -p 7、请输入StoneDB从库用户密码: SLAVE_USER_PASSWORD
7、请输入StoneDB从库用户密码:root
+ read -p 8、请输入主库的复制用户: REPLICATION_USER
8、请输入主库的复制用户:repl
+ read -p 9、请输入主库的复制用户密码: REPLICATION_USER_PAPASSWORD
9、请输入主库的复制用户密码:repl
+ read -p 10、请输入导出目录: EXPORT_PATH
10、请输入导出目录:/backup

+ change_sync_master
+ mysql -h127.0.0.1 -uroot -proot -e 'change master to master_host='\''192.168.64.2'\'',master_port=3306,master_user='\''repl'\'',master_password='\''repl'\'',master_auto_position=1;start slave;'
mysql: [Warning] Using a password on the command line interface can be insecure.
++ mysql -h127.0.0.1 -uroot -proot -e 'show slave status\G'
++ grep -E 'Slave.*Running: Yes'
++ wc -l
mysql: [Warning] Using a password on the command line interface can be insecure.
+ slave_status=2
+ '[' 2 -lt 2 ']'
+ echo 'StoneDB Master Replication setting successfully'
StoneDB Master Replication setting successfully

```
如果界面返回“Replication setting successfully”则说明主从搭建完成。

5. 在命令行中，使用复制账号登录 StoneDB ，确认主从同步状态。
```sql
#登录 StoneDB。
/opt/stonedb57/install/bin/mysql -uroot -p -S /opt/stonedb57/install/tmp/mysql.sock

#查看主从同步状态。
mysql> show slave status\G

```
