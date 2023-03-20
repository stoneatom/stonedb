---
id: quick-deployment-of-stonedb-as-the-secondary-database
sidebar_position: 3.5
---

# Quick Deployment of StoneDB as the Secondary Database

## Prerequisites
- Two servers are deployed in your environment. MySQL and StoneDB are respectively installed on each server. For details about how to install StoneDB, see [Quick Deployment of StoneDB-5.7](https://stonedb.io/docs/quick-deployment).
- StoneDB can communicate with MySQL.
- The disk space for the backup directory of StoneDB is sufficient.
- StoneDB and MySQL can properly run.
- StoneDB and MySQL are enabled with the GTID mode.
- A replication account is on MySQL.
:::info
Currently, this shell script applies only to StoneDB-5.7.
:::
## Procedure

1. Download the shell script.
```sql
wget https://github.com/stoneatom/stonedb/releases/download/5.7-v1.0.3-GA/shell.for.deploy.stonedb.as.replic.tar.gz
```

2. Download the package of the shell script.
```sql
tar zxvf shell.for.deploy.stonedb.as.replic.tar.gz
```

3. Run the shell script.

Before you run the shell script, read file **README.txt**.
```sql
cd shell.for.deploy.stonedb.as.replic

# Before you run the shell script, read file README.txt.
sh stonedb_slave_save_log.sh
```

4. Specify primary/secondary configuration parameters.
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
If **Replication setting successfully** is displayed, the primary/secondary configuration is completed.

5. In a MySQL command-line interface, use the replication account to log in to StoneDB to confirm the primary/secondary status.
```sql
# Log in to StoneDB.
/opt/stonedb57/install/bin/mysql -uroot -p -S /opt/stonedb57/install/tmp/mysql.sock

# Check the primary/secondary status.
mysql> show slave status\G

```
