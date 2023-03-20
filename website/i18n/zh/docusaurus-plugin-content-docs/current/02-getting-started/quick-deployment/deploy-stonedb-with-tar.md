---
id: deploy-stonedb-with-tar
sidebar_position: 3.13
---

# 使用 TAR 包快速部署 StoneDB

## 步骤 1：安装 StoneDB
1. 下载 StoneDB 软件包。
```sql
wget  https://github.com/stoneatom/stonedb/releases/download/5.7-v1.0.3-GA/stonedb-ce-5.7-v1.0.3.el7.x86_64.tar.gz
```

2. 解压软件包。
```sql
tar -vxf stonedb-ce-5.7-v1.0.3.el7.x86_64.tar.gz -C /opt/
```

3. 映射依赖包。
```sql
source /opt/stonedb57/install/bin/sourceenv
```

4. 创建用户 mysql 并授予用户 mysql 访问 /opt 目录的权限。
```sql
groupadd mysql
useradd -g mysql mysql
passwd mysql

chown -R mysql:mysql /opt
```

5. 初始化数据库。
```sql
/opt/stonedb57/install/bin/mysqld --defaults-file=/opt/stonedb57/install/my.cnf --initialize --user=mysql
```

## 步骤 2：启动 StoneDB
```sql
/opt/stonedb57/install/mysql_server start
```
## 步骤 3：修改 root 初始密码

1. 获取用户 root 的初始密码。
```sql
cat /opt/stonedb57/install/log/mysqld.log |grep password

[Note] A temporary password is generated for root@localhost: ceMuEuj6l4+!

# 初始密码为 ceMuEuj6l4+!

```

2. 在 MySQL 命令行中，使用 root 账号登录 StoneDB。
```sql
/opt/stonedb57/install/bin/mysql -uroot -p -S /opt/stonedb57/install/tmp/mysql.sock

# 输入上一步的初始密码
mysql: [Warning] Using a password on the command line interface can be insecure.

```

3. 修改 root 密码。
```sql
mysql> alter user 'root'@'localhost' identified by 'stonedb123';

```

4. 授予用户 root 远程登录的权限。
```sql
mysql> create user 'root'@'%' identified by 'stonedb123';
mysql> GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
```
## 步骤 4：停止 StoneDB

如需停止 StoneDB，请执行如下命令：

```sql
/opt/stonedb57/install/bin/mysqladmin -uroot -p -S /opt/stonedb57/install/tmp/mysql.sock shutdown
# 输入更改后的 root 密码

```