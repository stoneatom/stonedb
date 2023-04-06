---
id: deploy-stonedb-with-deb
sidebar_position: 3.14
---

# Ubuntu 20 下通过 DEB 包部署 StoneDB

## 步骤 1：安装 StoneDB
1. 下载 StoneDB 软件包。您可以通过以下两种方式进行下载：

- GitHub 下载
```sql
wget  https://github.com/stoneatom/stonedb/releases/download/5.7-v1.0.3-GA/stonedb-ce-5.7_v1.0.2.ubuntu.amd64.deb
```

   - Gitee 下载
```sql
wget https://gitee.com/StoneDB/stonedb/releases/download/5.7-v1.0.3-GA/stonedb-ce-5.7_v1.0.2.ubuntu.amd64.deb
```

2. 使用 DPKG 命令进行安装。
```sql
dpkg -i stonedb-ce-5.7_v1.0.2.ubuntu.amd64.deb
```
:::info
如该步骤执行失败，请执行 `ldd /opt/stonedb57/install/bin/mysqld | grep 'not found'` 命令，检查是否缺少依赖。如是，请执行 `source /opt/stonedb57/install/bin/sourceenv` 问题后重试。
:::

3. 映射依赖包。
```sql
source /opt/stonedb57/install/bin/sourceenv
```

4. 初始化数据库。
```sql
/opt/stonedb57/install/bin/mysqld --defaults-file=/opt/stonedb57/install/my.cnf --initialize --user=mysql
```

:::info
如该步骤执行失败，请执行 `ldd /opt/stonedb57/install/bin/mysqld | grep 'not found'` 命令，检查是否缺少依赖。如是，请执行 `source /opt/stonedb57/install/bin/sourceenv` 问题后重试。
:::

## 步骤 2：启动 StoneDB
```sql
/opt/stonedb57/install/mysql_server start
```

## 步骤 3：修改 root 的初始密码

1. 获取用户 root 的初始密码。
```sql
cat /opt/stonedb57/install/log/mysqld.log |grep password

[Note] A temporary password is generated for root@localhost: ceMuEuj6l4+!

# 初始密码为 ceMuEuj6l4+!

```

2. 在 MySQL 命令行中，使用 root 账号登录 StoneDB。
```sql
/opt/stonedb57/install/bin/mysql -uroot -p -S /opt/stonedb57/install/tmp/mysql.sock

# 输入上一步的初始密码。
mysql: [Warning] Using a password on the command line interface can be insecure.

```

3. 修改 root 密码。
```sql
mysql> alter user 'root'@'localhost' identified by 'stonedb123';

```

4. 授予用户 root 远程登录的权限。
```sql
mysql> GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'stonedb123';
mysql> FLUSH PRIVILEGES;
```
## 步骤 4：停止 StoneDB
如需停止 StoneDB，请执行如下命令：
```sql
/opt/stonedb57/install/bin/mysqladmin -uroot -p -S /opt/stonedb57/install/tmp/mysql.sock shutdown
#输入更改后的 root 密码。

```
