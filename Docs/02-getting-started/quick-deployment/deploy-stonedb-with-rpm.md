---
id: deploy-stonedb-with-rpm
sidebar_position: 3.15
---

# Use an RPM Package to Deploy StoneDB on CentOS 7

## Step 1. Install StoneDB
1. Download the StoneDB software package. 
```sql
wget  https://github.com/stoneatom/stonedb/releases/download/5.7-v1.0.3-GA/stonedb-ce-5.7-v1.0.3.el7.x86_64.rpm
```

2. Install the RPM package.
```sql
rpm -ivh stonedb-ce-5.7-v1.0.3.el7.x86_64.rpm
```
:::info
If this step fails,  run `ldd /opt/stonedb57/install/bin/mysqld | grep 'not found'` to check whether any dependent libraries are missing. If yes, run `source /opt/stonedb57/install/bin/sourceenv` and then retry this step.
:::

3. Map dependent libraries.
```sql
source /opt/stonedb57/install/bin/sourceenv
```


4. Initialize the database.
```sql
/opt/stonedb57/install/bin/mysqld --defaults-file=/opt/stonedb57/install/my.cnf --initialize --user=mysql
```
:::info
If this step fails,  run `ldd /opt/stonedb57/install/bin/mysqld | grep 'not found'` to check whether any dependent libraries are missing. If yes, run `source /opt/stonedb57/install/bin/sourceenv` and then retry this step.
:::

## Step 2. Start StoneDB
```sql
/opt/stonedb57/install/mysql_server start
```

## Step 3. Change the Initial Password of User root

1. Obtain the initial password of user **root**.
```sql
cat /opt/stonedb57/install/log/mysqld.log |grep password

[Note] A temporary password is generated for root@localhost: ceMuEuj6l4+!

# The initial password of user root is ceMuEuj6l4+!

```

2. In a MySQL command-line interface (CLI), log in to StoneDB as user **root**.
```sql
/opt/stonedb57/install/bin/mysql -uroot -p -S /opt/stonedb57/install/tmp/mysql.sock

# Enter the initial password obtained in step 1.
mysql: [Warning] Using a
```

3. Reset the password for user **root**.
```sql
mysql> alter user 'root'@'localhost' identified by 'stonedb123';

```

4. Grant the remote login privilege to user **root**.
```sql
mysql> GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'stonedb123';
mysql> FLUSH PRIVILEGES;
```
## Step 4. Stop StoneDB
If you want to stop StoneDB, run the following command:
```sql
/opt/stonedb57/install/bin/mysqladmin -uroot -p -S /opt/stonedb57/install/tmp/mysql.sock shutdown
# Enter the new password of user root.

```
