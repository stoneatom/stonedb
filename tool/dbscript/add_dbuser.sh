#!/bin/bash
# 2015-9-23
if [ $# -ne 2 ];then
    echo "******************************************"
    echo "user:用户名"
    echo "passwd: 密码"
    echo "exp:./add_dbuser.sh user passwd "
    echo "******************************************"
    exit;
fi
cmddir=/usr/local/stonedb/bin/
user=$1
passwd=$2
echo "user:$user passwd:$passwd"
if [ ! -x $cmddir/mysql ]
then
	 echo "error:$cmddir/mysql not exist!"
	 exit
fi
sql_local=$(echo "grant all privileges on *.* to $user@localhost identified by '$passwd' ")
sql_remote=$(echo "grant all privileges on *.* to $user@'%' identified by '$passwd' ")
echo $sql_local
echo $sql_remote
${cmddir}/mysql  -uroot   -s -e "$sql_local"
${cmddir}/mysql  -uroot   -s -e "$sql_remote"
${cmddir}/mysql  -uroot   -s -e "flush privileges "
echo "flush privileges"
