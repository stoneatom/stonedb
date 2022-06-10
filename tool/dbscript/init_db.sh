#!/bin/bash
# 2015-9-23
if [ $# -ne 5 ];then
    echo "******************************************"
    echo "dir:mysql客户端路径"
    echo "db_name: 数据库名字"
    echo "exp:exp:./init_db.sh t_xc_sellers_item_reviews_subscribe.sql ce t_xc_sellers_item_reviews_subscribe 5 9"
    echo "******************************************"
    exit;
fi
mysqldir=/usr/local/stonedb/bin/
sqlfile=$1
db_name=$2
tablename=$3
begin=$4
end=$5
#create db
if [ ! -x $mysqldir/mysql ]
then
        echo "mysql not exist:$mysqldir/mysql"
        exit
fi

dbsql=$(echo "create database if not exists $db_name ")
echo $dbsql
${mysqldir}/mysql  -uroot  -s -e "$dbsql"
#create table

#echo "tablename:$tablename, db_name:${db_name}"
for((i = $begin; i <= $end; i++))
do
    if [ ${i} -lt 10 ]
    then
        table_name=${tablename}_0${i}
    else
        table_name=${tablename}_${i}
    fi
    sql=$(cat ./$sqlfile)
 
    a='$table_name'
    sql=$(echo $sql | sed "s/$a/$table_name/g")
    
    ${mysqldir}/mysql  -uroot  ${db_name} -s -e "$sql"
    echo create table: $table_name
    #echo $sql


done
