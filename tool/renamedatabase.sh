#!/bin/bash
if [ $# -ne 2 ]; then
    echo "need args:olddbname newdbname"
    exit 1
fi
mysqlconn="/usr/local/stonedb/bin/mysql -uroot"
olddb=$1
newdb=$2
echo "olddb:${olddb} newdb:${newdb}"
echo "${mysqlconn} -e \"CREATE DATABASE if not exists ${newdb}\""
$mysqlconn -e "CREATE DATABASE if not exists $newdb"
params=$($mysqlconn -N -e "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE table_schema='$olddb'")
for name in $params; do
echo "tablename:${name}"
echo "${mysqlconn} -e \"RENAME TABLE ${olddb}.$name to ${newdb}.${name}\""
$mysqlconn -e "RENAME TABLE $olddb.$name to $newdb.$name"
done;
echo "${mysqlconn} -e \"DROP DATABASE ${olddb}\""
$mysqlconn -e "DROP DATABASE $olddb"
