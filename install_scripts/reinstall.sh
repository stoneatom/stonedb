mysql_server_script=mysql_server
mysql_install_db_script=mysqld

# uninstall
./${mysql_server_script} stop
echo "clean the directories..."
#sleep 3
rm -fr data
rm -fr binlog 
rm -fr log 
rm -fr tmp
rm -rf redolog
rm -rf undolog
# install begin
#echo "create the directories..."
sleep 3
if ! test -d data
then 
	mkdir data
fi

mkdir binlog
mkdir log
mkdir tmp
mkdir redolog
mkdir undolog

# init
echo "run mysqld initialize ..."
sleep 3
chown -R mysql:mysql *
./bin/mysqld --defaults-file=./stonedb.cnf --initialize
echo "start the instance..."
sleep 3
chown -R mysql:mysql *
sh -x ./${mysql_server_script} start
