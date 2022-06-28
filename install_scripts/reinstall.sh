mysql_server_script=support-files/mysql.server
#mysql_server_script=mysql_server
mysql_install_db_script=mysql_install_db
#mysql_install_db_script=mysql_install_db.sh
# uninstall
./${mysql_server_script} stop
echo "clean the directories..."
#sleep 3
rm -fr data
rm -fr binlog 
rm -fr log 
rm -fr tmp

# install begin
#echo "create the directories..."
sleep 3
if ! test -d data
then 
	mkdir data
fi
mkdir -p data/innodb
mkdir binlog
mkdir log
mkdir tmp

# init
echo "run mysql_install_db ..."
sleep 3
chown -R mysql:mysql *
./scripts/${mysql_install_db_script} --defaults-file=./my.cnf --user=mysql --basedir=/stonedb56/install --datadir=/stonedb56/install/data

echo "start the instance..."
sleep 3
chown -R mysql:mysql *
sh -x ./${mysql_server_script} start
