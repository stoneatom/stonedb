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

chown -R mysql:mysql *
./bin/mysqld --defaults-file=./my.cnf --initialize
chown -R mysql:mysql *
sh -x ./mysql_server start
