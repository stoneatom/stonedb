mkdir -p data/innodb
mkdir binlog
mkdir log
mkdir tmp

chown -R mysql:mysql *
sh -x ./scripts/mysql_install_db.sh --defaults-file=./stonedb.cnf --user=mysql --basedir=/stonedb/install --datadir=/stonedb/install/data
chown -R mysql:mysql *
sh -x ./mysql_server start
