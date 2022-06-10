#!/bin/bash
if [ $# -ne 4 ];then
    echo "******************************************"
    echo "args num error"
    echo "example:./install.sh es t_xc_sellers_item_reviews_subscribe 5 9"
    echo "******************************************"
    exit;
fi
db=$1
table=$2
begin=$3
end=$4

echo $table.sql
if [ ! -f $table.sql ]
then
	echo "$table.sql not exit"
	exit
fi
sh ./init_db.sh  $table.sql $db $table $begin $end > ./init_db.sh.log 2>&1;
