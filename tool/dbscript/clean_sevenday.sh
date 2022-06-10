#!/bin/bash
if [ $# -ne 4 ];then
    echo "******************************************"
    echo "args error"
    echo "******************************************"
    exit;
fi
TMP_FILE=/usr/local/stonedb/shell/clean_sevenday.sh.log
db=$1
table=$2
begin=$3
end=$4
cmddir=/usr/local/stonedb/bin/
if [ ! -x $cmddir/mysql ]
then
        echo "error:mysql not exist:$cmddir"
        exit
fi
for((i = $begin;i <= $end;i++ ))
do
        table_name=${table}_0${i}
        sql=$(cat << 'EOF'
        delete from $table_name where TIMESTAMPDIFF(DAY,gmt_modified,now()) > 7;
EOF
        )
        a='$table_name'
        sql=$(echo $sql | sed "s/$a/$table_name/g")
        echo $(date +%c),$sql
        $cmddir/mysql -uroot $db -s -e "$sql" 
done

echo 'end'
