#!/bin/bash
if [ $# -ne 2 ];then
    echo "******************************************"
    echo "useage./import.sh tablename 100000 "
    echo "******************************************"
    exit;
fi
number=$2
tablename=$1
month=1
total=$(($number/($month*30)))
mod=$(($number%($month*30)))

batch=10000

echo "tablename:$tablename,number:$number,$total $mod "
echo "python generate_stonedb_gaode.py $tablename $mod  1 month 1 day"
python generate_stonedb_gaode.py $tablename $mod  1 1
for((i = 1; i <= $month; i++))
do
    for((n = 1; n <= 30; n++))
    do
        if [ ${total} -gt ${batch} ]
        then
                fornum=$(($total/$batch))
                formod=$(($total%$batch))
                for((j = 0; j < $fornum; j++))
                do
                        echo "python generate_stonedb_gaode.py $tablename $batch  $i month $n day"
                         python generate_stonedb_gaode.py $tablename $batch  $i $n
                done
                echo "python generate_stonedb_gaode.py $tablename $formod  $i month $n day"
                python generate_stonedb_gaode.py $tablename $formod  $i $n
        else
                echo "python generate_stonedb_gaode.py $tablename $total  $i month $n day"
                python generate_stonedb_gaode.py $tablename $total  $i $n
        fi
    done
done

