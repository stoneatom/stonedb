#!/bin/sh
  
# invoke by scripts/CMakeLists.txt
cp my.cnf.sample my.cnf
cp mysql_server.sample mysql_server
echo $1 > ../build/install_target.tmp
sed -i 's/\//~/g' ../build/install_target.tmp
TARGET=`cat ../build/install_target.tmp`
sed -i 's/YOUR_ABS_PATH/'$TARGET'/g' my.cnf
sed -i 's/YOUR_ABS_PATH/'$TARGET'/g' mysql_server
sed -i 's/~/\//g' my.cnf
sed -i 's/~/\//g' mysql_server
rm -f ../build/install_target.tmp
