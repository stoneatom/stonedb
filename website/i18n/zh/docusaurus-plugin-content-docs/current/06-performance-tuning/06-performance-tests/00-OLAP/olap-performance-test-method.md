---
id: olap-performance-test-method
sidebar_position: 7.611
---

# StoneDB性能测试方法（OLAP）

# TPCH介绍
TPC-H是TPC提供的一个benchmark，用来模拟一个现实中的商业应用，可以生成一堆虚构的数据，且自带一些查询，可以导入到各种数据库中来模拟现实需求，检查性能。TPC-H 主要用于评测数据库的分析型查询能力。TPC-H查询包含8张数据表、22条复杂的SQL查询，大多数查询包含若干表Join、子查询和Group by聚合等，具体介绍可查看TPC-H官网：[https://www.tpc.org/tpch/](https://www.tpc.org/tpch/)
# 测试环境介绍
系统：Centos 7.9
CPU：Intel(R) Xeon(R) CPU E5-2683 v4 @ 2.10GHz   16核64线程
内存：125G
测试StoneDB版本：单节点部署
```shell
bin/mysqld  Ver 5.6.24-StoneDB for Linux on x86_64 (build-)
build information as follow:
        Repository address: https://github.com/stoneatom/stonedb.git:stonedb-5.6
        Branch name: stonedb-5.6
        Last commit ID: 90583b2
        Last commit time: Date:   Wed Jul 6 23:31:30 2022 +0800
        Build time: Date: Thu Jul  7 05:39:39 UTC 2022
```
# 测试方案
## 1、搭建StoneDB测试环境  
搭建方案可以参考：[快速部署](..../../../../../02-getting-started/quick-deployment.md)
## 2、编译部署TPCH
下载地址：[https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
由于下载需要注册登录，通过发送下载链接到邮箱后进行下载，所以详细步骤就不多做解释
上传下载好的zip包到需要测试的服务器上，本测试文档存放于/data目录下
```shell
unzip tpc-h-tool.zip
mv TPC-H_Tools_v3.0.0/ tpc-h/
cd /data/tpc-h/dbgen/
# 安装GCC 和MAKE
yum install gcc make -y
```
由于TPCH本身不支持Mysql，需要在makefile中修改部分配置，另外需要在tpch.h新增一个mysql类型
```shell
cp makefile.suite makefile
vim makefile
################
## CHANGE NAME OF ANSI COMPILER HERE
################
CC      = gcc
# Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata)
#                                  SQLSERVER, SYBASE, ORACLE, VECTORWISE
# Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS,
#                                  SGI, SUN, U2200, VMS, LINUX, WIN32
# Current values for WORKLOAD are:  TPCH
DATABASE= MYSQL
MACHINE = LINUX
WORKLOAD = TPCH
```
同时需要修改tpcd.h文件
```shell

vim tpcd.h

#ifdef MYSQL
#define GEN_QUERY_PLAN  ""
#define START_TRAN      "START TRANSACTION"
#define END_TRAN        "COMMIT"
#define SET_OUTPUT      ""
#define SET_ROWCOUNT    "limit %d;\n"
#define SET_DBASE       "use %s;\n"
#endif

#执行make命令
make
```
## 3、使用TPCH生成测试数据100GB
TPCH会生成8张tbl后缀的数据文件，生成命令参考
-s 1 表示生成1G的数据，如果你之前曾经尝试过生成数据，最好先make clean，再重新make，接着到这步加上-f覆盖掉
```shell
./dbgen -s 10

# 复制数据文件到stonedb文件夹下，方便导入
mkdir /data/tpc-h/stonedb/
mv *.tbl /data/tpc-h/stonedb/
```
生成之后可以用head命令检查一下tbl们，会看到每一行都有一些用“|”隔开的字段。
## 4、修改dss.ddl、dss.ri 表结构和索引创建语句
dss.ddl为建表语句，dss.ri为索引和外键索引创建语句
由于不同引擎创建表结构语法不同，所以需要对建表语句和索引创建语句进行修改，具体修改可以参考以下文件
复制结构索引文件到stonedb目录下进行修改
```shell
[root@htap2 dbgen]# cp dss.ddl /data/tpc-h/stonedb/dss_stonedb.ddl			#用于Stonedb的表结构
```
```sql
-- sccsid:     @(#)dss.ddl	2.1.8.1
create table nation  ( n_nationkey  integer not null,
                            n_name       char(25) not null,
                            n_regionkey  integer not null,
                            n_comment    varchar(152),primary key (n_nationkey))engine=StoneDB;

create table region  ( r_regionkey  integer not null,
                            r_name       char(25) not null,
                            r_comment    varchar(152),primary key (r_regionkey))engine=StoneDB;

create table part  ( p_partkey     integer not null,
                          p_name        varchar(55) not null,
                          p_mfgr        char(25) not null,
                          p_brand       char(10) not null,
                          p_type        varchar(25) not null,
                          p_size        integer not null,
                          p_container   char(10) not null,
                          p_retailprice decimal(15,2) not null,
                          p_comment     varchar(23) not null,primary key (p_partkey) )engine=StoneDB;

create table supplier ( s_suppkey     integer not null,
                             s_name        char(25) not null,
                             s_address     varchar(40) not null,
                             s_nationkey   integer not null,
                             s_phone       char(15) not null,
                             s_acctbal     decimal(15,2) not null,
                             s_comment     varchar(101) not null,primary key (s_suppkey))engine=StoneDB;

create table partsupp ( ps_partkey     integer not null,
                             ps_suppkey     integer not null,
                             ps_availqty    integer not null,
                             ps_supplycost  decimal(15,2)  not null,
                             ps_comment     varchar(199) not null,primary key (ps_partkey,ps_suppkey) )engine=StoneDB;

create table customer ( c_custkey     integer not null,
                             c_name        varchar(25) not null,
                             c_address     varchar(40) not null,
                             c_nationkey   integer not null,
                             c_phone       char(15) not null,
                             c_acctbal     decimal(15,2)   not null,
                             c_mktsegment  char(10) not null,
                             c_comment     varchar(117) not null,primary key (c_custkey))engine=StoneDB;

create table orders  ( o_orderkey       integer not null,
                           o_custkey        integer not null,
                           o_orderstatus    char(1) not null,
                           o_totalprice     decimal(15,2) not null,
                           o_orderdate      date not null,
                           o_orderpriority  char(15) not null,  
                           o_clerk          char(15) not null, 
                           o_shippriority   integer not null,
                           o_comment        varchar(79) not null,primary key (o_orderkey))engine=StoneDB;

create table lineitem ( l_orderkey    integer not null,
                             l_partkey     integer not null,
                             l_suppkey     integer not null,
                             l_linenumber  integer not null,
                             l_quantity    decimal(15,2) not null,
                             l_extendedprice  decimal(15,2) not null,
                             l_discount    decimal(15,2) not null,
                             l_tax         decimal(15,2) not null,
                             l_returnflag  char(1) not null,
                             l_linestatus  char(1) not null,
                             l_shipdate    date not null,
                             l_commitdate  date not null,
                             l_receiptdate date not null,
                             l_shipinstruct char(25) not null,
                             l_shipmode     char(10) not null,
                             l_comment      varchar(44) not null,primary key (l_orderkey,l_linenumber))engine=StoneDB;


```
## 5、导入表结构和表数据
表结构导入
```sql
mysql> create database tpch;
mysql> source /data/tpc-h/stonedb/dss_stonedb.ddl  #主要PATH要修改为tpch工具存放地址
```
数据导入
导入可以先导入这五个库：part 、region 、nation 、customer 、supplier 
另外三个库lineitem、orders、partsupp数据量比较大，建议使用脚本切割后导入，切割脚本参考：split_file2db.sh
```shell
#StoneDB导入方法，
mysql -uroot -pxxxx -hxx.xx.xx.xx -P3306 --local-infile -Dtpcd -e "load data local infile '/data/tpc-h/stonedb/part.tbl' into table part fields terminated by '|';"
mysql -uroot -pxxxx -hxx.xx.xx.xx -P3306 --local-infile -Dtpcd -e "load data local infile '/data/tpc-h/stonedb/region.tbl' into table region fields terminated by '|';"
mysql -uroot -pxxxx -hxx.xx.xx.xx -P3306 --local-infile -Dtpcd -e "load data local infile '/data/tpc-h/stonedb/nation.tbl' into table nation fields terminated by '|';"
mysql -uroot -pxxxx -hxx.xx.xx.xx -P3306 --local-infile -Dtpcd -e "load data local infile '/data/tpc-h/stonedb/customer.tbl' into table customer fields terminated by '|';"
mysql -uroot -pxxxx -hxx.xx.xx.xx -P3306 --local-infile -Dtpcd -e "load data local infile '/data/tpc-h/stonedb/supplier.tbl' into table supplier fields terminated by '|';"

```
```bash
#! /bin/bash

shopt -s expand_aliases
source ~/.bash_profile
#获取当前目录下.tbl文件，并获取对应表名
sql_path=/data/tpc-h/stonedb/
# split_tb=$(ls ${sql_path}/*.ddl)
#需要拆分的数据文件
split_tb=(lineitem orders partsupp)
# split_tb=(customer nation supplier part region)
# split_tb=(part nation)
#切割设置
#要分割成的每个小文件的行数line
line=1000000  

#数据库配置
db_host=192.168.30.102
db_port=3306
db_user=ztpch
db_pwd=******
db=ztpch  




#拆分大SQL文件
function split_file()
{
    for tb_name in ${split_tb[@]}
    do
        echo "$tb_name"
        #获取原文件总行数totalline
        totalline=$(cat $sql_path/$tb_name.tbl | wc -l)
        # echo totalline=$totalline
        a=`expr $totalline / $line`
        b=`expr $totalline % $line` 
        if  [[ $b -eq 0 ]] ;then
            filenum=$a
        else
            filenum=`expr $a + 1`
        fi
        # echo filenum=$filenum
        echo "$tb_name 共有 $totalline行数据,需要切割为 $filenum个文件"


        # 进行文件切割
        i=1        # 修改处2：38 修改为1
        while(( i<=$filenum ))
        do
                echo "切割文件名：$tb_name.tbl.$i"
                #每个小文件要截取行数在原文件范围min,max 
                p=`expr $i - 1`
                min=`expr $p \* $line + 1`
                max=`expr $i \* $line`
                sed -n "$min,$max"p $sql_path/$tb_name.tbl > $sql_path/$tb_name.tbl.$i
                #echo "本次操作没有进行切割"
                # 设置切割文件名
                filename=$sql_path/$tb_name.tbl.$i
                echo "$tb_name.tbl.$i 切割完成！文件名：$filename"
                #StoneDB导入方法，
                mysql -u$db_user -p$db_pwd -h$db_host -P$db_port --local-infile -D$db -e "load data local infile '$filename' into table $tb_name fields terminated by '|';" $2>1 > /dev/null
            i=`expr $i + 1`
        done
    done
}

split_file

```
## 6、生成TPC-H测试查询语句
复制qgen和dists.dss文件到queries 文件夹下
```shell
cp /data/tpc-h/dbgen/qgen /data/tpc-h/dbgen/queries
cp /data/tpc-h/dbgen/dists.dss /data/tpc-h/dbgen/qgen/queries
# 复制到/data/tpc-h/stonedb 统一路径下存放
cp -a /data/tpc-h/dbgen/qgen/queries /data/tpc-h/stonedb/queries
```
```bash
#!/usr/bin/bash
#执行chmod +x tpch_querys.sh
#./tpch_querys.sh stonedb
db_type=$1
for i in {1..22}
do
./qgen -d $i -s 100 > $db_type"$i".sql
done
```
```shell
#执行查询语句生成脚本
mkdir query
./tpch_querys.sh query
mv query*.sql /data/tpc-h/stonedb/queries 
```
## 7、修改查询语句，执行测试
查询测试脚本
```bash
#!/bin/bash

# stone
host=192.168.30.120
port=3306
user=root
password=********
database=ztpch


#查询语句绝对路径，需要修改成自己的路径
banchdir=/data/tpc-h/stonedb/queries
db_type=stonedb #ck、stone、mysql
resfile=$banchdir/"TPCH_${db_type}_`date "+%Y%m%d%H%M%S"`"


echo "start test run at"`date "+%Y-%m-%d %H:%M:%S"`|tee -a ${resfile}.out
echo "日志地址: ${resfile}"
for (( i=1; i<=22;i=i+1 ))
    do

    queryfile=${db_type}${i}".sql"

    echo "run query ${i}"|tee -a ${resfile}.out
    echo " $database  $banchdir/query$i.sql "  #|tee -a ${resfile}.out
    start_time=`date "+%s.%N"`
    #clickhouse
    #clickhouse-client --port $port --user $user --password $password --host $host -d $database < $banchdir/query$i.sql |tee -a ${resfile}.out
    #stonedb和mysql
    mysql -u$user -p$password -h$host -P$port $database -e "source $banchdir/query$i.sql" 2>&1 |tee -a ${resfile}.out

    end_time=`date "+%s.%N"`
    start_s=${start_time%.*}
    start_nanos=${start_time#*.}
    end_s=${end_time%.*}
    end_nanos=${end_time#*.}
    if [ "$end_nanos" -lt "$start_nanos" ];then
            end_s=$(( 10#$end_s -1 ))
            end_nanos=$(( 10#$end_nanos + 10 ** 9))
    fi
    time=$(( 10#$end_s - 10#$start_s )).`printf "%03d\n" $(( (10#$end_nanos - 10#$start_nanos)/10**6 ))`
    echo ${queryfile} "the $i run cost "${time}" second start at "`date -d @$start_time "+%Y-%m-%d %H:%M:%S"`" stop at "`date -d @$end_time "+%Y-%m-%d %H:%M:%S"` >> ${resfile}.time
    # systemctl stop clickhouse-server
    done

```
### 原始查询语句
[q2.sql](https://static.stoneatom.com/custom/sql/q2.sql) [q3.sql](https://static.stoneatom.com/custom/sql/q3.sql) [q4.sql](https://static.stoneatom.com/custom/sql/q4.sql) [q5.sql](https://static.stoneatom.com/custom/sql/q5.sql) [q6.sql](https://static.stoneatom.com/custom/sql/q6.sql) [q7.sql](https://static.stoneatom.com/custom/sql/q7.sql) [q8.sql](https://static.stoneatom.com/custom/sql/q8.sql) [q9.sql](https://static.stoneatom.com/custom/sql/q9.sql) [q10.sql](https://static.stoneatom.com/custom/sql/q10.sql) [q11.sql](https://static.stoneatom.com/custom/sql/q11.sql) [q12.sql](https://static.stoneatom.com/custom/sql/q12.sql) [q13.sql](https://static.stoneatom.com/custom/sql/q13.sql) [q14.sql](https://static.stoneatom.com/custom/sql/q14.sql) [q15.sql](https://static.stoneatom.com/custom/sql/q15.sql) [q16.sql](https://static.stoneatom.com/custom/sql/q16.sql) [q17.sql](https://static.stoneatom.com/custom/sql/q17.sql) [q18.sql](https://static.stoneatom.com/custom/sql/q18.sql) [q19.sql](https://static.stoneatom.com/custom/sql/q19.sql) [q20.sql](https://static.stoneatom.com/custom/sql/q20.sql) [q21.sql](https://static.stoneatom.com/custom/sql/q21.sql) [q22.sql](https://static.stoneatom.com/custom/sql/q22.sql) [q1.sql](https://static.stoneatom.com/custom/sql/q1.sql)
### 修改后查询语句
（为了保证测试可重复性，建议使用以下修改后兼容语法进行测试）
[q.zip](https://static.stoneatom.com/custom/sql/q.zip)
## 8、执行TPCH脚本，获得测试结果
out为执行结果文件，time为每条Query 执行时间
```shell
ll /data/tpc-h/stonedb/queries/stonedb
-rw-r--r-- 1 root root   15019 Jun  1 00:55 TPCH_stone_20220531233024.out
-rw-r--r-- 1 root root    2179 Jun  1 00:57 TPCH_stone_20220531233024.time
```

