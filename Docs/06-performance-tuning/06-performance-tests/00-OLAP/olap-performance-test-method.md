---
id: olap-performance-test-method
sidebar_position: 7.611
---

# OLAP Performance Test Method

## **TPC-H introduction**
The TPC Benchmark-H (TPC-H) is a decision support benchmark. It consists of a suite of business-oriented ad-hoc queries and concurrent data modifications. The queries and the data populating the database have been chosen to have broad industry-wide relevance.<br />In the TPC-H benchmark, 22 complex SQL queries are performed on 8 tables. Most queries contain joins on several tables, subqueries, and GROUP BY clauses. For more information, visit [https://www.tpc.org/tpch/](https://www.tpc.org/tpch/).
## **Test environment introduction**

- OS: CentOS 7.9
- CPU: Intel(R) Xeon(R) CPU E5-2683 v4 @ 2.10GHz, 16 cores, and 64 threads
- Memory: 125 GB
- Deployment mode of StoneDB: standalone
```shell
bin/mysqld  Ver 5.6.24-StoneDB for Linux on x86_64 (build-)
build information as follow:
        Repository address: https://github.com/stoneatom/stonedb.git:stonedb-5.6
        Branch name: stonedb-5.6
        Last commit ID: 90583b2
        Last commit time: Date:   Wed Jul 6 23:31:30 2022 +0800
        Build time: Date: Thu Jul  7 05:39:39 UTC 2022
```
## **Test scheme**
### **1. Set up the test environment** 
For information about how to set up the test environment,  see [Quick Deployment](..../../../../../02-getting-started/quick-deployment).
### 2. Compile and deploy TPC-H

1. Download the [TPC-H](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) installation package, upload the package to the test machine. 

For example, upload it to the** /data** folder.
```shell
unzip tpc-h-tool.zip
mv TPC-H_Tools_v3.0.0/ tpc-h/
cd /data/tpc-h/dbgen/
# Install GCC and MAKE.
yum install gcc make -y
```

2. Modify file **makefile** as shown in the following code. 
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
This modification is mandatory, because TPC-H originally does not support MySQL.

3. Modify file **tpcd.h** to add the database type MySQL to TPC-H.
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

# Run the "make" command.
make
```
### **3. Use TPC-H to generate 100 GB test data**
For example, run the following command to use TPC-H to generate **.tbl** data files.
```shell
./dbgen -s 100

# Copy the data files to the "stonedb" folder.
mkdir /data/tpc-h/stonedb/
mv *.tbl /data/tpc-h/stonedb/
```
**-s** _n_ in the command indicates the size of data generated. Unit: GB.<br />After the test files are generated, you can run the **head** command to check whether each row in the **.tbl** data files has some fields that are separated with vertical bars (|).
:::info
If this is not the first time you use TPC-H to generate data in the environment, we recommend that you run **make clean** and then **make** to clear data first and then run the command with **-f** specified to overwrite the data previously generated.
:::
### **4. Modify the dss.ddl and dss.ri commands**
The `dss.ddl` command is used to create tables. The `dss.ri` command is used to create indexes and foreign key indexes.

Because the syntax to create table schemas varies with the storage engine. Therefore, the statements for creating table schemas and indexes must be modified. The following code provides an example.

To modify the statements, copy the file that stores schemas and indexes to the **stonedb** folder.
```shell
[root@htap2 dbgen]# cp dss.ddl /data/tpc-h/stonedb/dss_stonedb.ddl			# Schemas used for StoneDB tables
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
### **5. Import table schemas and data**

1. Import table schemas.
```sql
mysql> create database tpch;
mysql> source /data/tpc-h/stonedb/dss_stonedb.ddl  # Modify the value of the PATH parameter to be the path to the TPC-H tool.
```

2. Import data.

You can directly import tables **part**, **region**, **nation**, **customer**, and **supplier**. For tables **lineitem**, **orders**, and **partsupp**, we recommend that you use a script such as split_file2db.sh to split them before the import.
```shell
# Import data to StoneDB.
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
# Obtain the .tbl files and the corresponding table names.
sql_path=/data/tpc-h/stonedb/
# split_tb=$(ls ${sql_path}/*.ddl)
# Files to split.
split_tb=(lineitem orders partsupp)
# split_tb=(customer nation supplier part region)
# split_tb=(part nation)
# Split settings.
# The interval (number of rows) for splitting.
line=1000000  

# Database configuration.
db_host=192.168.30.102
db_port=3306
db_user=ztpch
db_pwd=******
db=ztpch  




# Split a large SQL file.
function split_file()
{
    for tb_name in ${split_tb[@]}
    do
        echo "$tb_name"
        # Obtain the number of the file before it is split.
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
        echo "File $tb_name has $totalline rows of data and needs to be split into $filenum files."


        # Split the file.
        i=1        # Change 38 to 1.
        while(( i<=$filenum ))
        do
                echo "File to split: $tb_name.tbl.$i"
                # The interval for splitting must falls with [min, max] of the original file. 
                p=`expr $i - 1`
                min=`expr $p \* $line + 1`
                max=`expr $i \* $line`
                sed -n "$min,$max"p $sql_path/$tb_name.tbl > $sql_path/$tb_name.tbl.$i
                #echo "This operation does not split the file."
                # Specify the name of the file to split.
                filename=$sql_path/$tb_name.tbl.$i
                echo "$tb_name.tbl.$i is split. File name: $filename"
                # Import data to StoneDB.
                mysql -u$db_user -p$db_pwd -h$db_host -P$db_port --local-infile -D$db -e "load data local infile '$filename' into table $tb_name fields terminated by '|';" $2>1 > /dev/null
            i=`expr $i + 1`
        done
    done
}

split_file

```
### **6. Generate test SQL statements**
Copy the **qgen** and **dists.dss** files to the **queries** directory.
```shell
cp /data/tpc-h/dbgen/qgen /data/tpc-h/dbgen/queries
cp /data/tpc-h/dbgen/dists.dss /data/tpc-h/dbgen/qgen/queries
# Copy the files to path "data/tpc-h/stonedb".
cp -a /data/tpc-h/dbgen/qgen/queries /data/tpc-h/stonedb/queries
```
```bash
#!/usr/bin/bash
# Run "chmod +x tpch_querys.sh".
#./tpch_querys.sh stonedb
db_type=$1
for i in {1..22}
do
./qgen -d $i -s 100 > $db_type"$i".sql
done
```
```shell
# Execute the script to generate statements.
mkdir query
./tpch_querys.sh query
mv query*.sql /data/tpc-h/stonedb/queries 
```
### **7. Modify the SQL statements and start the test**
Test script:
```bash
#!/bin/bash

# stone
host=192.168.30.120
port=3306
user=root
password=********
database=ztpch


# The absolate path. The following is for reference only.
banchdir=/data/tpc-h/stonedb/queries
db_type=stonedb #ck、stone、mysql
resfile=$banchdir/"TPCH_${db_type}_`date "+%Y%m%d%H%M%S"`"


echo "start test run at"`date "+%Y-%m-%d %H:%M:%S"`|tee -a ${resfile}.out
echo "Path to log: ${resfile}"
for (( i=1; i<=22;i=i+1 ))
    do

    queryfile=${db_type}${i}".sql"

    echo "run query ${i}"|tee -a ${resfile}.out
    echo " $database  $banchdir/query$i.sql "  #|tee -a ${resfile}.out
    start_time=`date "+%s.%N"`
    #clickhouse
    #clickhouse-client --port $port --user $user --password $password --host $host -d $database < $banchdir/query$i.sql |tee -a ${resfile}.out
    #stonedb and mysql
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
#### **Statements before the modification**
[q1.sql](https://static.stoneatom.com/custom/sql/q1.sql)

[q2.sql](https://static.stoneatom.com/custom/sql/q2.sql) 

[q3.sql](https://static.stoneatom.com/custom/sql/q3.sql) 

[q4.sql](https://static.stoneatom.com/custom/sql/q4.sql) 

[q5.sql](https://static.stoneatom.com/custom/sql/q5.sql) 

[q6.sql](https://static.stoneatom.com/custom/sql/q6.sql)

[q7.sql](https://static.stoneatom.com/custom/sql/q7.sql)

[q8.sql](https://static.stoneatom.com/custom/sql/q8.sql)

[q9.sql](https://static.stoneatom.com/custom/sql/q9.sql)

[q10.sql](https://static.stoneatom.com/custom/sql/q10.sql)

[q11.sql](https://static.stoneatom.com/custom/sql/q11.sql)

[q12.sql](https://static.stoneatom.com/custom/sql/q12.sql)

[q13.sql](https://static.stoneatom.com/custom/sql/q13.sql)

[q14.sql](https://static.stoneatom.com/custom/sql/q14.sql)

[q15.sql](https://static.stoneatom.com/custom/sql/q15.sql)

[q16.sql](https://static.stoneatom.com/custom/sql/q16.sql)

[q17.sql](https://static.stoneatom.com/custom/sql/q17.sql) 

[q18.sql](https://static.stoneatom.com/custom/sql/q18.sql)

[q19.sql](https://static.stoneatom.com/custom/sql/q19.sql)

[q20.sql](https://static.stoneatom.com/custom/sql/q20.sql)

[q21.sql](https://static.stoneatom.com/custom/sql/q21.sql)

[q22.sql](https://static.stoneatom.com/custom/sql/q22.sql) 
#### **Statements after the modification**
To ensure the repeatability of this test, we recommend that you use the statements after the modification.

[q.zip](https://static.stoneatom.com/custom/sql/q.zip)
### **8. Execute the TPC-H script to obtain the test result**
The **.out** file stores the test results. The **.time** file records the execution time of each query.
```shell
ll /data/tpc-h/stonedb/queries/stonedb
-rw-r--r-- 1 root root   15019 Jun  1 00:55 TPCH_stone_20220531233024.out
-rw-r--r-- 1 root root    2179 Jun  1 00:57 TPCH_stone_20220531233024.time
```