---
id: quick-add-mtr-test
sidebar_position: 3.6
---

# 快速添加 MTR Test

## 添加并运行一个mtr测试用例

当你在StoneDB贡献代码的同时，你需要测试你贡献的代码

测试用例在路径 \${StoneDB-Source-DIR}/mysql-test/suite/tianmu/中.

> \${StoneDB-Source-DIR} 是你的StoneDB源码目录.
> 
> 下文中我们将用\${TEST-PATH}来指代路径\${StoneDB-Source-DIR}/mysql-test/suite/tianmu/.

### 第一步. 编写你的测试用例和应该输出的结果的文件

将你的测试文件放在 \${TEST-PATH}/t/ 中，并将你的结果文件放在 \${TEST-PATH}/r/ 中.

你可以像下面的例子中一样编写你的测试用例和结果.

issue736.test
```sql
--source include/have_tianmu.inc
use test;
CREATE TABLE t1(col1 INT, col2 CHAR(5))ENGINE=tianmu;
INSERT INTO t1 VALUES(NULL,''),(1,'a'),(1,'b'),(1,'c'),(2,'dd'),(3,'eee');
INSERT INTO t1 VALUES(8,UNHEX('CEB2'));
SELECT COUNT(DISTINCT col2) FROM t1;
SELECT COUNT(DISTINCT col1) FROM t1;
DROP TABLE t1;
```

Issue736.result
```sql
use test;
CREATE TABLE t1(col1 INT, col2 CHAR(5))ENGINE=tianmu;
INSERT INTO t1 VALUES(NULL,''),(1,'a'),(1,'b'),(1,'c'),(2,'dd'),(3,'eee');
INSERT INTO t1 VALUES(8,UNHEX('CEB2'));
SELECT COUNT(DISTINCT col2) FROM t1;
COUNT(DISTINCT col2)
7
SELECT COUNT(DISTINCT col1) FROM t1;
COUNT(DISTINCT col1)
4
DROP TABLE t1;

```

### 第二步. 编译安装StoneDB.

在这个例子中, 我将StoneDB编译在 \${StoneDB-Source-DIR}/build/mysql8/ 中，并且将其安装在 \${StoneDB-Source-DIR}/build/install8/ 中

 ```shell
cd stonedb/build
mkdir mysql8 install8
# build your stonedb
.........
 ```

### 第三步. 运行测试用例.

在安装完成后, 测试用例会被移动至安装目录中. 在这个例子中, 路径为 \${StoneDB-Source-DIR}/build/install8/mysql-test

``` shell
cd install8/mysql-test
# run your test case. issue736 is my testcase.
# run one case.
./mtr --suite=tianmu testcase
# run all cases.
./mysql-test-run.pl --suite=tianmu --nowarnings --force --nocheck-testcases --parallel=10

### Execute the test in this example.
root@htap-dev-64-2:/stonedb/build/install8/mysql-test# ./mtr --suite=tianmu issue736
Logging: ./mtr  --suite=tianmu issue736
MySQL Version 8.0.30
Checking supported features
 - Binaries are debug compiled
Collecting tests
Checking leftover processes
Removing old var directory
Creating var directory '/stonedb/build/install8/mysql-test/var'
Installing system database
Using parallel: 1

==============================================================================
                  TEST NAME                       RESULT  TIME (ms) COMMENT
------------------------------------------------------------------------------
worker[1] mysql-test-run: WARNING: running this script as _root_ will cause some tests to be skipped
[ 50%] tianmu.issue736                           [ pass ]     78
[100%] shutdown_report                           [ pass ]
------------------------------------------------------------------------------
The servers were restarted 0 times
The servers were reinitialized 0 times
Spent 0.078 of 35 seconds executing testcases

Completed: All 2 tests were successful.

```