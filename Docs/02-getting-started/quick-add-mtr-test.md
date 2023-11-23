---
id: quick-add-mtr-test
sidebar_position: 3.6
---

# Qucik Add MTR Test

## Add and Run a MTR test

When you contribute your code into StoneDB, test your code in the meantime.

Test cases are in the path `\${StoneDB-Source-DIR}/mysql-test/suite/tianmu/`.

:::info
`${StoneDB-Source-DIR}` is your StoneDB source code directory.
 
`${TEST-PATH}=${StoneDB-Source-DIR}/mysql-test/suite/tianmu/` in the following context.
:::

### Step 1. Write your test file and result file

Put your test file in the `${TEST-PATH}/t/` and result file in the `${TEST-PATH}/r/`.

You can write your test cases like this.

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

### Step 2. Compile your StoneDB.

In this example, I compile my StoneDB in `${StoneDB-Source-DIR}/build/mysql8/` and install in `${StoneDB-Source-DIR}/build/install8/`

 ```shell
cd stonedb/build
mkdir mysql8 install8
# build your stonedb
.........
 ```

### Step 3. Run your test.

After installation, test cases will be moved into your installation directory.In this example, the path is `${StoneDB-Source-DIR}/build/install8/mysql-test`

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