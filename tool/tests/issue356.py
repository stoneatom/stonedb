import stonedbtester
import unittest
import MySQLdb
import os, shutil
import time

class Issue356Case(stonedbtester.HistoreTester):
    def setUp(self):
        super(Issue356Case, self).setUp()
        self.db.autocommit(True)
        if (self.is_table_existing('tbl_test_5')):
            self.drop_table('tbl_test_5')
        self.execute("CREATE TABLE `tbl_test_5` ( `UAEWA_ts` INT(11)) ENGINE=STONEDB;")
    def tearDown(self):
        self.drop_table('tbl_test_5')
        super(Issue356Case, self).tearDown()
    def test_issue_356(self):
        sql = """
INSERT INTO `tbl_test_5` VALUES (1506355200);
select sleep(2);
update tbl_test_5 set UAEWA_ts = -1 where UAEWA_ts = 1506355200;
INSERT INTO `tbl_test_5` VALUES (1506355201);
INSERT INTO `tbl_test_5` VALUES (1506268800);
select sleep(2);
update tbl_test_5 set UAEWA_ts = -1 where UAEWA_ts = 1506268800;
select * from tbl_test_5;
"""
        for cmd in sql.split('\n'):
            if len(cmd) == 0:
                continue
            self.execute(cmd)
        res = self.cursor.fetchall()
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0][0], -1)
        self.assertEqual(res[1][0], 1506355201)
        self.assertEqual(res[2][0], -1)
