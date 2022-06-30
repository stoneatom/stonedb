import stonedbtester
import unittest
import MySQLdb
import os, shutil
import time

class Issue349Case(stonedbtester.StoneDBTester):
    def setUp(self):
        super(Issue349Case, self).setUp()
        self.db.autocommit(True)
        self.tablename = "issue349"
        columns = [["INT", [], ''], ["INT", [], ''], ["VARCHAR", [10], '']]
        if self.is_table_existing(self.tablename):
            self.drop_table(self.tablename)
        self.create_table(self.tablename, columns)
    def tearDown(self):
        self.drop_table(self.tablename)
        super(Issue349Case, self).tearDown()
    def test_issue_349(self):
        data = [(1, 2, 'dd')]
        self.load_data(self.tablename, data)
        self.execute("INSERT INTO issue349(col1, col2) VALUES(1, 2)")
        time.sleep(3)
        self.execute("UPDATE issue349  SET col3='dd' WHERE col3 IS NULL")
        time.sleep(3)
        self.execute("UPDATE issue349  SET col3=NULL")
        time.sleep(3)
        self.execute("UPDATE issue349  SET col3='dd' WHERE col3 IS NULL")
        time.sleep(3)
        self.execute("SELECT * FROM issue349")
        res = self.cursor.fetchone()
        self.assertEqual('dd', res[2])
        res = self.cursor.fetchone()
        self.assertEqual('dd', res[2])
