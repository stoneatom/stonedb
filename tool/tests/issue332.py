import stonedbtester
import unittest
import MySQLdb
import os, shutil
import time

class Issue332Case(stonedbtester.HistoreTester):
    def setUp(self):
        super(Issue332Case, self).setUp()
        self.db.autocommit(True)
        self.tablename = "issue332"
        columns = [["DATETIME", [], '']]
        if self.is_table_existing(self.tablename):
            self.drop_table(self.tablename)
        self.create_table(self.tablename, columns)
    def tearDown(self):
        self.drop_table(self.tablename)
        super(Issue332Case, self).tearDown()
    def test_issue_332(self):
        data = [("2017-09-18 00:00:00",), ("2017-09-19 03:00:00",)]
        self.load_data(self.tablename, data)
        time.sleep(3)
        self.execute("SELECT SUM(col1 > '2017-09-19 00:00:00') FROM " + self.tablename)
        res = self.cursor.fetchone()
        self.assertEqual(1, int(res[0]))
