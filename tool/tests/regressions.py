import stonedbtester
import unittest
import MySQLdb
import os, shutil
import time

class RegressionTestCases(stonedbtester.HistoreTester):
    def setUp(self):
        super(RegressionTestCases, self).setUp()
        self.db.autocommit(True)
        self.rdir = "/tmp/regressions"
        os.mkdir(self.rdir)
    def tearDown(self):
        shutil.rmtree(self.rdir)
        super(RegressionTestCases, self).tearDown()
    def write_data_file(self, data, filename):
        with open(self.rdir + "/" + filename, "w") as pfile:
            for d in data:
                pfile.write(";".join([str(x) for x in d]) + "\n")
            pfile.flush()
    def test_issue_324(self):
        tablename = "issue324"
        columns = [["INT", [], ""], ["INT", [], ""]]
        if self.is_table_existing(tablename):
            self.drop_table(tablename)
        self.create_table(tablename, columns)
        data = []
        for i in range(1, 80001):
            data.append([i, 9527])
        self.load_data(tablename, data)
#        self.write_data_file(data, tablename)
#        self.load_data_file(tablename, tablename)
        sql = "SELECT * from %s where col1=60000" % (tablename)
        self.execute(sql)
        self.assertTrue(len(self.cursor.fetchall()) == 1)
        self.drop_table(tablename)
    def test_issue_dayou(self):
        tablename = "issue_dayou"
        columns = [["INT", [], '']]
        if self.is_table_existing(tablename):
            self.drop_table(tablename)
        self.create_table(tablename, columns)
        data = [[9527]] * 70000
        data[35000] = [382736]
        data[35001] = [9238721]
        self.load_data(tablename, data)
        sql = "UPDATE " + tablename + " SET col1=NULL WHERE col1=382736"
        self.execute(sql)
        sql = "UPDATE " + tablename + " SET col1=9527 WHERE col1=9238721"
        self.execute(sql)
        self.execute("SELECT * FROM " + tablename + " WHERE col1>=382736")
        self.assertTrue(len(self.cursor.fetchall()) == 0)
        time.sleep(1)
        self.execute("SELECT COUNT(*) FROM " + tablename + " WHERE col1=9527")
        self.assertEqual(self.cursor.fetchone()[0], 69999)
        self.drop_table(tablename)
