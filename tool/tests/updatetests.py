import stonedbtester
import unittest
import MySQLdb
import os, shutil
import time

class UpdateTestCases(stonedbtester.HistoreTester):
    def setUp(self):
        super(UpdateTestCases, self).setUp()
        self.db.autocommit(True)
        self.tablename = "updatetests"
        columns = [["INT", [], ''], ["INT", [], '']]
        if self.is_table_existing(self.tablename):
            self.drop_table(self.tablename)
        self.create_table(self.tablename, columns)
    def tearDown(self):
        self.drop_table(self.tablename)
        super(UpdateTestCases, self).tearDown()
    def test_update_from_uniform(self):
        data=[]
        for i in range(1, 70001):
            data.append([i, 9527])
        self.load_data(self.tablename, data)
        sql = "UPDATE " + self.tablename + " SET col2=95270 WHERE col1=3500"
        time.sleep(1)
        self.execute(sql)
        self.execute("SELECT count(*) FROM " + self.tablename + " WHERE col2=95270")
        self.assertEqual((self.cursor.fetchone()[0]), 1)
        self.execute("SELECT count(*) FROM " + self.tablename + " WHERE col2=9527")
        self.assertEqual((self.cursor.fetchone()[0]), 69999)
    def test_update_to_uniform(self):
        data=[]
        for i in range(1, 70001):
            if i == 3500:
                data.append([i, 123456])
            else:
                data.append([i, 9527])
        self.load_data(self.tablename, data)
        time.sleep(2)
        sql = "UPDATE " + self.tablename + " SET col2=9527 WHERE col2=123456"
        self.execute(sql)
        self.execute("SELECT count(*) FROM " + self.tablename + " WHERE col2=123456")
        self.assertEqual((self.cursor.fetchone()[0]), 0)
        self.execute("SELECT count(*) FROM " + self.tablename + " WHERE col2=9527")
        self.assertEqual((self.cursor.fetchone()[0]), 70000)
    def test_check_all_inserted(self):
        data=[]
        for i in range(1, 70001):
            data.append([i, hash(i)])
        self.load_data(self.tablename, data)
        for d in data:
            self.execute("SELECT col2 FROM " + self.tablename + " WHERE col1=%s", d[0])
            self.assertEqual(self.cursor.fetchone()[0], d[1])
        self.execute("UPDATE " + self.tablename + " SET col2=col2+123")
        for d in data:
            self.execute("SELECT col2 FROM " + self.tablename + " WHERE col1=%s", d[0])
            self.assertEqual(self.cursor.fetchone()[0], d[1]+123)
    def test_check_data_loaded_from_file(self):
        data=[]
        for i in range(1, 70001):
            data.append([i, int((i*37 + i*23)/31)])
        with open("/tmp/updatedatasource.txt", "w") as pfile:
            for d in data:
                pfile.write(";".join([str(x) for x in d]) + "\n")
            pfile.flush()
        self.load_data_file(self.tablename, "/tmp/updatedatasource.txt")
        for d in data:
            self.execute("SELECT col2 FROM " + self.tablename + " WHERE col1=%s", d[0])
            self.assertEqual(self.cursor.fetchone()[0], d[1])
        os.unlink("/tmp/updatedatasource.txt")
    def test_update_all_rows_to_uniform(self):
        data=[]
        for i in range(70001, 1, -1):
            data.append([i, int((i*37 + i*23)/31)])
        with open("/tmp/updatedatasource.txt", "w") as pfile:
            for d in data:
                pfile.write(";".join([str(x) for x in d]) + "\n")
            pfile.flush()
        self.load_data_file(self.tablename, "/tmp/updatedatasource.txt")
        self.execute("UPDATE " + self.tablename + " SET col2=9383762")
        self.execute("SELECT COUNT(*) FROM %s WHERE col2=9383762" % self.tablename)
        self.assertEqual(self.cursor.fetchone()[0], 70000)
        os.unlink("/tmp/updatedatasource.txt")
    def test_check_data_loaded_from_file_after_restart(self):
        data=[]
        for i in range(1, 70001):
            data.append([i, int((i*37 + i*23)/31)])
        with open("/tmp/updatedatasource.txt", "w") as pfile:
            for d in data:
                pfile.write(";".join([str(x) for x in d]) + "\n")
            pfile.flush()
        self.load_data_file(self.tablename, "/tmp/updatedatasource.txt")
        self.restart_mysqld()
        for d in data:
            self.execute("SELECT col2 FROM " + self.tablename + " WHERE col1=%s", d[0])
            self.assertEqual(self.cursor.fetchone()[0], d[1])
        os.unlink("/tmp/updatedatasource.txt")
