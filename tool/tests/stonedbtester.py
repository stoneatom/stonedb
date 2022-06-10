import unittest
import MySQLdb
import subprocess
import time

Config = {
    'unix_socket':'/tmp/stonedb.sock',
    'db':'test',
    'user':'root',
    'passwd':''
}

def generate_create_table_statement(tablename, columns):
    res = "CREATE TABLE " + tablename + " (\n\t"
    cols = []
    colidx = 1
    for col in columns:
        params = ''
        if len(col[1]) > 0:
            params = "(" + ", ".join([str(x) for x in col[1]]) + ")"
        options = ''
        cols.append("col%d %s %s %s" % (colidx, col[0], params, options))
        colidx += 1
    res += ",\n\t".join(cols) + "\n);"
    return res

class HistoreTester(unittest.TestCase):
    def setUp(self):
        self.db = MySQLdb.connect(**Config)
        self.cursor = self.db.cursor()
        self.pid = self.get_mysqld_pid()
    def tearDown(self):
        self.assertTrue(self.pid == self.get_mysqld_pid(),
                        "ERROR! mysqld restarted unexpectedly!")
        self.cursor.close()
        self.db.close()
    def execute(self, sql, *args, **kwargs):
#        print sql
        self.cursor.execute(sql, *args, **kwargs)
    def create_table(self, tablename, columns):
        sql = generate_create_table_statement(tablename, columns)
        self.execute(sql);
    def is_table_existing(self, tablename):
        self.execute("""SHOW TABLES;""")
        for res in self.cursor.fetchall():
            if tablename.lower() == res[0].lower():
                return True;
        return False
    def drop_table(self, tablename):
        sql = "DROP TABLE %s;" % (tablename)
        self.execute(sql)
    def get_mysqld_pid(self):
        return subprocess.Popen(['pidof', 'mysqld'], stdout=subprocess.PIPE).communicate()[0]
    def restart_mysqld(self):
        self.assertTrue(self.pid == self.get_mysqld_pid(),
                        "ERROR! mysqld restarted unexpectedly!")
        self.cursor.close()
        self.db.close()
        subprocess.call(["sudo", "service", "stonedb", "restart"])
        time.sleep(3)
        for i in range(20):
            try:
                self.db = MySQLdb.connect(**Config)
                self.cursor = self.db.cursor()
                self.pid = self.get_mysqld_pid()
                return
            except MySQLdb.OperationalError:
                time.sleep(3)
        self.fail("stonedb service did not start in 60 secs")
    def load_data(self, tablename, data):
        placeholder = ["%s"] * len(data[0])
        sql = "INSERT INTO " + tablename + " VALUES (" + ",".join(placeholder) + ")"
        for d in data:
            self.execute(sql, d)
        time.sleep(2)
    def load_data_file(self, tablename, pathname):
        sql = 'LOAD DATA INFILE "%s" INTO TABLE %s;' % (pathname,
                                                        tablename)
        self.execute(sql)
