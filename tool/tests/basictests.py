import stonedbtester
import unittest
import MySQLdb

NumericTypes = [
    ("BOOL", [], []),
#    ("BIT", [(1, 64)], ["UNSIGNED", "ZEROFILL"]),
    ("TINYINT", [(1, 255)], ["UNSIGNED", "ZEROFILL"]),
    ("SMALLINT", [(1, 255)], ["UNSIGNED", "ZEROFILL"]),
    ("MEDIUMINT", [(1, 255)], ["UNSIGNED", "ZEROFILL"]),
    ("INT", [(1, 255)], ["UNSIGNED", "ZEROFILL"]),
    ("INTEGER", [(1, 255)], ["UNSIGNED", "ZEROFILL"]),
    ("BIGINT", [(1, 255)], ["UNSIGNED", "ZEROFILL"]),
#    ("DECIMAL", [(1, 65), (0, 30)], ["UNSIGNED", "ZEROFILL"]),
    ("DECIMAL", [(1, 18), (0, 30)], ["UNSIGNED", "ZEROFILL"]),
    ("FLOAT", [(1, 8), (0, 5)], ["UNSIGNED", "ZEROFILL"]),
    ("DOUBLE", [(1, 16), (0, 10)], ["UNSIGNED", "ZEROFILL"]),
]
DateTimeTypes = [
    ("DATE", [], []),
    ("DATETIME", [(0, 6)], []),
    ("TIMESTAMP", [(0, 6)], []),
    ("TIME", [(0, 6)], []),
    ("YEAR", [(4, 4)], []),
]
StringTypes = [
    ("CHAR", [(0, 10)], []),
    ("CHAR", [(100, 110)], []),
    ("CHAR", [(250, 255)], []),
    ("VARCHAR", [(0, 5)], []),
    ("VARCHAR", [(30000, 30005)], []),
    ("VARCHAR", [(65530, 65535)], []),
    ("BINARY", [(0, 10)], []),
    ("BINARY", [(100, 110)], []),
    ("BINARY", [(250, 255)], []),
    ("VARBINARY", [(25000, 25005)], []),
    ("TINYBLOB", [], []),
    ("TINYTEXT", [], []),
    ("BLOB", [(65533, 65535)], []),
    ("TEXT", [(65533, 65535)], []),
    ("MEDIUMBLOB", [], []),
#    ("MIDIUMTEXT", [], []),
    ("LONGBLOB", [], []),
    ("LONGTEXT", [], [])
]


class BasicTestCases(stonedbtester.HistoreTester):
    def setUp(self):
        super(BasicTestCases, self).setUp()
    def tearDown(self):
        super(BasicTestCases, self).tearDown()
    def create_table_then_drop(self, tablename, columns):
        self.create_table(tablename, columns)
        self.assertTrue(self.is_table_existing(tablename))
        self.drop_table(tablename)
    def test_create_tester_table(self):
        tablename = "stonedb_tester"
        if self.is_table_existing(tablename):
            self.drop_table(tablename)
        self.create_table_then_drop(tablename,
                                    [["INT", [], ""], ["INT", [], ""]])
    def create_all_single_column_table(self, tablename, coldefs):
        if self.is_table_existing(tablename):
            self.drop_table(tablename)
        for col in coldefs:
            if len(col[1]) == 0:
                self.create_table_then_drop(tablename,
                                            [[col[0], [], ""]])
            elif len(col[1]) == 1:
                for mi, ma in col[1]:
                    for i in range(mi, ma+1):
                        self.create_table_then_drop(tablename,
                                                    [[col[0], [i], ""]])
            elif len(col[1]) == 2:
                for i in range(col[1][0][0], col[1][0][1] + 1):
                    for j in range(col[1][1][0], i + 1):
                        self.create_table_then_drop(tablename,
                                                    [[col[0], [i,j], ""]])
            else:
                print col[0], col[1]
                self.assertTrue(False, "Too many params for " + col[0])
    def test_create_numeric_types(self):
        tablename = "singlecolumn"
        self.create_all_single_column_table(tablename, NumericTypes)
    def test_create_datetime_types(self):
        tablename = "singlecolumn"
        self.create_all_single_column_table(tablename, DateTimeTypes)
    def test_create_string_types(self):
        tablename = "singlecolumn"
        self.create_all_single_column_table(tablename, StringTypes)
    # @unittest.skip("Unsupported column type: ENUM")
    # def test_create_table_with_enum(self):
    #     tablename = "singlecolumn"
    #     values = []
    #     for i in range(100):
    #         values.append("'value%d'" % i)
    #     enumtype = "ENUM(" + ", ".join(values) + ")"
    #     self.create_table_then_drop(tablename, [[enumtype, [], ""]])
    # @unittest.skip("Unsupported column type: SET")
    # def test_create_table_with_set(self):
    #     tablename = "singlecolumn"
    #     values = []
    #     for i in range(10):
    #         values.append("'value%d'" % i)
    #     enumtype = "SET(" + ", ".join(values) + ")"
    #     self.create_table_then_drop(tablename, [[enumtype, [], ""]])
    def create_table_with_many_columns(self, column_count):
        column = ["INT", [], ""]
        columns = [column] * column_count
        tablename = "fourkcolumns"
        if self.is_table_existing(tablename):
            self.drop_table(tablename)
        self.create_table_then_drop(tablename, columns)
    def test_table_with_2410_columns(self):
        self.create_table_with_many_columns(2410)
    def test_table_with_2411_columns(self): # throws
        self.assertRaises(MySQLdb.OperationalError,
                          self.create_table_with_many_columns,
                          2411)
