#!/usr/bin/env python

import sys
import os
import struct
import string
import argparse
import ConfigParser

if sys.version_info < (2, 7):
    sys.exit("Please use python 2.7 or greater")

TABLE_FILE        = 'Table.ctb'
TABLE_FILE_IDENT  = 'RScT'
CLMD_FORMAT_RSC10 = "RS10"
DPN_PACK_SIZE     = 37
PF_NULLS_ONLY     = -1
PF_NO_OBJ         = -2
PF_NOT_KNOWN      = -3
PF_END            = -4
PF_DELETED        = -5

NULL_VALUE_64     = 0x8000000000000001

RC_STRING         = 0
RC_VARCHAR        = 1
RC_INT            = 2
RC_NUM            = 3
RC_DATE           = 4
RC_TIME           = 5
RC_BYTEINT        = 6
RC_SMALLINT       = 7
RC_BIN            = 8
RC_BYTE           = 9
RC_VARBYTE        = 10
RC_REAL           = 11
RC_DATETIME       = 12
RC_TIMESTAMP      = 13
RC_DATETIME_N     = 14
RC_TIMESTAMP_N    = 15
RC_TIME_N         = 16
RC_FLOAT          = 17
RC_YEAR           = 18
RC_MEDIUMINT      = 19
RC_BIGINT         = 20
RC_UNKNOWN        = 255

RC_TYPES = [
    'STRING',
    'VARCHAR',
    'INT',
    'NUM',
    'DATE',
    'TIME',
    'BYTEINT',
    'SMALLINT',
    'BIN',
    'BYTE',
    'VARBYTE',
    'REAL',
    'DATETIME',
    'TIMESTAMP',
    'DATETIME_N',
    'TIMESTAMP_N',
    'TIME_N',
    'FLOAT',
    'YEAR',
    'MEDIUMINT',
    'BIGINT',
    'UNKNOWN' ]


class OPT:
   cnf_file = '/etc/my.cnf'
   dump_table_info = False
   dump_column_info = False
   dump_dp_info = False

class AttrType:
    def __init__(self, no):
        self.type = no

    def name(self):
        return RC_TYPES[self.type]

    def is_numeric(self):
        return self.type in [
                        RC_BIGINT,
                        RC_BYTEINT,
                        RC_DATE,
                        RC_DATETIME,
                        RC_DATETIME_N,
                        RC_FLOAT,
                        RC_INT,
                        RC_MEDIUMINT,
                        RC_NUM,
                        RC_REAL,
                        RC_SMALLINT,
                        RC_TIME,
                        RC_TIMESTAMP,
                        RC_TIMESTAMP_N,
                        RC_TIME_N,
                        RC_YEAR]


class DP:
    def __init__(self, attr, bytes, no):
        self.owner = attr
        self.has_file = True
        self.no = no
        '''
  <pack_file>     4     - file number for actual data (i.e. an identifier of large data file)
                              Special values for no storage needed:
                                        PF_NULLS_ONLY (-1)  - only nulls,
                                        PF_NO_OBJ (-2) - no objects (empty pack)
  <pack_addrs>    4     - file offset for actual data in the large data file
  <local_min>     8     - min of data pack values:
                                      for int/decimal/datetime - as int_64,
                                      for double - as double,
                                      for string - as the first 8 bytes of minimal string
  <local_max>     8     - max, as above
  <local_sum/size>  8   - sum of values of data pack (NULL_VALUE_64 means "not known or overflow")
                                 as int64 for fixed precision columns, as double for floating point.
                                 In case of strings, this value stores the longest string size of data pack.

  <loc_no_obj>    2   - number of objects-1 (i.e. 0 means 1 object etc.), 0 objects are stored as pak_file = PF_NO_OBJ
  <loc_no_nulls>  2   - number of nulls,
                         0 means either no nulls, or all nulls (but in this case pak_file = PF_NULLS_ONLY)

  <special_flags> 1   - default 0, but the following bits may be set:
                                 SEQUENTIAL_VALUES - the pack is derivable as x_n = (local_min + n), no data needed
        '''
        (self.file_no, self.file_off, self.min,
         self.max, self.sum, self.no_obj,
         self.no_nulls, self.flags) = struct.unpack('=iiQQQHHb', bytes)

        if self.file_no in [ PF_NULLS_ONLY, PF_NO_OBJ, PF_NOT_KNOWN, PF_END, PF_DELETED]:
            self.has_file = False
        else:
            self.filename = 'TA{:05d}{:09d}.ctb'.format(self.owner.attr_no, self.file_no)

    def file_no_special(self):
        return self.file_no in [ PF_NULLS_ONLY, PF_NO_OBJ, PF_NOT_KNOWN, PF_END, PF_DELETED]

    def show(self):
        if not OPT.dump_dp_info:
            return
        print '----------- Data Pack {:03} ---------------'.format(self.no)
        if self.file_no == PF_NULLS_ONLY:
            print 'Only NULL'
        elif self.file_no == PF_NO_OBJ:
            print 'Empty'
        elif not self.file_no_special():
            print 'File name   : {}'.format(self.filename)

        print 'File offset : {}'.format(self.file_off)
        print 'File min/max: {}/{}'.format(self.min, self.max)

    def get_file(self, dpn):
        pass

"""
attribute file:
        offset  size    remarks
        0       4       - format id CLMD_FORMAT_RSC10
        4       4       - reserved for security encryption info
        8       1       - type.  enum AttributeType  {... RC_UNKNOWN = 255 }
                        bit (0x80) indicates string interpretation: 0 for dictionary (lookup), 1 for long text (>=RSc4)
        9       8       - number of objects
        17      8       - number of nulls
        25      1       - attribute flags
                                null_mode         : bit 1
                                declared_unique   : bit 2
                                is_primary        : bit 3
                                is_unique         : bit 4
                                is_unique_updated : bit 5
                                no_compress?      : 0x80
        26      2       - precision
        28      1       - scale
        29      1       - obsolete
        30      4       - number of packs
        34      4       - packs offset          # after PF_END
        38      4       - dict offset           #
        42      4       - special offset        #
        47      ?       - column name, null terminated
                ?       - description string, null terminated
                4       - total number of pack files
                4       - PF_END (-4) Special value indicates the end of numbers list.
[ for lookup or numeric type]
                8       - i_min
                8       - i_max
[ for lookup ]
                ??      - dict data  [ see FTree ]
"""
class Attribute:
    def __init__(self, table, attr_no):
        self.table = table
        self.attr_no = attr_no

        self.file = 'TA{:05}.ctb'.format(attr_no)
        self.i_min = None
        self.i_max = None
        self.dict = None
        self.last_pack_a = None
        self.last_pack_b = None
        self.packs = []

        self.parse()

    def parse(self):
        with open(self.file) as f:
            bytes = f.read(4)
            if bytes != CLMD_FORMAT_RSC10:
                print "Invalid table file: wrong attribute format. attr {}".format(attr)
                sys.exit(1)

            self.fmt = bytes

            # reserved for security encryption info
            eat = f.read(4)

            bytes = f.read(1)
            type = struct.unpack('=B', bytes)[0]
            self.lookup = (type & 0x80) == 0
            type &= ~0x80
            self.type = AttrType(type)

            bytes = f.read(8)
            self.no_obj = struct.unpack('=Q', bytes)[0]

            bytes = f.read(8)
            self.no_nulls = struct.unpack('=Q', bytes)[0]

            bytes = f.read(1)
            self.flags = struct.unpack('=B', bytes)[0]
            self.null_mode = self.flags & 0x01 != 0
            self.declared_unique = self.flags & 0x02 != 0
            self.is_primary = self.flags & 0x04 != 0
            self.is_unique = self.flags & 0x08 != 0
            self.is_unique_updated  = self.flags & 0x10 != 0
            self.no_compress = self.flags & 0x80 != 0

            bytes = f.read(2)
            self.precision = struct.unpack('=H', bytes)[0]

            bytes = f.read(1)
            self.scale = struct.unpack('=B', bytes)[0]

            eat = f.read(1)

            bytes = f.read(4)
            self.no_packs = struct.unpack('=I', bytes)[0]

            bytes = f.read(4)
            self.pack_offset = struct.unpack('=I', bytes)[0]

            bytes = f.read(4)
            self.dict_offset = struct.unpack('=I', bytes)[0]

            bytes = f.read(4)
            self.special_offset = struct.unpack('=I', bytes)[0]

            # read all the left
            bytes = f.read()
            lst = string.split(bytes, '\0', 2)
            self.name = lst[0]
            self.desc = lst[1]
            left = lst[2]
            self.total_pack_files = struct.unpack('=I', left[:4])[0]
            pf_end = struct.unpack('=i', left[4:8])[0]
            if pf_end != -4:
                print "Incorrect PF_END: {}. attr file {}".format(pf_end, self.file)
                sys.exit(1)

            if self.lookup or self.type.is_numeric():
                bytes = left[8:16]
                self.i_min = struct.unpack('=Q', bytes)[0]
                bytes = left[16:24]
                self.i_max = struct.unpack('=Q', bytes)[0]

            if self.lookup:
                # dict data...
                self.dict = left[24:]

        self.load_dpns()

    def load_dpns(self):
        dpn_file = 'TA{:05}DPN.ctb'.format(self.attr_no)
        st = os.path.getsize(dpn_file)
        if st % DPN_PACK_SIZE:
            sys.exit('DPN file size {} is incorect.'.format(st))


        no_dpns = st/DPN_PACK_SIZE

        # the first two blocks are mandatory
        if no_dpns < 2:
            sys.exit('DPN file size {} is incorect.'.format(st))

        with open(dpn_file) as f:
            self.last_pack_a = DP(self, f.read(DPN_PACK_SIZE), no_dpns-2)
            self.last_pack_b = DP(self, f.read(DPN_PACK_SIZE), no_dpns-2)
            bytes = f.read(DPN_PACK_SIZE)
            dp_no = 0
            while bytes:
                self.packs.append(DP(self, bytes, dp_no))
                dp_no = dp_no+1
                bytes = f.read(DPN_PACK_SIZE)
            self.packs.append(self.last_pack_a)

    def show(self):
        print 'Name    : {}'.format(self.name)
        print 'Type    : {}'.format(self.type.name())
        print 'Format  : {}'.format(self.fmt)
        print 'Lookup  : {}'.format(self.lookup)
        print 'No objs : {}'.format(self.no_obj)
        print 'No nulls: {}'.format(self.no_nulls)
        print 'Compress: {}'.format(not self.no_compress)

        if self.i_min:
            print "min     : {}".format(self.i_min)
            print "max     : {}".format(self.i_max)
        #print 'and dict data...',
        for dpn in self.packs:
            dpn.show()

class StoneTable:
    """A class for StoneDB Table """

    def __init__(self, path):
        self.path = path
        self.attrs = []
        os.chdir(path)
        self.blocks = []
        self.mysql_tab_num = -1

        self.file_list = os.listdir(path)

        if TABLE_FILE not in self.file_list:
            print "No table file exists"
            sys.exit(1)

        with open(TABLE_FILE) as f:
            self.ident = f.read(4)
            if self.ident != TABLE_FILE_IDENT:
                print "Invalid table file: wrong identifier"
                sys.exit(1)

            self.format_id = f.read(1)
            if self.format_id != '2':
                print "Invalid table file: wrong format id {0} ".format(self.format_id)
                sys.exit(1)

            bytes = f.read(4)
            bytes = f.read(8)
            bytes = f.read(8)
            bytes = f.read(4)
            self.no_attr = struct.unpack('=i', bytes)[0]

            bytes = f.read(4)
            self.block_offset = struct.unpack('=i', bytes)[0]

            bytes = f.read(self.no_attr)
            self.attr_mode = struct.unpack('='+str(self.no_attr)+'b', bytes)

            # read all the left
            bytes = f.read()
            left = string.split(bytes, '\0', 2)
            self.name = left[0]
            self.desc = left[1]
            self.parse_blocks(left[2])
        self.parse_table_file()

    def parse_blocks(self, data):
        length = len(data)
        while length > 0:
            (blk_len, blk_type, has_next) = struct.unpack('=ibb', data[0:6])
            self.blocks.append((blk_len, blk_type, has_next, data[6:blk_len]))
            length = length - blk_len
        # the first block stores the MySQL internal table number
        self.mysql_tab_num = struct.unpack('=i', self.blocks[0][3])[0]

    def show(self):
        if OPT.dump_table_info:
            print ''
            print '---------- Table Info ------------'
            print 'Table name    : {}'.format(self.name)
            print 'Table ident   : {}'.format(self.ident)
            print 'Format ID     : {}'.format(self.format_id)
            print 'Description   : {}'.format(self.desc)
            print 'No. of cols   : {}'.format(self.no_attr)
            print 'Attr Mode     : {}'.format(self.attr_mode)
            print 'Block offset  : {}'.format(self.block_offset)
            print 'MySQL table # : {}'. format(self.mysql_tab_num)
            print ''

        if OPT.dump_column_info:
            for a in self.attrs:
                print '----------- Column {:03} ---------------'.format(a.attr_no)
                a.show()

    def parse_table_file(self):
        for attr in range(0, self.no_attr):
            self.parse_attr_file(attr)

    def parse_attr_file(self, attr):
        attr = Attribute(self, attr)
        self.attrs.append(attr)

"""

 file format ("Table.ctb"):
     "RScT"          4   - file identifier
     <format_id>     1   - "0", "1" - obsolete formats, "2" - current
     <security>      4   - reserved for security info
     <no_all_obj>    8   - reserved for total no. of objects (including deleted); currently not used
     <no_cur_obj>    8   - reserved for current (excluding deleted) no. of objects; currently not used
     <no_attr>       4   - number of attributes
     <block_offset>  4   - offset of the first special block (0 means no such block)

     <attr_mode>     no_attr - one-byte attribute parameter; 0 for deleted attribute, !=0 otherwise

     <tab_name>      ...   - table name, null-terminated
     <tab_desc>      ...   - table description, null-terminated

     <block_1>       ...   - MySQL special blocks special block: internal table number
     <block_2>       ...   - no used ???
     ...
     <block_n>       ...

     general sctructure of a block header:

                          <blk_len>   4 - including 6 bytes of header
                          <blk_type>  1 - reserved (0 is assumed as interpretable now)
                          <has_next>   1 - 0 for the last block, 1 otherwise
"""
def get_datadir():
    try:
        config = ConfigParser.RawConfigParser({'datadir': '/usr/local/stonedb/data'},
                                              allow_no_value=True)
        config.readfp(open(OPT.cnf_file))
        return config.get('mysqld', 'datadir')
    except ConfigParser.Error as error:
        raise

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('db', help='database to use')
    parser.add_argument('table', help='table name')
    parser.add_argument('--config_file', help='mysql config file')
    parser.add_argument('-t', '--table_info', action="store_true", help='dump table information')
    parser.add_argument('-c', '--column_info', action="store_true", help='dump columns information')
    parser.add_argument('-d', '--dp_info', action="store_true", help='dump DP information')
    args = parser.parse_args()

    db, table = args.db, args.table
    if args.config_file:
        OPT.cnf_file = args.config_file
    if args.table_info:
        OPT.dump_table_info = True
    if args.column_info:
        OPT.dump_column_info = True
    if args.dp_info:
        OPT.dump_dp_info = True

    datadir = get_datadir()

    datadir = datadir+ "/" + db + "/" + table + ".bht"

    tab = StoneTable(datadir)
    tab.show()


if __name__ == '__main__':

    main()

