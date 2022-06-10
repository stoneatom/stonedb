#!/usr/bin/python
import os
import sys
import time
import re
import traceback
import fcntl
import socket
from random import Random
import MySQLdb

desccmd='mysql -uroot  test -e "desc '
g_user = "alimail_alarm"
g_password = "alimail"
g_port = 3306
g_host ="127.0.0.1"
database="test"

field_dict={}
table_field_list=[]
date_id_list=[20150720,20150821,20150622,20150523,20150824,20150825,20150826,20150827,20150828,20150829]

try:
    conn =MySQLdb.connect(host=g_host, user=g_user, passwd=g_password, db=database, port=g_port, use_unicode=1 ,charset='utf8')
    cur = conn.cursor()
    cur.execute('set names utf8')
except Exception, e:
    print "sql exception",str(e)


def random_str(randomlength=8):
    str = ''
    chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        str+=chars[random.randint(0, length)]
    return str

def random_int(min,max):
    return Random().randint(min,max)
def generate_record(id):
    global table_field_list
    record='('
    for field in table_field_list:
        #print field
        tmp=field.split('\t')
        field_name=tmp[0]
        field_type=tmp[1]
        if(field_type.find('int')>=0):
            if(field_name =='id'):
                record=record+str(id)+','
            elif(field_name == 'date_id'):
                record=record+str(date_id_list[random_int(0,9)])+','
            else:
                record=record+str(random_int(1,100))+','
        elif(field_type.find('varchar')>=0):
            length=int(field_type.split('(')[1].split(')')[0])
            if( field_name =='date_id' ):
                record=record+'"'+str(date_id_list[random_int(0,9)])+'",'
            else:
                record=record+'"'+random_str(random_int(1,length))+'",'
        else:
            length=int(field_type.split('(')[1].split(')')[0])
            if( field_name =='date_id' ):
                record=record+'"'+str(date_id_list[random_int(0,9)])+'",'
            else:
                record=record+'"'+random_str(length)+'",'
    record=record[0:-1]+'),'
    return record

def insert_record(record):
    sqlcmd="insert ignore into "+sys.argv[1]+" values"+record
    try:
    #    print sqlcmd
        cur.execute(sqlcmd)
        conn.commit()
    except Exception, e:
        print "insert exception: "+str(e)
def main():
    try:
        if(len(sys.argv)!=4 ):
            print "useage:",sys.argv[0],"table_name generaterecordnum beginid"
            return
        print "statr time:",time.asctime()
        start_time=time.mktime(time.localtime())
        global desccmd
        global field_dict
        global table_field_list
        table_name = sys.argv[1]
        recordnum=sys.argv[2]
        beginid=int(sys.argv[3])
        desccmd = desccmd + table_name +'"'
        table_field_list = os.popen(desccmd).read().split('\n')
        #print "length:",len(table_field_list)
        #print table_field_list
        table_field_list=table_field_list[1:len(table_field_list)-1]
        num=0;
        values=''
        for i in range(int(recordnum)):
            record=generate_record(i+beginid)
            values=values+record
            num=num+1
            if( num%1000 == 0 ):
                print "generate record num:",num
                values=values[0:-1]
                insert_record(values)
                values=''
        if( len(values)>1 ):
            values=values[0:-1]
            insert_record(values)
        print "finish total generate record num:",num
        print "end time:",time.asctime()
        end_time=time.mktime(time.localtime())
        print "cost time:",end_time-start_time," seconds"
        cur.close()
        conn.close()
    except Exception, e:
        print "generate record error:",e
        cur.close()
        conn.close()
sys.exit( main() )
