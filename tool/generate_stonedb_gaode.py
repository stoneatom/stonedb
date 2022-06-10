#!/usr/bin/python
import os
import sys
import time
import re
import traceback
import fcntl
import socket
from random import Random
sql='mysql -h 10.183.199.143 -uangle -p123456'
desccmd=sql+' test -e "desc '
loadcmd=sql+' test -e "LOAD DATA LOCAL INFILE \''
field_dict={}
table_field_list=[]
month=1
date_id_list=['20150520','20150621','20150722','20150823','20150824','20150825','20150826','20150827','20150828','20150829']
date_id1=[20150100,20150200,20150300,20150400,20150500,20150600,20150700,20150800,20150900,20151000,20151100,20151200]
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
    record=''
    userid=random_int(1,1000000)
    for field in table_field_list:
        #print field
        tmp=field.split('\t')
        field_name=tmp[0]
        field_type=tmp[1]
        if(field_type.find('int')>=0):
            if(field_name =='d_inttime'):
                record=record+str(date_id1[month-1]+id)+','
            elif( field_name =='d_intuserid' ):
                record=record+str(userid)+','
            elif(field_name =='d_intversion'):
                record=record+str(random_int(1,100))+','
            elif(field_name =='m_pv'):
                record=record+str(random_int(1,100000000))+','
            else:
                record=record+str(random_int(1,100))+','
        elif(field_type.find('date')>=0):
            if( field_name =='d_datetime' ):
                record=record+str(date_id1[month-1]+id)+','
        elif(field_type.find('varchar')>=0):
            length=int(field_type.split('(')[1].split(')')[0])
            if( field_name =='d_userid' ):
                record=record+'"'+str(userid)+'",'
            elif(field_name =='d_version'):
               record=record+'"'+str(random_int(1,100))+'",'
            else:
                record=record+'"'+random_str(length)+'",'
        else:
            length=int(field_type.split('(')[1].split(')')[0])
            if( field_name =='date_id' ):
                record=record+'"'+str(date_id1[0]+id)+'",'
            else:
                record=record+'"'+random_str(length)+'",'
    record=record[0:-1]+'\n'
    return record
def main():
    try:
        if(len(sys.argv)!=5 ):
            print "useage:",sys.argv[0],"table_name generaterecordnum  month day"
            return
        global desccmd
        global loadcmd
        global field_dict
        global table_field_list
        global month
        table_name = sys.argv[1]
        recordnum=sys.argv[2]
        beginkey=0
        month=int(sys.argv[3])
        day=int(sys.argv[4])
        desccmd = desccmd + table_name +'"'
        table_field_list = os.popen(desccmd).read().split('\n')
        #print "length:",len(table_field_list)
        #print table_field_list
        table_field_list=table_field_list[1:len(table_field_list)-1]
        #print "length:",len(table_field_list)
        #print table_field_list
        file_name=table_name+".csv"
        fd=open(file_name,"w")
        num=0;
        for i in range(int(recordnum)):
            record=generate_record(day)
            fd.write(record)
            num=num+1;
            if( num%1000 == 0 ):
                fd.flush()
                print "generate record num:",num
        fd.close()
        print "finish total generate record num:",num
        selectcmd=sql+' test -e "select count(*) from  '+table_name+'"'
        create_result_table=''
        create_result_table+=sql+' test -e "create table if not exists test_load_result(storageEngine varchar(20),tablename varchar(100),'
        create_result_table+='currentRow bigint,insertRow bigint,costtime double,testtime datetime)"'
        os.popen(create_result_table).read().split('\n')
        selectresult=os.popen(selectcmd).read().split('\n')
        print "select recult:",selectresult[1]
        loadcmd=loadcmd+os.getcwd()+"/"+file_name
        loadcmd+='\' INTO TABLE '+table_name+' FIELDS TERMINATED BY \',\'"'
        print loadcmd
        print "statr time:",time.asctime()
        start_time=time.mktime(time.localtime())
        t1=time.time()
        loadresult_list = os.popen(loadcmd).read().split('\n')
        t2=time.time()
        costtime=str(t2-t1)
        print "end time:",time.asctime()
        end_time=time.mktime(time.localtime())
        print "cost time:",end_time-start_time," seconds"
        insert_result_cmd=''
        insert_result_cmd+=sql+'  test -e "insert into test_load_result  values(\'stonedb\',\''+table_name+'\','+selectresult[1]
        insert_result_cmd+=','+recordnum+','+costtime+',now())"'
        print insert_result_cmd
        os.popen(insert_result_cmd).read().split('\n')
    except Exception, e:
        fd.close()
        print "generate record error:",e
sys.exit( main() )

