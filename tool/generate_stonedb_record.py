#!/usr/bin/python
import os
import sys
import time
import re
import traceback
import fcntl
import socket
from random import Random

desccmd='/usr/local/stonedb/bin/mysql -uroot  test_long -e "show create table  '
loadcmd='/usr/local/stonedb/bin/mysql -uroot  test_long -e "LOAD DATA LOCAL INFILE \''
field_dict={}
table_field_list=[]
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
def get_ip():
    A=120
    B=random_int(1,100)
    C=random_int(100,200)
    D=random_int(1,2)
    ip = "%d.%d.%d.%d" %(A,B,C,D)
    return ip
def generate_record(id):
    global table_field_list
    record=''
    for field in table_field_list:
        tmp=field.split('`')
        field_name=tmp[1]
        field_type=tmp[2]
        if( field_type.find('bigint')>=0):
            if( field_name == '_version' or field_name == 'version' or field_name == 'id'):
                record=record+str(int(time.time()))+','
            elif( field_name =='app'):
                record=record+str(random_int(1,1000))+','
            else:
                record=record+str(random_int(1,10000000))+','
        elif(field_type.find('int')>=0):
            if(field_name =='id'):
                record=record+str(id)+','
            else:
                record=record+str(random_int(1,10000))+','
        elif(field_type.find('varchar')>=0):
            length=int(field_type.split('(')[1].split(')')[0])
            if(length>10000):
                length=800
            if( field_type.find('lookup' )>=0):
                record=record+'"'+str(random_int(1,999))+'loo",'
            elif(field_name.lower().find('ip' )>=0):
                record=record+'"'+get_ip()+'",'
            else:
                if(length>100):
                    record=record+'"'+random_str(random_int(10,30))+'",'
                else:
                    record=record+'"'+random_str(random_int(3,length))+'",'
        elif(field_type.find('date')>=0 or field_type.find('timestamp')>=0):
            record=record+'"'+time.strftime('%Y-%m-%d %H:%M:%S')+'",'
        else:
            length=int(field_type.split('(')[1].split(')')[0])
            if( field_type.find('lookup' )>=0):
                record=record+'"'+str(random_int(1,199))+'lookkk",'
            else:
                record=record+'"'+random_str(length)+'",'
    record=record[0:-1]+'\n'
    return record
def main():
    try:
        if(len(sys.argv)!=4 ):
            print "useage:",sys.argv[0],"table_name generaterecordnum beginkey"
            return
        global desccmd
        global loadcmd
        global field_dict
        global table_field_list
        table_name = sys.argv[1]
        recordnum=sys.argv[2]
        beginkey=int(sys.argv[3])
        desccmd = desccmd + table_name +'"'
        table_field_list = os.popen(desccmd).read().split('\\n')
        #print "length:",len(table_field_list)
        #print table_field_list
        table_field_list=table_field_list[1:len(table_field_list)-1]
        #print "length:",len(table_field_list)
        #print table_field_list
        #return
        file_name=table_name+".csv"
        fd=open(file_name,"w")
        num=0;
        for i in range(int(recordnum)):
            record=generate_record(i+beginkey)
            fd.write(record)
            num=num+1;
            if( num%1000 == 0 ):
                fd.flush()
                print "generate record num:",num
        fd.close()
        print "finish total generate record num:",num
        selectcmd='/usr/local/stonedb/bin/mysql -uroot  test_long -e "select count(*) from  '+table_name+'"'
        create_result_table='/usr/local/stonedb/bin/'
        create_result_table+='mysql -uroot test_long -e "create table if not exists test_long_load_result(storageEngine varchar(20),tablename varchar(100),'
        create_result_table+='currentRow bigint,insertRow bigint,costtime double,test_longtime datetime)"'
        os.popen(create_result_table).read().split('\n')
        selectresult=os.popen(selectcmd).read().split('\n')
        print "select recult:",selectresult[1]
        loadcmd_lz=loadcmd+os.getcwd()+"/"+file_name
        loadcmd=loadcmd+os.getcwd()+"/"+file_name
        loadcmd+='\' INTO TABLE '+table_name+' FIELDS TERMINATED BY \',\'"'
        loadcmd_lz+='\' INTO TABLE '+table_name+'_lz FIELDS TERMINATED BY \',\'"'
        print loadcmd
        print loadcmd_lz
        print "statr time:",time.asctime()
        start_time=time.mktime(time.localtime())
        t1=time.time()
        loadresult_list = os.popen(loadcmd).read().split('\n')
        t2=time.time()
        costtime=str(t2-t1)
        #loadresult_list = os.popen(loadcmd_lz).read().split('\n')
        print "end time:",time.asctime()
        end_time=time.mktime(time.localtime())
        print "cost time:",end_time-start_time," seconds"
        insert_result_cmd='/usr/local/stonedb/bin/'
        insert_result_cmd+='mysql -uroot  test_long -e "insert into test_long_load_result  values(\'stonedb\',\''+table_name+'\','+selectresult[1]
        insert_result_cmd+=','+recordnum+','+costtime+',now())"'
        print insert_result_cmd
        #os.popen(insert_result_cmd).read().split('\n')
    except Exception, e:
        fd.close()
        print "generate record error:",e
sys.exit( main() )
