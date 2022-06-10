#!/bin/bash

# Alimonitor command for StoneDB QPS monitor

TMP_FILE=/tmp/stonedb_stat.tmp
SAVE_FILE=/tmp/stonedb_stat.save

check_qps() {
        sed -i 's/  /:/g' $TMP_FILE
        UPTIME=$(cut -d: -f2 $TMP_FILE)
        Q=$(cut -d: -f6 $TMP_FILE)
        QPS=$(cut -d: -f16 $TMP_FILE)
        SERVER_PID=$(pidof mysqld)
        
        RECORD="NONE"
        if [ -f $SAVE_FILE ]
        then
                # we have historical data
                RECORD=($(cat $SAVE_FILE))
        fi
        
        echo "$SERVER_PID $UPTIME $QPS $Q" > $SAVE_FILE
        
        if [ "$RECORD" != "NONE" ]
	then
		if [ $SERVER_PID -eq ${RECORD[0]} ]
		then
			LAST_UPTIME=${RECORD[1]}
			if [ $LAST_UPTIME -eq $UPTIME ]
			then
				ERROR="script called too frequently"
				return
			fi

			LAST_Q=${RECORD[3]}
			let Q_=Q-LAST_Q
			let UPTIME_=UPTIME-LAST_UPTIME

			# bash doesn't ssupport floating number so this is a kludge
			let Q_=Q_*1000
			let QPS=Q_/UPTIME_

			let QPS_2=QPS%1000
			let QPS_1=QPS/1000
			QPS=$QPS_1.$QPS_2
		else
			#"server restarted"
			ERROR="server restarted"
		fi
	fi

        echo -n '{ "collection_flag":0, "error_info":"", '
        echo "\"MSG\":[{\"uptime\":$UPTIME, \"questions\":$Q, \"qps\":$QPS}]}"

}

# main procedure

if [ ! -x /usr/local/stonedb/bin/mysqladmin ]
then
        echo '{ "collection_flag":1, "error_info":"mysqladmin does not exist", "MSG": [{}] }'
        exit
fi

ERROR="OK"

/usr/local/stonedb/bin/mysqladmin status 2>/dev/null 1>$TMP_FILE
if [ $? -eq 0 ]
then
	check_qps
else
	ERROR="mysqladmin status failed"
fi

if [ "$ERROR" != "OK" ]
then
        echo "{ \"collection_flag\":1, \"error_info\":\"$ERROR\", \"MSG\": [{}] }"
fi

rm -f $TMP_FILE

