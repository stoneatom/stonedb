#!/bin/bash

# Alimonitor command for StoneDB statistic

MYSQL_USER=stonedb
MYSQL_PASSWORD="-p'jboj3pfeOF'"

MYSQL=/usr/local/stonedb/bin/mysql
QUERY_COUNT='select count(*) from information_schema.tables where ENGINE="STONEDB"'
CMD_Q_COUNT="${MYSQL} -u${MYSQL_USER} ${MYSQL_PASSWORD} --skip-column-names --batch -e '${QUERY_COUNT}' 2>/dev/null"

QUERY='select TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,TABLE_COMMENT from information_schema.tables where ENGINE="STONEDB";'
CMD_Q_TABLE="${MYSQL} -u${MYSQL_USER} ${MYSQL_PASSWORD} --skip-column-names --table --batch -e '${QUERY}' 2>/dev/null"

ERROR="OK"

has_error() {
	if [ "$ERROR" != "OK" ]
	then
		return 0
	else
		return 1
	fi
}

get_stonedb_table_count() {
	cnt=$(eval ${CMD_Q_COUNT})
	ret=$?
	if [ $ret -ne 0 ]
	then
		ERROR="Failed to retrieve stonedb table count"
		return
	fi
	return $cnt
}

trim() {
	local var="$*"
	var="${var#"${var%%[![:space:]]*}"}"   # remove leading whitespace characters
	var="${var%"${var##*[![:space:]]}"}"   # remove trailing whitespace characters
	echo -n "$var"
}

########### main procedure ###########

get_stonedb_table_count
COUNT=$?
if has_error
then
	echo "{ \"collection_flag\":1, \"error_info\":\"$ERROR\", \"MSG\": [{}] }"
	exit
fi

if [ "$COUNT" == "0" ]
then
	# not an error, just return empty
	echo '{ "collection_flag":0, "error_info":"", "MSG": [{}] }'
	exit
fi

IFS=$(echo -en "\n\b") result=( $(eval ${CMD_Q_TABLE}) )
if [ $? -ne 0 ]
then
	echo '{ "collection_flag":1, "error_info":"Failed to query stonedb table stat", "MSG": [{}] }'
	exit
fi

echo '{ "collection_flag":0, "error_info":"", "MSG": ['

# need to exclude the first and last line which are borders
for i in $(seq 1 $COUNT)
do
	# format
	#  | db     | table | rows   |  comment                        |
	# ----------------------------------------------------------------
	#  | test   |  ts3  | 190000 |  Overall compression ratio: 0.588, Raw size=11 MB |

	IFS='|' read -r -a array <<< "${result[$i]}"

	DB=$(trim ${array[1]})
	TABLE=$(trim ${array[2]})
	ROWS=$(trim ${array[3]})
	COMMENT=$(trim ${array[4]})
	COMMENT=${COMMENT#*Overall compression ratio: }
	COMMENT=${COMMENT%% MB}
	TO_REMOVE=", "
	IFS=" " ARRAY=(${COMMENT/, Raw size=/ })
	echo -n "{\"db\":\"$DB\", \"table\":\"$TABLE\", \"rows\":\"$ROWS\","
	echo    "\"compress_ratio\":\"${ARRAY[0]}\", \"raw_size\":\"${ARRAY[1]}\"},"
done

echo '] }'

