#!/bin/ksh/
echo "starting the job `date`"
source /etc/profile.d/eit.sh

. <>

export hive_db_nm="inventory"
today_date_hr=$(date +"%Y%m%d%H%M%S")
today_date=$(date +"%Y%m%d")
row_audit_hive_table=<>
row_audit_hive_raw_table=<>
raw_s3_loc=<>
archive_s3_loc=<>
partitionPath=${raw_s3_loc}/${today_date_hr}/

source_path=<>/AUDIT_Venkat
master_config_file_name=master_file_hive_tables.txt
#previous_date=$(date --date yesterday +"%Y%m%d")
previous_date='20180606'
log_file_path=${source_path}/logs
mkdir -p ${log_file_path}/${today_date_hr}
export DATE_FIELD='SRC_DATE'
export hiveSchema=${hive_db_nm}
script_start=$( date +%s )

filename=${source_path}/${master_config_file_name}
total_tables_audit=`wc -l ${filename} | awk {'print $1'}`
echo "total_tables_audit:$total_tables_audit"
filelines=`cat $filename`
total_tables_audit_raw_file=${log_file_path}/<>_${today_date_hr}.txt

a=0
b=0
c=0
for line in $filelines ; do
    echo "start"
	
    field1[a]=`echo $line | awk -F ',' '{print $1}'`
	field2=`echo $line | awk -F ',' '{print $2}'`
	log_file_name=${field1[a]}.txt
	log_file=${log_file_path}/${today_date_hr}
	
	if [ -z ${field1} ] || [ -z ${field2} ] ; then
    echo "Reading parameters from master config file failed, please check"
	exit 1
    fi
	# echo "field1:${field1[a]}"
	# echo "field2:$field2"
	
	hadoop fs -ls ${field2} | grep src_date=${previous_date}* | awk -F '=' {'print $2'} | awk -v s="${previous_date}" 'index($0, s) == 1' > ${log_file}/${log_file_name}
	a=`expr $a + 1`
done

for file in $log_file/*.txt ; do

	echo file:$file
	log_file_read=`cat ${file}`
	i=0
	
	tableName=`echo $file | rev | cut -d '/' -f 1 | rev | awk -F '.' {'print $1'}`
	export tableName=$tableName
	b=`expr $b + 1`
	
	if [ -z ${log_file_read} ]; then
    echo "$tableName: log file is empty, No data loaded for $previous_date" >> ${log_file_path}/empty_file_info_${today_date_hr}.txt
	echo "${tableName}|${DATE_FIELD}|||0" >> ${total_tables_audit_raw_file}
	if [ $? != 0 ]; then
		echo "Inserting data into $total_tables_audit_raw_file failed, please check"
		exit 1
	fi
	c=`expr $c + 1`
	continue
	fi

	for l in $log_file_read ; do
    latestPartition=`echo $l | grep -Eo '[[:digit:]]{12}'`
	if [ -z ${latestPartition} ]; then
    echo "Reading latest partition from path:$l failed, please check"
	exit 1
    fi
	partition_arr[i]=$latestPartition
	echo "*******************************************************"
	echo ${partition_arr[i]}
	echo "*******************************************************"
	i=`expr $i + 1`
	done
	
max=${partition_arr[0]}
min=${partition_arr[0]}

	for k in ${partition_arr[@]} ; do
		if [[ $k > $max ]] ; then
			max=$k
		fi
		if [[ $k < $min ]] ; then
			min=$k
		fi
	done

export row_audit_hive_table=${row_audit_hive_table}
export FIRST_DATE=${min}
export LAST_DATE=${max}

#current_count=`beeline -u "$hive2_jdbc_url;serviceDiscoveryMode=$serviceDiscoveryMode;zooKeeperNamespace=$zooKeeperNamespace" --silent --outputformat=csv2 --showHeader=false  -e "select count(*) from $hiveSchema.$tableName where ${DATE_FIELD} between $FIRST_DATE and $LAST_DATE;"`

current_count=18

	if [ $? != 0 ] || [ "$current_count" == "" ] || [ "$(echo ${current_count} | tr [:upper:] [:lower:])" == "null" ] ; then
        echo "ERROR : Exception happened fetching maximum count from hive."
        exit -1
	fi

export current_count=$current_count

echo "${tableName}|${DATE_FIELD}|${FIRST_DATE}|${LAST_DATE}|${current_count}" >> ${total_tables_audit_raw_file}

if [ $? != 0 ]; then
echo "Inserting data into $total_tables_audit_raw_file failed, please check"
exit 1
fi
done

if [ $b = $total_tables_audit ]; then
echo "$c table have no data for previous day:$previous_date, for details please look into Hive table:$hiveSchema.$row_audit_hive_table for todays partition:$today_date_hr"

########################################Data Insert Into Hive#############################################
hadoop fs -mkdir ${partitionPath}
hadoop fs -copyFromLocal ${total_tables_audit_raw_file} ${partitionPath}
if [ $? != 0 ]; then
echo "Copying $total_tables_audit_raw_file file from local failed, please check"
exit 1
fi
#beeline -u "$hive2_jdbc_url;serviceDiscoveryMode=$serviceDiscoveryMode;zooKeeperNamespace=$zooKeeperNamespace" -e "ALTER TABLE $hiveSchema.$row_audit_hive_raw_table ADD IF NOT EXISTS PARTITION (${DATE_FIELD} = '${today_date_hr}') LOCATION '${partitionPath}';"

hive -e "ALTER TABLE ${hiveSchema}.${row_audit_hive_raw_table} ADD IF NOT EXISTS PARTITION (${DATE_FIELD} = '${today_date_hr}') LOCATION '${partitionPath}';"
if [ $? != 0 ]; then
echo "Adding partition failed, please check"
exit 1
fi

#beeline -u "$hive2_jdbc_url;serviceDiscoveryMode=$serviceDiscoveryMode;zooKeeperNamespace=$zooKeeperNamespace" -e "INSERT INTO TABLE ${hiveSchema}.${row_audit_hive_table} partition (${DATE_FIELD}='${start_load_hr}') select TABLE_NAME,DATE_FIELD,FIRST_DATE,LAST_DATE,ROW_COUNT FROM ${hiveSchema}.${row_audit_hive_raw_table}  where ${DATE_FIELD}='${today_date_hr}';"

hive -e "INSERT INTO TABLE ${hiveSchema}.${row_audit_hive_table} partition (${DATE_FIELD}='${today_date_hr}') select TABLE_NAME,DATE_FIELD,FIRST_DATE,LAST_DATE,ROW_COUNT FROM ${hiveSchema}.${row_audit_hive_raw_table} where ${DATE_FIELD}='${today_date_hr}';"
if [ $? != 0 ]; then
echo "Inserting data from ${row_audit_hive_raw_table} to ${row_audit_hive_table} table failed, please check"
exit 1
fi

#beeline -u "$hive2_jdbc_url;serviceDiscoveryMode=$serviceDiscoveryMode;zooKeeperNamespace=$zooKeeperNamespace" -e "ALTER TABLE $hiveSchema.$row_audit_hive_raw_table DROP PARTITION (${DATE_FIELD} = '${today_date_hr}');"
hive -e "ALTER TABLE $hiveSchema.$row_audit_hive_raw_table DROP PARTITION (${DATE_FIELD} != '0');"
if [ $? != 0 ]; then
echo "Drop partition failed, please check"
exit 1
fi
hadoop fs -mkdir ${archive_s3_loc}
hadoop fs -mv ${partitionPath}/* ${archive_s3_loc}
if [ $? != 0 ]; then
echo "Archiving files failed, please check"
exit 1
fi
#################################################################################################

script_end=$( date +%s )
runtime=$((script_end-script_start))
echo "Runtime: ${runtime} seconds"
fi
########################################Log Clean Up#############################################
#rm -r ${log_file_path}/${today_date_hr}
#echo "Log Clean Up Completed"


