#!/bin/ksh/
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#Description- ESD_DETAIL_REPORT job from HIVE to POSTGRES OHL 
#  Source Table (Hive): NPIPOS.ESD_DETAIL_REPORT
#  Target Table (POSTGRES-OHL): PCD_T.L_ESD_DETAIL_REPORT
#Usage-
#  sh runJob.sh
#Modification history-
#Initial version- vmadduk1 01/07/2019
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
cd /efs/eit_hadoop/applications/idw_hdp/export_jobs/hdp_AWS_POSTGRES_PCD_T_ESD_DETAIL_REPORT_exp
. /usr/local/share/eit_hadoop/applications/idw_hdp/export_engine/db.properties
export mysqljarLibPath=/usr/local/share/eit_hadoop/applications/idw_hdp/export_lib

jobName='hdp_AWS_POSTGRES_PCD_T_ESD_DETAIL_REPORT_exp'
hiveSchema=NPIPOS
tableName=ESD_DETAIL_REPORT
current_partition=$(date +"%Y%m%d%H%M")
echo "#############Getting Latest value from Mysql JobParition table ###############"

echo "##########################GET OLD PARTITION VALUE (getProcDirectory)##########################"

oldPartitionValue=$(java -classpath "$mysqljarLibPath/mysql/*" com.tmobile.mySqlConnector.MySqlLogger "getProcDirectory" "$mySqlKeyStore" "$mySqlEncryptedPassword" "$Partition_MySql_dbHost" "$Partition_MySql_dbPort" "$Partition_MySql_dbSchema" "$Partition_MySql_dbUserName" "$jobName")

if [ "$oldPartitionValue" == "No Partition Found. Please insert record in MySql table" ]; then
        echo "No Partition Found. Please insert record in MySql table"
        exit 1
elif [ "$oldPartitionValue" == "$current_partition" ]; then
        echo "No new partition found. So, exiting gracefully"
        exit 0
else
        echo "Partition:$oldPartitionValue is available in mysql and data will be exported"
        export my_sql_part_start_val=$oldPartitionValue

fi


export TZ=America/Los_Angeles
export my_sql_part_end_val=${current_partition}
export SPARK_MAJOR_VERSION=2;
export HADOOP_CONF_DIR=/etc/hadoop/conf/;spark-submit \
--master yarn \
--deploy-mode cluster \
--jars /efs/eit_hadoop/applications/retail_inventry_ingest/lib/postgresql-42.2.2.jar \
--queue default \
--keytab /etc/security/keytabs/svc_din_dev_plt.keytab \
--principal svc_din_dev_plt@DLK_DEV_CORP.COM \
--num-executors 8 \
--executor-cores 2 \
--executor-memory 10G \
--driver-memory 10G \
--conf spark.my_sql_part_start_val=${my_sql_part_start_val} \
--conf spark.my_sql_part_end_val=${my_sql_part_end_val} \
--properties-file conf.properties \
exportHiveToPostgres.py 

if [ $? == 0 ]; then
        echo "process has successfully completed!!"
        #############latest value in MySql JobParition table ###############
	update=$(java -classpath "$mysqljarLibPath/mysql/*" com.tmobile.mySqlConnector.MySqlLogger "updateDirectory" "$mySqlKeyStore" "$mySqlEncryptedPassword" "$Partition_MySql_dbHost" "$Partition_MySql_dbPort" "$Partition_MySql_dbSchema" "$Partition_MySql_dbUserName" "$jobName" "$current_partition")

	if [ $? == 0 ]; then
        	echo "Successfully updated current_partition:$current_partition value in Mysql"
	else
        	echo "updating current_partition:$current_partition value in Mysql failed. Manual intervention required"
        	echo "update=$(java -classpath "$mysqljarLibPath/mysql/*" com.tmobile.mySqlConnector.MySqlLogger "updateDirectory" "$mySqlKeyStore" "$mySqlEncryptedPassword" "$Partition_MySql_dbHost" "$Partition_MySql_dbPort" "$Partition_MySql_dbSchema" "$Partition_MySql_dbUserName" "$jobName" "$current_partition")"
	        exit 1
	fi
	    #############Updating oldPartitionValue in Mysql JobParition table ###############
    
    echo "oldPartitionValue update process starts"
    echo "oldPartitionValue:$oldPartitionValue will be updated to Mysql"
    echo "updateDirectory"

    update_old=$(java -classpath "$mysqljarLibPath/mysql/*" com.tmobile.mySqlConnector.MySqlLogger "updateDirectory" "$mySqlKeyStore" "$mySqlEncryptedPassword" "$Partition_MySql_dbHost" "$Partition_MySql_dbPort" "$Partition_MySql_dbSchema" "$Partition_MySql_dbUserName" "$jobName"_start "$oldPartitionValue")

    if [ $? == 0 ]; then
        echo "Successfully updated oldPartitionValue:$oldPartitionValue value in Mysql"
    else 
        echo "updating oldPartitionValue:$oldPartitionValue value in Mysql failed. Manual intervention required"

        echo "update_old=$(java -classpath "$mysqljarLibPath/mysql/*" com.tmobile.mySqlConnector.MySqlLogger "updateDirectory" "$mySqlKeyStore" "$mySqlEncryptedPassword" "$Partition_MySql_dbHost" "$Partition_MySql_dbPort" "$Partition_MySql_dbSchema" "$Partition_MySql_dbUserName" "$jobName"_start "$oldPartitionValue")"
    exit 1
    fi
    #####################MySql Update end#######################
else
        echo "Error occurred with spark submit job!!"
        exit -3
fi
