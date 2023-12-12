#!/bin/bash 

export HADOOP_HOME=/usr/apps/vmas/hadoop-3.2.2 
export SPARK_HOME=/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2 
export PYSPARK_PYTHON=/usr/apps/vmas/anaconda3/bin/python3 

# Define the date range 
start_date="2023-12-04" 
end_date="2023-12-11" 
 
echo "RES: Starting===" 
echo $(date) 
 
# Update your script using the variable 
spark_submit_command="/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://njbbepapa1.nss.vzwnet.com:7077 --conf \"spark.sql.session.timeZone=UTC\" --conf \"spark.driver.maxResultSize=2g\" --conf \"spark.dynamicAllocation.enabled=false\" --num-executors 30 --executor-cores 2 --total-executor-cores 60 --executor-memory 2g --driver-memory 6g --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2" 

# Iterate over the date range 
current_date=$start_date 
while [ "$current_date" != "$end_date" ]; do 
  echo "Processing date: $current_date" 

  # Modify your commands to use the current date variable 
  $spark_submit_command /usr/apps/vmas/script/ZS/SNAP/enodeb/pre_enodeb.py --date "$current_date" > /usr/apps/vmas/script/ZS/SNAP/enodeb/pre_enodeb.log 
  $spark_submit_command /usr/apps/vmas/script/ZS/SNAP/enodeb/post_enodeb.py --date "$current_date" > /usr/apps/vmas/script/ZS/SNAP/enodeb/post_enodeb.log 
  $spark_submit_command /usr/apps/vmas/script/ZS/SNAP/sector/pre_sector.py --date "$current_date" > /usr/apps/vmas/script/ZS/SNAP/sector/pre_sector.log 
  $spark_submit_command /usr/apps/vmas/script/ZS/SNAP/sector/post_sector.py --date "$current_date" > /usr/apps/vmas/script/ZS/SNAP/sector/post_sector.log 
  $spark_submit_command /usr/apps/vmas/script/ZS/SNAP/carrier/pre_carrier.py --date "$current_date" > /usr/apps/vmas/script/ZS/SNAP/carrier/pre_carrier.log 
  $spark_submit_command /usr/apps/vmas/script/ZS/SNAP/carrier/post_carrier.py --date "$current_date" > /usr/apps/vmas/script/ZS/SNAP/carrier/post_carrier.log 
  # Increment the current date 
  current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d) 
done 