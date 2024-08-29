#!/bin/bash 

export HADOOP_HOME=/usr/apps/vmas/hadoop-3.3.6 
export SPARK_HOME=/usr/apps/vmas/spark 
export PYSPARK_PYTHON=/usr/apps/vmas/anaconda3/bin/python3 
echo "RES: Starting" 
echo $(date) 
/usr/apps/vmas/spark/bin/spark-submit \
--master spark://njbbvmaspd11.nss.vzwnet.com:7077 \
--conf "spark.sql.session.timeZone=UTC" \
--conf "spark.driver.maxResultSize=2g" \
--conf "spark.dynamicAllocation.enabled=false" \
--num-executors 3 \
--executor-cores 2 \
--total-executor-cores 6 \
--executor-memory 1g \
--driver-memory 2g \
--packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 \
/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/vmb_post_snap.py > \
/usr/apps/vmas/scripts/ZS/snap/VMAS-Post-SNAP-Maintenance-Alert/vmb_post_snap.log

 
 

