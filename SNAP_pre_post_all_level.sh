#!/bin/bash 

export HADOOP_HOME=/usr/apps/vmas/hadoop-3.2.2 
export SPARK_HOME=/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2 
export PYSPARK_PYTHON=/usr/apps/vmas/anaconda3/bin/python3 
echo "RES: Starting===" 
echo $(date) 

/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://njbbepapa1.nss.vzwnet.com:7077 --conf "spark.sql.session.timeZone=UTC" --conf "spark.driver.maxResultSize=2g" --conf "spark.dynamicAllocation.enabled=false" --num-executors 30 --executor-cores 2 --total-executor-cores 60 --executor-memory 2g --driver-memory 6g --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 /usr/apps/vmas/script/ZS/SNAP/enodeb/pre_enodeb.py> /usr/apps/vmas/script/ZS/SNAP/enodeb/pre_enodeb.log

/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://njbbepapa1.nss.vzwnet.com:7077 --conf "spark.sql.session.timeZone=UTC" --conf "spark.driver.maxResultSize=2g" --conf "spark.dynamicAllocation.enabled=false" --num-executors 50 --executor-cores 2 --total-executor-cores 100 --executor-memory 2g --driver-memory 6g --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 /usr/apps/vmas/script/ZS/SNAP/enodeb/post_enodeb.py> /usr/apps/vmas/script/ZS/SNAP/enodeb/post_enodeb.log

/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://njbbepapa1.nss.vzwnet.com:7077 --conf "spark.sql.session.timeZone=UTC" --conf "spark.driver.maxResultSize=2g" --conf "spark.dynamicAllocation.enabled=false" --num-executors 10 --executor-cores 2 --total-executor-cores 20 --executor-memory 2g --driver-memory 6g --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 /usr/apps/vmas/script/ZS/SNAP/sector/pre_sector.py> /usr/apps/vmas/script/ZS/SNAP/sector/pre_sector.log

/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://njbbepapa1.nss.vzwnet.com:7077 --conf "spark.sql.session.timeZone=UTC" --conf "spark.driver.maxResultSize=2g" --conf "spark.dynamicAllocation.enabled=false" --num-executors 50 --executor-cores 2 --total-executor-cores 100 --executor-memory 2g --driver-memory 16g --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 /usr/apps/vmas/script/ZS/SNAP/sector/post_sector.py> /usr/apps/vmas/script/ZS/SNAP/sector/post_sector.log

/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://njbbepapa1.nss.vzwnet.com:7077 --conf "spark.sql.session.timeZone=UTC" --conf "spark.driver.maxResultSize=2g" --conf "spark.dynamicAllocation.enabled=false" --num-executors 10 --executor-cores 2 --total-executor-cores 20 --executor-memory 2g --driver-memory 6g --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 /usr/apps/vmas/script/ZS/SNAP/carrier/pre_carrier.py> /usr/apps/vmas/script/ZS/SNAP/carrier/pre_carrier.log

/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://njbbepapa1.nss.vzwnet.com:7077 --conf "spark.sql.session.timeZone=UTC" --conf "spark.driver.maxResultSize=2g" --conf "spark.dynamicAllocation.enabled=false" --num-executors 50 --executor-cores 2 --total-executor-cores 100 --executor-memory 2g --driver-memory 6g --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 /usr/apps/vmas/script/ZS/SNAP/carrier/post_carrier.py> /usr/apps/vmas/script/ZS/SNAP/carrier/post_carrier.log

/usr/apps/vmas/anaconda2/bin/python /usr/apps/vmas/venky/Event_Enodeb_Post_tickets_Feature_Date/Event_Enodeb_Post_tickets_Feature_Date_v1.py 

/usr/apps/vmas/anaconda2/bin/python /usr/apps/vmas/venky/Event_Enodeb_Post_tickets_Feature_Date/sector/Event_Enodeb_Post_tickets_Feature_Date_v1.py 

/usr/apps/vmas/anaconda2/bin/python /usr/apps/vmas/venky/Event_Enodeb_Post_tickets_Feature_Date/Carrier/Event_Enodeb_Post_tickets_Feature_Date.py 

/usr/apps/vmas/anaconda2/bin/python /usr/apps/vmas/venky/Daily_KPI_14_days_pre_Event/Daily_KPI_14_days_pre_Event_v1.py 

/usr/apps/vmas/anaconda2/bin/python /usr/apps/vmas/venky/Daily_KPI_14_days_pre_Event/sector/Daily_KPI_14_days_pre_Event_v1.py 

/usr/apps/vmas/anaconda2/bin/python /usr/apps/vmas/venky/Daily_KPI_14_days_pre_Event/Carrier/Daily_KPI_14_days_pre_Event.py 

Echo "Waiting Druid upload"
sleep 300 

/usr/apps/vmas/spark-3.2.0-bin-hadoop3.2/bin/spark-submit --master spark://njbbepapa1.nss.vzwnet.com:7077 --conf "spark.sql.session.timeZone=UTC" --conf "spark.driver.maxResultSize=2g" --conf "spark.dynamicAllocation.enabled=false" --num-executors 50 --executor-cores 2 --total-executor-cores 100 --executor-memory 2g --driver-memory 6g --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 /usr/apps/vmas/script/ZS/SNAP/CheckCompleteSnap.py > /usr/apps/vmas/script/ZS/SNAP/CheckCompleteSnap.log