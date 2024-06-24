from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
from pyspark.sql import functions as F 
from pyspark.sql.functions import ( 
    abs, avg, broadcast, count, col, concat_ws, countDistinct, desc, exp, expr, explode, first, from_unixtime, 
    lpad, length, lit, max, min, rand, regexp_replace, round, sum, to_date, udf, when, 
) 
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession 
from pyspark.sql import Row
from datetime import datetime, timedelta, date 
from dateutil.parser import parse
import tempfile 
import argparse 
import time 
import requests 
import pyodbc 
import numpy as np 
import pandas as pd 
import functools
import json
from operator import add 
import mgrs
from functools import reduce 
from operator import add 
from sklearn.neighbors import BallTree
from math import radians, cos, sin, asin, sqrt
import sys 
from typing import List

sys.path.append('/usr/apps/vmas/scripts/ZS/OOP_dir') 
from MailSender import MailSender

def flatten_df_v2(nested_df):
    # flat the nested columns and return as a new column
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    array_cols = [c[0] for c in nested_df.dtypes if c[1][:5] == "array"]
    #print(len(nested_cols))
    if len(nested_cols)==0 and len(array_cols)==0 :
        #print(" dataframe flattening complete !!")
        return nested_df
    elif len(nested_cols)!=0:
        flat_df = nested_df.select(flat_cols +
                                   [F.col(nc+'.'+c).alias(nc+'_b_'+c)
                                    for nc in nested_cols
                                    for c in nested_df.select(nc+'.*').columns])
        return flatten_df_v2(flat_df)
    elif len(array_cols)!=0:
        for array_col in array_cols:
            flat_df = nested_df.withColumn(array_col, F.explode(F.col(array_col)))
        return flatten_df_v2(flat_df) 

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS').getOrCreate()
    mail_sender = MailSender() 
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa = "hdfs://njbbepapa1.nss.vzwnet.com:9000/"

    for i in [5,10,15,20,30,45]:
        date_str = ( date.today() - timedelta(i) ).strftime('%Y-%m-%d')

        cause_path = hdfs_pa+ f"/test/cpdata-7/date={date_str}/"
        cause_codes = ["8000","8001","8002","8040","8010","8011","8012","8013","8020","8021","8040" ]

        flatten_df_v2(spark.read.parquet(cause_path))\
                .select("data_b_cause_code")\
                .groupby("data_b_cause_code")\
                    .count()\
                    .orderBy(F.desc("count"))\
                    .show(truncate = False)
        sys.exit()
        df_cause = flatten_df_v2(spark.read.parquet(cause_path))\
                .withColumn("cause_code", F.regexp_extract(col("data_b_cause_code"), r"^(\d{4})", 1).cast("string"))\
                .withColumn("enodeb", F.split(col("data_b_eNodeB"), "_")[0])\
                .withColumn('msisdn_num',F.substring('data_b_msisdn', 2,10))\
                .withColumn('fromUri_num', F.substring('data_b_fromUri', 7, 10))\
                .filter( col("eNodeB").isNotNull() )\
                .filter( col("cause_code").isNotNull() )\
                .filter( col("cause_code").isin(cause_codes) )
        df_cause.groupby("cause_code")\
            .count()\
            .orderBy(F.desc("count"))\
            .show()
        


        read_path = hdfs_pd + "/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_Post_tickets_Feature_Date/{}_tickets_Post_Feature_of_Enodeb/".format(date_str)

        features_list = ['bearer_drop_rate',"sip_dc_rate","dl_data_volume","rrc_setup_failure","rtp_gap","avgconnected_users","uptp_user_perceived_throughput"]
        alert_feature_list = [  f"{feature}_alert"  for feature in features_list]
        selected_feature = ["enodeb"]  + alert_feature_list

        df_snap = spark.read.option("recursiveFileLookup", "true")\
                    .option("header", "true").csv(read_path)\
                    .filter(col("has_abnormal_kpi") == 1)\
                    .select(selected_feature)
        
        site_kpis_dict = { 
                            "bearer_drop_rate_alert": ["8000", "8001"], 
                            "sip_dc_rate_alert": ["8002", "8000", "8001"], 
                            "dl_data_volume_alert": ["8040"], 
                            "rrc_setup_failure_alert": ["8010", "8013", "8011", "8012"], 
                            "rtp_gap_alert": ["8021", "8020", "8010", "8013", "8011", "8012"] 
                        } 

        df_snap_cause = df_cause.join( F.broadcast( df_snap ), "enodeb" )\
                                .select( ["enodeb","fromUri_num","msisdn_num","cause_code"] + alert_feature_list )
        
        df_snap_cause.groupby("cause_code")\
            .count()\
            .orderBy(F.desc("count"))\
            .show()


        df_list = []
        key = "sip_dc_rate_alert"
        for key in site_kpis_dict.keys():
            for v in site_kpis_dict[key]:

                df = df_snap_cause.filter( col("cause_code")==v )\
                        .filter( col(key)==1 )\
                        .select("enodeb","fromUri_num", "msisdn_num", "cause_code",)\
                        .withColumn( "abnormal_feature", F.lit(key))

                if df.count() >0:
                    df_list.append(df)

        from functools import reduce
        df_list = list(filter(None, df_list)) 
        result_df = reduce(lambda df1, df2: df1.union(df2), df_list) 
        result_df.show()
        print( result_df.count() )
        sys.exit()
        output_path = hdfs_pd + f"/user/ZheS/snap_impacted_users/{date_str}"
        result_df.write.format("parquet")\
                    .mode("overwrite")\
                    .save(output_path)