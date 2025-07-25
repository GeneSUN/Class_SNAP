from pyspark.sql.window import Window 
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.functions import ( 
    abs, avg, broadcast, col, concat ,concat_ws, countDistinct, exp, expr, explode, first, from_unixtime, 
    lpad, length, lit, max, min, rand, regexp_replace, round, sum, to_date, udf, when, 
) 
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
from datetime import datetime, timedelta, date 
import argparse 
import numpy as np 
import pandas as pd 
import functools
import json
from operator import add 
import concurrent
from functools import reduce 
from operator import add 
from math import radians, cos, sin, asin, sqrt
import concurrent.futures 
import sys 
sys.path.append('/usr/apps/vmas/script/ZS/SNAP') 
from class_SNAP import SNAP_post_enodeb, get_date_window

from hdfs import InsecureClient 

def post_enodeb(spark,date_str,id_column, xlap_enodeb_path, Enodeb_Pre_Feature_path, enodeb_date,enodeb_path): 
 
    snap_post_instance = SNAP_post_enodeb( sparksession = spark,
                                            date_str = date_str,   
                                            id_column = id_column,   
                                            xlap_enodeb_path = xlap_enodeb_path,  
                                            Enodeb_Pre_Feature_path = Enodeb_Pre_Feature_path,  
                                            enodeb_date = enodeb_date,  
                                            enodeb_path = enodeb_path) 
                 

    output_path = f"{hdfs_title}/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_Post_tickets_Feature_Date/{date_str}_tickets_Post_Feature_of_Enodeb/{date_str}_tickets_Post_feature_maintained_{enodeb_date}.csv" 

    snap_post_instance.result_df.repartition(1).write.csv(output_path, header=True, mode="overwrite") 

if __name__ == "__main__":

#----------------------------------------------------------------------------------------------------------------------------
    spark = SparkSession.builder\
            .appName('MonitorEnodebPef_Enodeb_Post')\
            .master("spark://njbbepapa1.nss.vzwnet.com:7077")\
            .config("spark.ui.port","24049")\
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=(date.today() - timedelta(1) ).strftime("%Y-%m-%d")) 
    args = parser.parse_args()
    date_str = args.date

    id_column = ['ENODEB']

    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'
    hdfs_caro = "hdfs://carovmaspd1.nss.vzwnet.com:9000"
    xlap_enodeb_path = hdfs_caro + "//SAS/snap/snap_4g/nokia_fsm_kpis_updated_v3/NokiaFSMKPIsSNAP_{}.csv"
    #xlap_enodeb_path = hdfs_title + '/user/rohitkovvuri/nokia_fsm_kpis_updated_v3/NokiaFSMKPIsSNAP_{}.csv'
    Enodeb_Pre_Feature_path = hdfs_title + "/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_Pre_Feature_Date/Event_Enodeb_Pre_{}.csv"
    enodeb_path = hdfs_title + "/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_List_Date/Event_Enodeb_List_{}.csv"
    
    previous_14_days = [(datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=day)).strftime("%Y-%m-%d") for day in range(15)]  

    import concurrent
    with concurrent.futures.ThreadPoolExecutor() as executor: 
        executor.map(lambda enodeb_date: post_enodeb(spark,
                                                     date_str,
                                                     id_column, 
                                                     xlap_enodeb_path, 
                                                     Enodeb_Pre_Feature_path, 
                                                     enodeb_date, 
                                                     enodeb_path), 
                                                                    previous_14_days) 
