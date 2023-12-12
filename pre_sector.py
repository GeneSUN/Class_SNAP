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
from class_SNAP import SNAP_pre_carrier, get_date_window

from hdfs import InsecureClient 

def pre_sector(spark, date_str, sector_source, sector_path_list, event_enodeb_path, id_column):

    SnapPreSector = SNAP_pre_carrier( 
        sparksession = spark,
        date_str = date_str, 
        id_column=id_column, 
        xlap_enodeb_path = sector_source,
        event_enodeb_path = event_enodeb_path    
    ) 

    dataframes_list = [ 
        (SnapPreSector.df_event_enodeb_daily_features, sector_path_list[0]), 
        (SnapPreSector.df_enodeb_stats, sector_path_list[1]) 
    ] 

    for df, output_path in dataframes_list: 
        df.repartition(1).write.csv(output_path, header=True, mode="overwrite")

if __name__ == "__main__":

#----------------------------------------------------------------------------------------------------------------------------
    spark = SparkSession.builder\
            .appName('MonitorEnodebPef_Sector_Pre')\
            .master("spark://njbbepapa1.nss.vzwnet.com:7077") \
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=(date.today() - timedelta(1) ).strftime("%Y-%m-%d")) 
    args = parser.parse_args()
    date_str = args.date

    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'
    sector_source = hdfs_title + "/user/rohitkovvuri/fsm_sector_kpis/fsmkpis_snap_sector_{}.csv"
    event_enodeb_path = hdfs_title+"/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_List_Date/Event_Enodeb_List_{}.csv"
    sector_path_list = ["/user/ZheS/MonitorEnodebPef/Sector/Daily_KPI_14_days_pre_Event/Daily_KPI_14_days_pre_{}.csv",
                "/user/ZheS/MonitorEnodebPef/Sector/Event_Enodeb_Pre_Feature_Date/Event_Enodeb_Pre_{}.csv"]
    sector_path_list = [ hdfs_title + path.format(date_str) for path in sector_path_list]
    id_column = ['ENODEB', "EUTRANCELL"]
    pre_sector(spark, date_str, sector_source, sector_path_list, event_enodeb_path, id_column)
