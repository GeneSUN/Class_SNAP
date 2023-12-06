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
from class_SNAP import SNAP_pre_enodeb, get_date_window
from MailSender import MailSender
from hdfs import InsecureClient 

def pre_enodeb(spark, date_str, sourse_path, path_list, id_column):

    SnapPreEnodeb = SNAP_pre_enodeb( 
        sparksession = spark,
        date_str=date_str, 
        id_column= id_column, 
        xlap_enodeb_path=sourse_path
    ) 

    dataframes_list = [ 
        (SnapPreEnodeb.df_event_enodeb, path_list[0]), 
        (SnapPreEnodeb.df_event_enodeb_daily_features, path_list[1]), 
        (SnapPreEnodeb.df_enodeb_stats, path_list[2]) 
    ] 

    for df, output_path in dataframes_list: 
        df.repartition(1).write.csv(output_path, header=True, mode="overwrite") 

if __name__ == "__main__":

#----------------------------------------------------------------------------------------------------------------------------
    spark = SparkSession.builder\
            .appName('MonitorEnodebPef_Enodeb_level')\
            .master("spark://njbbepapa1.nss.vzwnet.com:7077") \
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=datetime.today().strftime('%Y-%m-%d')) 
    args = parser.parse_args()
    date_str = args.date
    if date_str == "no date input from shell script":
        date_str = (date.today() - timedelta(1) ).strftime("%Y-%m-%d")

    mail_sender = MailSender() 
    mail_sender.send(text = date_str)
    """
    try:
        date_str = (date.today() - timedelta(1) ).strftime("%Y-%m-%d")
        #date_str = "2023-11-30"
        hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'
        sourse_path = hdfs_title + "/user/rohitkovvuri/nokia_fsm_kpis_updated_v3/NokiaFSMKPIsSNAP_{}.csv"
        path_list = ["/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_List_Date/Event_Enodeb_List_{}.csv",
                    "/user/ZheS/MonitorEnodebPef/enodeb//Daily_KPI_14_days_pre_Event/Daily_KPI_14_days_pre_{}.csv",
                    "/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_Pre_Feature_Date/Event_Enodeb_Pre_{}.csv"]
        path_list = [ hdfs_title + path.format(date_str) for path in path_list]
        id_column = ['ENODEB']

        pre_enodeb(spark,date_str,sourse_path, path_list, id_column)

        df_kpis = spark.read.option("header", "true").csv( path_list[0] )

    except Exception as e:
        print(e)
        mail_sender = MailSender() 
        mail_sender.send(send_from="sassupport@verizon.com", 
        send_to=["zhe.sun@verizonwireless.com"], 
        subject="insufficient Enodeb data", 
        cc=[], 
        text=f"an error occured at pre_enodeb code: {e}") 
    
    else:
        
        if df_kpis.count() < 1000:
            mail_sender = MailSender() 
            mail_sender.send(send_from="sassupport@verizon.com", 
            send_to=["zhe.sun@verizonwireless.com"], 
            subject="insufficient Enodeb data", 
            cc=[], 
            text=f"no enodeb list at {date_str}") 
    finally:
        pass

    """
