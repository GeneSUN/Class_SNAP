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

def get_date_window(start_date, days = 1, direction = "backward", formation ="str", end_time = None ): 
    from datetime import datetime, timedelta, date
    # Calculate the end date and start_date--------------------------------------
    
    start_date = datetime.strptime(start_date, "%Y-%m-%d")
    
    if end_time is None:
        if direction.lower() == 'forward': 
            end_date = start_date + timedelta(days=days)  
        elif direction.lower() == 'backward': 
            end_date = start_date - timedelta(days=days) 
        else: 
            raise ValueError("Invalid direction argument. Use 'forward' or 'backward'.")
    else:
        end_date = datetime.strptime(end_time, "%Y-%m-%d")
        if end_date > start_date:
            direction = "forward"
        else:
            direction = "backward"

    # Generate the date range and format them as strings -------------------------------
    date_list = []
    if direction.lower() == 'backward':
        while end_date <= start_date: 
            date_string = end_date.strftime("%Y-%m-%d") 
            date_list.insert(0, date_string)  # Insert at the beginning to make it descending 
            end_date += timedelta(days=1) 
                
    if direction.lower() == 'forward':
        while end_date >= start_date: 
            date_string = end_date.strftime("%Y-%m-%d") 
            date_list.insert(0, date_string)  # Insert at the beginning to make it descending 
            end_date -= timedelta(days=1) 
    
    if formation == "datetime":
        # Convert date strings to date objects using list comprehension 
        date_list = [datetime.strptime(date_string, "%Y-%m-%d").date() for date_string in date_list] 
    elif formation == "timestamp":
        # Convert date objects to timestamps using list comprehension
        date_list = [ datetime.timestamp(datetime.strptime(date_string, "%Y-%m-%d")) for date_string in date_list] 
    else:
        pass
    
    return date_list
def round_numeric_columns(df, decimal_places=2, numeric_columns = None): 

    if numeric_columns == None:
        numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == "double" or data_type == "float"]

    # Apply rounding to all numeric columns 
    for col_name in numeric_columns: 
        df = df.withColumn(col_name, round(df[col_name], decimal_places)) 
        
    return df


class SNAP():

    global hdfs_title, categorical_column, fsm_column_mapping, sea_column_mapping, distinct_column,fsm_s1u_features, fsm_features,\
    sea_features, sea_s1u_features, fsm_sea_features_list, features_list_inc, features_list_dec, features_list
    hdfs_title = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    categorical_column = ['DAY', 'MMEPOOL','REGION', 'MARKET', 'MARKET_DESC', 'SITE']
    fsm_column_mapping = { 
        "FSM_LTE_DataERABDrop%": "bearer_drop_rate", 
        "FSM_LTE_AvgRRCUsers": "avgconnected_users", 
        "FSM_LTE_DLRLCMBytes": "dl_data_volume", 
        "FSM_LTE_DL_BurstTputMbps": "uptp_user_perceived_throughput", 
        "S1U_SIP_SC_CallDrop%": "sip_dc_rate", 
        "FSM_LTE_RRCF%": "rrc_setup_failure",
        "RTP_Gap_Duration_Ratio_Avg%": "rtp_gap"
    } 

    sea_column_mapping = { 
        "SEA_AvgRRCconnectedUsers_percell": "avgconnected_users", 
        "SEA_TotalDLRLCLayerDataVolume_MB": "dl_data_volume", 
        "SEA_DLUserPerceivedTput_Mbps": "uptp_user_perceived_throughput", 
        "S1U_SIP_SC_CallDrop%": "sip_dc_rate", 
        "SEA_RRCSetupFailure_%": "rrc_setup_failure", 
        "SEA_DefaultERABDropRate_%": "bearer_drop_rate" ,
        "RTP_Gap_Duration_Ratio_Avg%": "rtp_gap"
    }
    features_list_inc = ['sip_dc_rate', 'rrc_setup_failure', 'bearer_drop_rate',"rtp_gap"]
    features_list_dec = ['avgconnected_users', 'dl_data_volume','uptp_user_perceived_throughput']
    distinct_column = ["RTP_Gap_Duration_Ratio_Avg%","S1U_SIP_SC_CallDrop%"]

    fsm_s1u_features= list( fsm_column_mapping.keys() ) # count "S1U_SIP_SC_CallDrop%"
    fsm_features = [feature for feature in fsm_s1u_features if feature not in distinct_column]
    sea_s1u_features = list( sea_column_mapping.keys() )
    sea_features = [feature for feature in sea_s1u_features if feature not in distinct_column]
    fsm_sea_features_list =  fsm_s1u_features + sea_features
    features_list = features_list_inc + features_list_dec

    def __init__(self, 
                 sparksession,
                 date_str, 
                 id_column, 
                ) -> None:
        self.spark = sparksession
        self.date_str = date_str
        self.id_column = id_column


    def union_df_list(self, df_list):   

        df_post = reduce(lambda df1, df2: df1.union(df2), df_list)

        return df_post.dropDuplicates()
    def process_csv_files(self, date_range, file_path_pattern, func = None): 

        """ 
        Reads CSV files from HDFS for the given date range and file path pattern and processes them.
        Args: 
            date_range (list): List of date strings in the format 'YYYY-MM-DD'. 
            file_path_pattern (str): File path pattern with a placeholder for the date, e.g., "/user/.../{}.csv"
        Returns: 
            list: A list of processed PySpark DataFrames. 
        """ 
        df_list = [] 
        for d in date_range: 
            file_path = file_path_pattern.format(d) 
            try:
                df_kpis = self.spark.read.option("header", "true").csv(file_path)
                
                if func is not None:
                    df_kpis = func(df_kpis)
                
                df_list.append(df_kpis)
            except Exception as e:
                print(e)
                #print(f"data missing at {file_path}")

        return df_list

    def preprocess_xlap(self, df, Strings_to_Numericals): 
        """ 
        Preprocesses the given DataFrame containing XLAP data. 
        This function performs the following operations: 
        1. Converts specified string-type columns to numerical values. 
        2. Removes duplicates from the DataFrame. 
        3. pad '0' if only 5 digits the 'ENODEB' column. (12345 to 012345)
        4. Converts the 'DAY' column from 'MM/dd/yyyy' format to 'yyyy-MM-dd'. 
        5. Selects the first element of duplicated 'SITE' values and retains unique records. 

        Args: 
            df (pyspark.sql.DataFrame): Input DataFrame containing XLAP data. 
            
        Returns: 
            pyspark.sql.DataFrame: Processed DataFrame after applying the necessary transformations. 
        """ 
        
        df = df.filter(~(F.col("ENODEB") == "*"))\
                .filter(F.col("ENODEB").isNotNull())\
                .withColumn('ENODEB', lpad(col('ENODEB'), 6, '0'))\
                .withColumn("DAY", F.to_date(F.col("DAY"), "MM/dd/yyyy"))\
                .withColumn("DAY", F.date_format(F.col("DAY"), "yyyy-MM-dd"))\
                .select([F.col(column).cast('double') if column in Strings_to_Numericals else F.col(column) for column in df.columns])\
                .dropDuplicates() 
        if "RTP_Gap_Duration_Ratio_Avg%" not in df.columns:
            df = df.withColumn("RTP_Gap_Duration_Ratio_Avg%", lit(0))
        # during data transmission, we have few duplicate data with different SITE
        # such as "NEW YORK CITY" and "NEW YORK C", which should be same
        column_name = df.columns 
        column_name.remove('SITE') 
        df = df.groupBy(column_name).agg(first('SITE').alias('SITE')).select(df.columns) 
    
        return df
    def rename_features(self, df, column_mapping): 

        for old_col, new_col in column_mapping.items(): 
            df = df.withColumnRenamed(old_col, new_col)
            
        return df

    def lower_case_col_names(self, df, lower_case_cols_list=None): 

        if lower_case_cols_list is None: 
            lower_case_cols_list = df.columns 

        for col_name in lower_case_cols_list: 
            df = df.withColumnRenamed(col_name, col_name.lower()) 

        return df 

    def fill_allday_zero_with_NA(self, df, features_list, groupby_columns):
        """ 
        Fill samples with all zero values in specified features with 'None' (NA) for non-enodeb columns.
        Parameters: 
            df (DataFrame): Input DataFrame containing the data.
            features_list (list): List of feature columns to consider for zero value check. 
            groupby_columns (list, optional): List of columns to group by. Default is ['day', 'enodeb', 'eutrancell'].
        Returns: 
            DataFrame: A new DataFrame with samples having all zero values in specified features replaced with 'None'. 
        """ 
        # step 1. find samples (enodeb and day) features values are all zero
        fill_zero_na_df = df.withColumn("FSM_result", reduce(add, [col(x) for x in features_list])).filter(col('FSM_result') == 0 ).select(df.columns)
        for column in features_list: 
            if column != "enodeb": 
                fill_zero_na_df = fill_zero_na_df.withColumn(column, lit(None)) 
                
        # step 2. remove null_samples from original dataframe
        df_without_null = df.join(fill_zero_na_df,groupby_columns, "left_anti").select(df.columns)
        
        # step 3. union two dataframe together
        df_return = df_without_null.union(fill_zero_na_df)
        
        return df_return

    def round_numeric_columns(self, df, decimal_places=2, numeric_columns = None): 

        if numeric_columns == None:
            numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == "double" or data_type == "float"]

        # Apply rounding to all numeric columns 
        for col_name in numeric_columns: 
            df = df.withColumn(col_name, round(df[col_name], decimal_places)) 
            
        return df

class SNAP_pre_enodeb(SNAP):
    
    def __init__(self,xlap_enodeb_path, *args, **kwargs): 
        super().__init__(*args, **kwargs)
        self.xlap_enodeb_path = xlap_enodeb_path

        self.df_xlap_14 = self.read_pre_14_days()
        self.df_event_enodeb = self.get_event_df()
        self.df_event_enodeb_daily_features = self.fsm_feature_daily() 
        self.df_enodeb_stats = self.get_enodeb_stats()

    def read_pre_14_days(self, date_str = None):
        
        if date_str is None:
            date_str = self.date_str
        date_range = get_date_window(date_str, days = 14, direction = 'backward')[:]
        df = self.process_csv_files(date_range,  self.xlap_enodeb_path)
        df = self.union_df_list(df)
        df = self.preprocess_xlap(df, fsm_sea_features_list)
        df = df.withColumn("nokia", reduce(add, [col(x) for x in fsm_features]))\
                .withColumn("samsung", reduce(add, [col(x) for x in sea_features]))
        return df
 
    def get_event_df(self, df = None, date_str = None):
        
        """ 
        Identify and return dataframe of 'ENODEB' values representing network nodes, and event date
        that experience change from Nokia to Samsung 
        Args: 
        df_kpis (DataFrame): A PySpark DataFrame containing network performance data. including enodeb, DAY, and FSM/SEA related features

        Returns: 
        dataframe: A dataframe with 'ENODEB' values that meet the criteria. 

        This function performs the following steps: 

        1. Filters network nodes where the sum of Samsung features is 0. which means the enodeb does not have maintenance the entire time window. 

        2. Calculates the remaining network nodes by subtracting excluded nodes from the total nodes. 
        
        3. 
        """ 
        if df is None:
            df = self.df_xlap_14
        if date_str is None:
            date_str = self.date_str
        #----------------------------------------------------------------------------------
        # step 1. Filter 'ENODEB' values where the samsung features of 14 days is 0,(this is redundant but quick reduce df size)
        #         Filter 'ENODEB' values where the nokia features of 14 days is 0, 
        
        # Sum up all samsung features across all days of each ENODEB 
        sum_df = df.groupBy("ENODEB").agg(
                                    sum('nokia').alias("sum_nokia"),
                                    sum('samsung').alias("sum_samsung")
                                    ) 
        
        exclude_no_samsung = sum_df.filter(col("sum_samsung") == 0).select("ENODEB")# second excluded enodeb list
        exclude_no_nokia = sum_df.filter(col("sum_nokia") == 0).select("ENODEB")# third excluded enodeb list
        
        exclude_enodeb =  exclude_no_samsung.union(exclude_no_nokia)
        remain_enodeb_df = df.join( exclude_enodeb, "ENODEB", 'left_anti' )
        #---------------------------------------------------------------------------------
        # step 2. filter row whose first samsung is not date_before_td
        window_spec = Window.partitionBy("ENODEB").orderBy("DAY")
        df_event_first_samsung = remain_enodeb_df.withColumn( 
            "first_positive_samsung_day", 
            first(when(col("samsung") > 0, col("DAY")), ignorenulls=True).over(window_spec) 
        )
            # explain
            #positive_samsung_day_column = when(col("samsung") > 0, col("DAY")).otherwise(None).alias("positive_samsung_day") 
            #df1 = df.withColumn("positive_samsung_day", positive_samsung_day_column) 
            #df1.show()
            #df2 = df1.withColumn("first_positive_samsung_day", first( 'positive_samsung_day' , ignorenulls=True).over(window_spec) ) 
            #df2.show()
        
        return_df = df_event_first_samsung\
                        .filter( col("first_positive_samsung_day")==date_str )\
                        .withColumnRenamed("first_positive_samsung_day","event_date" )
        
        return return_df

    def fsm_feature_daily(self, df_event_enodeb = None, df_xlap_14 =None, date_str =None, id_column = None, \
                          ):

        if df_event_enodeb is None:
            df_event_enodeb = self.df_event_enodeb
        if df_xlap_14 is None:
            df_xlap_14 = self.df_xlap_14
        if date_str is None:
            date_str = self.date_str
        if id_column is None:
            id_column = self.id_column

        broadcast_df = broadcast(df_event_enodeb.select('ENODEB',"event_date") ) 
        df_event_enodeb_daily_features = df_xlap_14.join(broadcast_df, 'ENODEB', "inner")\
                                                        .filter( col('DAY')!=date_str )\
                                                        .select(categorical_column + id_column + fsm_s1u_features+ ["event_date"])

        df_event_enodeb_daily_features = self.rename_features(df_event_enodeb_daily_features, fsm_column_mapping)
        df_event_enodeb_daily_features = self.lower_case_col_names(df_event_enodeb_daily_features)

        features_without_s1u = [e for e in features_list if e not in distinct_column]
        df_event_enodeb_daily_features = self.fill_allday_zero_with_NA(df_event_enodeb_daily_features, features_without_s1u, ["day"] + id_column) 
        return df_event_enodeb_daily_features


    def get_enodeb_stats(self,df_event_enodeb_daily_features = None, id_column =None):
        if df_event_enodeb_daily_features is None:
            df_event_enodeb_daily_features = self.df_event_enodeb_daily_features
        if id_column is None:
            id_column = self.id_column
            # def enodeb_function(df,  features, groupby_feature, aggregate_functions = F.avg): 
        df_enodeb_std = self.stats_features( df_event_enodeb_daily_features,features_list, id_column, aggregate_functions = F.stddev)
        df_enodeb_avg = self.stats_features( df_event_enodeb_daily_features,features_list, id_column, aggregate_functions = F.avg)

        df_enodeb_stats = df_enodeb_avg.join(df_enodeb_std, on = id_column, how = "inner")
        return df_enodeb_stats

    def stats_features(self, df,  features, groupby_feature, aggregate_functions ): 
        """ 
        Calculates the statistics of specified features for each unique groupby_feature in a PySpark DataFrame. 
        
        Parameters: 
            df (DataFrame): The PySpark DataFrame containing the data. 
            features (list, optional): A list of feature column names to calculate the statistics for. 
    
        Returns: 
            DataFrame: A new PySpark DataFrame with the mean values of the specified features for each ENODEB. 
        """ 

        # Create expressions to calculate the statistics for features 
        expressions = [aggregate_functions(F.col(col_name)).alias(col_name) for col_name in features] 
        
        df_agg_features = df.groupBy(groupby_feature).agg(*expressions)
        
        df_agg_features = round_numeric_columns(df_agg_features, decimal_places=2) 
        
        columns_to_rename = [item for item in df_agg_features.columns if item not in groupby_feature]

        # Rename except enodeb 
        for column in columns_to_rename:
            df_agg_features = df_agg_features.withColumnRenamed(column, column +"_" + (aggregate_functions.__name__)[:3] ) 
        
        return df_agg_features

class SNAP_pre_carrier(SNAP_pre_enodeb):
    def __init__(self,event_enodeb_path, *args, **kwargs): 
        self.event_enodeb_path = event_enodeb_path
        super().__init__(*args, **kwargs)
    
    def read_pre_14_days(self, date_str = None):
        
        if date_str is None:
            date_str = self.date_str
        date_range = get_date_window(date_str, days = 14, direction = 'backward')[:]
        df = self.process_csv_files(date_range,  self.xlap_enodeb_path)
        df = self.union_df_list(df).filter( col("EUTRANCELL") != '0' )
        df = self.preprocess_xlap(df, fsm_sea_features_list)
        df = df.withColumn("nokia", reduce(add, [col(x) for x in fsm_features]))\
                .withColumn("samsung", reduce(add, [col(x) for x in sea_features]))
        return df
    
    def get_event_df(self, event_enodeb_path = None, date_str = None):

        if event_enodeb_path is None:
            event_enodeb_path = self.event_enodeb_path
        if date_str is None:
            date_str = self.date_str
        try:
            df = self.spark.read.option("header", "true").csv(event_enodeb_path.format(date_str))
        except Exception as e:
            if datetime.strptime(date_str, "%Y-%m-%d").weekday() < 5 or date_str in ["2023-11-27","2023-11-24", "2023-11-23", "2023-12-25"] :
                pass
            else:    
                print(e, f"missing event enodeb list at {date_str}")
        return df

class SNAP_post(SNAP):
    global avg_features_list, std_features_list, change_rate_features_list, \
    threshold_std_features_list, threshold_avg_features_list, threshold_max_features_list, alert_features_list

    avg_features_list = [f"{feature}_avg" for feature in features_list] 
    std_features_list = [f"{feature}_std" for feature in features_list] 
    change_rate_features_list = [f"{feature}_change_rate" for feature in features_list] 
    threshold_std_features_list = [f"{feature}_threshold_std" for feature in features_list] 
    threshold_avg_features_list = [f"{feature}_threshold_avg" for feature in features_list] 
    threshold_max_features_list = [f"{feature}_threshold_max" for feature in features_list] 
    alert_features_list = [f"{feature}_alert" for feature in features_list] 

    def __init__(self,xlap_enodeb_path, Enodeb_Pre_Feature_path, enodeb_date, enodeb_path,*args, **kwargs): 
        super().__init__(*args, **kwargs)
        self.xlap_enodeb_path = xlap_enodeb_path
        self.Enodeb_Pre_Feature_path = Enodeb_Pre_Feature_path
        self.enodeb_date = enodeb_date
        self.enodeb_path = enodeb_path

        self.df_xlap = self.preprocess_xlap( self.spark.read.option("header", "true")
                                            .csv(self.xlap_enodeb_path.format(self.date_str) ), fsm_sea_features_list)
        self.df_enodeb = self.spark.read.option("header", "true").csv(self.enodeb_path.format(self.enodeb_date))
        self.df_xlap_pre_stas = self.spark.read.option("header", "true")\
                                        .csv( self.Enodeb_Pre_Feature_path.format(self.enodeb_date) )
        self.df_pre_post = self.get_post_feature()
        self.df_enb_cord = self.get_enb_cord()

    def patch1(self, result_df, enodeb_date = None):
        if enodeb_date is None:
            enodeb_date = self.enodeb_date
        # patch: for data with all features as zero, change its has_abnormal_kpi to 0.
        all_zero_condition = reduce(lambda x, y: x & (col(y) == 0), features_list, lit(True)) 
        result_df = result_df.withColumn("has_abnormal_kpi", when(all_zero_condition, 0).otherwise(col("has_abnormal_kpi")))\
                            .withColumn("event_day", lit(enodeb_date) )

        return result_df
    
    def get_enb_cord(self, date_str = None):
        if date_str is None:
            date_str = self.date_str
        try:
            oracle_file = f"hdfs://njbbepapa1.nss.vzwnet.com:9000/fwa/atoll_oracle_daily/date={date_str}"
            df_enb_cord = self.spark.read.format("com.databricks.spark.csv").option("header", "True").load(oracle_file)\
                                .filter(F.col("LATITUDE_DEGREES_NAD83").isNotNull())\
                                .filter(F.col("LONGITUDE_DEGREES_NAD83").isNotNull())\
                                .filter(F.col("ENODEB_ID").isNotNull())\
                                .groupby("ENODEB_ID","LATITUDE_DEGREES_NAD83","LONGITUDE_DEGREES_NAD83")\
                                .count()\
                                .select( col('ENODEB_ID').alias('ENODEB'), 
                                        F.col('LATITUDE_DEGREES_NAD83').cast("double").alias('LATITUDE'), 
                                        F.col('LONGITUDE_DEGREES_NAD83').cast("double").alias('LONGITUDE'))\
                                .withColumn('ENODEB', lpad(col('ENODEB'), 6, '0'))\
                                .dropDuplicates( ['ENODEB'] )
        except:
            oracle_file = f"hdfs://njbbepapa1.nss.vzwnet.com:9000/fwa/atoll_oracle_daily/date=2023-11-15"
            df_enb_cord = self.spark.read.format("com.databricks.spark.csv").option("header", "True").load(oracle_file)\
                                .filter(F.col("LATITUDE_DEGREES_NAD83").isNotNull())\
                                .filter(F.col("LONGITUDE_DEGREES_NAD83").isNotNull())\
                                .filter(F.col("ENODEB_ID").isNotNull())\
                                .groupby("ENODEB_ID","LATITUDE_DEGREES_NAD83","LONGITUDE_DEGREES_NAD83")\
                                .count()\
                                .select( col('ENODEB_ID').alias('ENODEB'), 
                                        F.col('LATITUDE_DEGREES_NAD83').cast("double").alias('LATITUDE'), 
                                        F.col('LONGITUDE_DEGREES_NAD83').cast("double").alias('LONGITUDE'))\
                                .withColumn('ENODEB', lpad(col('ENODEB'), 6, '0'))\
                                .dropDuplicates( ['ENODEB'] )
        return df_enb_cord

    def get_post_feature(self,  df_enodeb = None, df_xlap = None, id_column = None, df_xlap_pre_stas = None):
        # get post feature for enodeb maintained 14 days ahead
        if df_enodeb is None:
            df_enodeb = self.df_enodeb
        if df_xlap is None:
            df_xlap = self.df_xlap
        if id_column is None:
            id_column = self.id_column
        if df_xlap_pre_stas is None:
            df_xlap_pre_stas = self.df_xlap_pre_stas

        df_post = df_xlap.join( df_enodeb.select("enodeb"), "enodeb" )\
                                .select(categorical_column + id_column + sea_s1u_features)
        
        df_post = self.fill_allday_zero_with_NA(df_post, sea_features, id_column)
        df_post = self.rename_features(df_post, sea_column_mapping)
        
        

        df_pre_post = self.calculate_metrics(df_post, df_xlap_pre_stas, id_column)

        exclude_columns =  threshold_avg_features_list + threshold_std_features_list + threshold_max_features_list
        selected_columns = [column for column in df_pre_post.columns if column not in exclude_columns]
        df_pre_post = self.round_numeric_columns( df_pre_post.select(selected_columns) )
    
        return df_pre_post

    def calculate_metrics(self, df_post_feature_event_j, df_xlap_pre_stas, groupby_features):
        
        df_pre_post = df_xlap_pre_stas.join(df_post_feature_event_j, on = groupby_features, how = 'inner' )
        
        for feature in features_list_inc: 
            avg_feature = f"{feature}_avg"
            std_feature = f"{feature}_std"
            change_rate_column_name = f"{feature}_change_rate" 
            df_pre_post = df_pre_post.withColumn(change_rate_column_name, ( col(avg_feature) - col(feature) ) / col(avg_feature)) 
            df_pre_post = df_pre_post.withColumn(change_rate_column_name, when(col(change_rate_column_name) < -1, -1).otherwise(col(change_rate_column_name))) 
            
            threshold_avg = f"{feature}_threshold_avg" 
            df_pre_post = df_pre_post.withColumn(threshold_avg, (col(avg_feature) + 0.1*col(avg_feature)) ) 
        
            threshold_std = f"{feature}_threshold_std" 
            df_pre_post = df_pre_post.withColumn(threshold_std, (col(avg_feature) + 2*col(std_feature)) )
            
            threshold_max = f"{feature}_threshold_max"
            df_pre_post = df_pre_post.withColumn(threshold_max, when(col(threshold_std) > col(threshold_avg), col(threshold_std)).otherwise(col(threshold_avg)))
            
            alert_feature = f"{feature}_alert"
            df_pre_post = df_pre_post.withColumn(alert_feature, when( col(threshold_max) < col(feature), 1).otherwise(0) )

        for feature in features_list_dec: 
            avg_feature = f"{feature}_avg"
            std_feature = f"{feature}_std"
            change_rate_column_name = f"{feature}_change_rate" 
            df_pre_post = df_pre_post.withColumn(change_rate_column_name, (col(feature) - col(avg_feature)) / col(avg_feature)) 
            df_pre_post = df_pre_post.withColumn(change_rate_column_name, when(col(change_rate_column_name) > 1, 1).otherwise(col(change_rate_column_name))) 
            
            threshold_avg = f"{feature}_threshold_avg" 
            df_pre_post = df_pre_post.withColumn(threshold_avg, (col(avg_feature) - 0.1*col(avg_feature)) ) 
        
            threshold_std = f"{feature}_threshold_std" 
            df_pre_post = df_pre_post.withColumn(threshold_std, (col(avg_feature) - 2*col(std_feature)) )
            
            threshold_max = f"{feature}_threshold_max"
            df_pre_post = df_pre_post.withColumn(threshold_max, when(col(threshold_std) < col(threshold_avg), col(threshold_std)).otherwise(col(threshold_avg)))
            
            alert_feature = f"{feature}_alert"
            df_pre_post = df_pre_post.withColumn(alert_feature, when( col(threshold_max) > col(feature), 1).otherwise(0) )

    #--------------------------------------------------------------------------------------------
        df_pre_post = df_pre_post.withColumn("avgconnected_users_alert", lit(0))
    #--------------------------------------------------------------------------------------------    
        alert_list = [f"{feature}_alert" for feature in features_list]
        
        df_pre_post = df_pre_post.withColumn("abnormal_kpi", reduce(add, [col(x) for x in alert_list])  )
        df_pre_post = df_pre_post.withColumn('has_abnormal_kpi', when(df_pre_post['abnormal_kpi'] > 0, 1).otherwise(0))

        return df_pre_post     
       
class SNAP_post_enodeb(SNAP_post):
    def __init__(self,*args, **kwargs): 
        super().__init__(*args, **kwargs)
        self.df_enb_tickets_nrb = self.get_enodeb_tickets_nrb()
        self.df_w360_tickets = self.get_enodeb_tickets_w360()
        self.result_df = self.patch1( self.join_df() )
    
    
    def patch1(self, result_df, enodeb_date = None):
        if enodeb_date is None:
            enodeb_date = self.enodeb_date
        # patch: for data with all features as zero, change its has_abnormal_kpi to 0.
        all_zero_condition = reduce(lambda x, y: x & (col(y) == 0), features_list, lit(True)) 
        result_df = result_df.withColumn("has_abnormal_kpi", when(all_zero_condition, 0).otherwise(col("has_abnormal_kpi")))\
                            .withColumn("event_day", lit(enodeb_date) )

        return result_df

    def join_df(self, df_enb_cord = None, df_pre_post = None, df_enb_tickets_nrb = None, df_w360_tickets = None):
        if df_enb_cord is None:
            df_enb_cord = self.df_enb_cord
        if df_pre_post is None:
            df_pre_post = self.df_pre_post
        if df_enb_tickets_nrb is None:
            df_enb_tickets_nrb = self.df_enb_tickets_nrb
        if df_w360_tickets is None:
            df_w360_tickets = self.df_w360_tickets
        joined_df = ( 
            df_enb_cord 
            .join(broadcast(df_pre_post), 'ENODEB', 'right') 
            .join(broadcast(df_enb_tickets_nrb), 'ENODEB', 'left') 
            .join(broadcast(df_w360_tickets), 'ENODEB', 'left') 
        )
        joined_df = self.round_numeric_columns(joined_df,decimal_places=4)
        joined_df = self.lower_case_col_names( joined_df)
        return joined_df
    
    def get_enodeb_tickets_nrb(self, df_enb_list = None, date_str = None, enodeb_date = None, df_enb_cord = None):
        
        if df_enb_list is None:
            df_enb_list = self.df_enodeb
        if date_str is None:
            date_str = self.date_str
        if enodeb_date is None:
            enodeb_date = self.enodeb_date
        if df_enb_cord is None:
            df_enb_cord = self.df_enb_cord

        def haversine(lat2,lon2, lat1, lon1):
            if lat2 is not None and lat1 is not None and lon1 is not None and lon2 is not None :
                """
                Calculate the great circle distance in kilometers between two points 
                on the earth (specified in decimal degrees)
                """
                # meter per sce to mph
                # convert decimal degrees to radians 
                lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
            
                # haversine formula 
                dlon = lon2 - lon1 
                dlat = lat2 - lat1 
                a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
                c = 2 * asin(sqrt(a)) 
                r = 6371.7 # Radius of earth in kilometers. Use 3956 for miles. Determines return value units.
                return c * r
            else:
                return 1000.0
        get_haversine_udf = F.udf(haversine, DoubleType()) 
        
        df_enb_list = df_enb_list.withColumn('event_date_final',lit(enodeb_date) )\
                                        .withColumnRenamed("ENODEB","enodeb_event")
        
        window_recentTickets=Window().partitionBy("trouble_id","status").orderBy(F.desc("MODIFIED_DATE"),F.desc("create_date_nrb"),F.desc("NRB_ASSIGNED_DATE"),F.desc("lat"),F.desc("lng"))
        window_dist=Window().partitionBy("trouble_id","status").orderBy("distance_from_enb")
        
        tickets_path='hdfs://njbbepapa1.nss.vzwnet.com:9000/user/kovvuve/epa_tickets/epa_tickets_{}-*.csv.gz'

        df_tickets = self.spark.read.option("header","true").option("delimiter", "|}{|")\
                                .csv(tickets_path.format(date_str))\
                            .dropDuplicates()\
                            .filter(F.col("lat").isNotNull())\
                            .filter(F.col("lng").isNotNull())\
                            .filter( F.col("copy_group_assigned") == "NRB" )\
                            .withColumnRenamed("COPY_OF_STATUS", "status")\
                            .withColumn("unix_date_mdified",
                                        F.from_unixtime("modified_unix_time",
                                        "MM-dd-yyyy"))\
                            .select( 'trouble_id', 'status', 
                                    F.to_date('NRB_ASSIGNED_DATE').alias("NRB_ASSIGNED_DATE"), 
                                    F.to_date('create_date').alias("create_date_nrb"), 
                                    F.to_date('MODIFIED_DATE').alias("MODIFIED_DATE"), 'lat', 'lng')\
                            .dropDuplicates()\
                            .withColumn("recent_tickets_row",F.row_number().over(window_recentTickets))\
                            .filter(F.col("recent_tickets_row")==1)\
                            .withColumn("id_lat",F.col("lat").cast("double"))\
                            .withColumn("id_lng",F.col("lng").cast("double"))\
                            .withColumn("id_lat",(F.col("lat")*F.lit(1.0)).cast("int"))\
                            .withColumn("id_lng",(F.col("lng")*F.lit(1.0)).cast("int"))\
                            .select("trouble_id","status","create_date_nrb",
                                        F.col("lat").cast("double"),
                                        F.col("lng").cast("double"))
                                    
        df_tickets_agg =  df_tickets.groupby("trouble_id","status","create_date_nrb","lat","lng").count()
        df_tickets_agg.cache()
        df_tickets_agg.count()
        
        df_tickets_open =df_tickets_agg.filter(F.lower(F.col("status"))=="open")
        df_tickets_notopen =df_tickets_agg.filter(~(F.lower(F.col("status"))=="open"))
        
        df_tickets_2 = df_tickets_open.join(df_tickets_notopen,
                                            df_tickets_notopen.trouble_id == df_tickets_open.trouble_id,
                                            "left_anti")\
                                    .select(df_tickets_open['*'])\
                                    .filter(F.col("lat").isNotNull() & F.col("lng").isNotNull())
        df_enb_comb =df_enb_cord.join(broadcast(df_enb_list),
                                        F.lpad(df_enb_list.enodeb_event,6,'0') ==F.lpad(df_enb_cord.ENODEB,6,'0'),"right")
        df_enb_comb = df_tickets_2.crossJoin(broadcast(df_enb_comb))\
                                .withColumn("includeTicket",
                                            F.when(F.col("create_date_nrb")>=F.to_date("event_date_final"),
                                            F.lit("Y")).otherwise(F.lit("N")))\
                                .filter(F.col("includeTicket")=="Y")
        #--------------------------------------------------------------------------------------------------
        # cross this logic once again..
        df_enb_comb_dist = df_enb_comb\
                            .withColumn("distance_from_enb",
                                        get_haversine_udf(df_enb_comb.LATITUDE,
                                                            df_enb_comb.LONGITUDE,df_enb_comb.lat,
                                                            df_enb_comb.lng).cast("double")/F.lit(1.60934))\
                            .filter(F.col("distance_from_enb")<=5.0)\
                            .withColumn("trouble_id_row",F.row_number().over(window_dist))\
                            .filter(F.col("trouble_id_row")==1)\
                            .filter(F.lower(F.col("status"))=="open")\
                            .withColumn("po_box_address",F.lit(0))\
                            .withColumn("ticket_source",F.lit("nrb"))\
                            .select("enodeb_event",
                                    F.col("LATITUDE").alias("enb_lat"),
                                    F.col("LONGITUDE").alias('enb_lng'),
                                    "trouble_id","event_date_final",
                                    F.col("create_date_nrb").alias("create_date"),
                                    "po_box_address","ticket_source")\
                            .sort("enodeb_event","event_date_final","create_date")
                            
        df_enb_tickets = df_enb_comb_dist.groupby("enodeb_event")\
                                        .agg(F.count("trouble_id").alias('nrb_ticket_counts'))\
                                        .select(F.col('enodeb_event').alias("ENODEB"), 
                                                F.col('nrb_ticket_counts') )
                                        

        return df_enb_tickets
    
    def get_enodeb_tickets_w360(self, df_enb_list = None, date_start = None, enodeb_date=None):
        if df_enb_list is None:
            df_enb_list = self.df_enodeb
        if date_start is None:
            date_start = self.date_str
        if enodeb_date is None:
            enodeb_date = self.enodeb_date
        
        date1 = datetime.strptime(date_start, "%Y-%m-%d") 
        date2 = datetime.strptime(enodeb_date, "%Y-%m-%d") 
        daysBack = (date1 - date2).days + 1

        df_enb_list = df_enb_list.withColumn('event_date_final',lit(enodeb_date) )\
                                .withColumnRenamed("ENODEB","enodeb_event")\
                                .groupby("enodeb_event")\
                                .agg(F.first("event_date_final").alias("event_date_final"))
                                
        window_recentTickets=Window().partitionBy('NOC_TT_ID', 'status')\
                                    .orderBy(F.desc("event_start_date"),
                                            F.desc("event_end_date"),
                                            F.desc("lat_enb"),
                                            F.desc("lng_enb"))
        
        df_kpis_w360 = self.spark.read.option("header","true")\
                        .csv("hdfs://njbbepapa1.nss.vzwnet.com:9000//fwa/workorder_oracle/DT={}".format(date_start))\
                        .dropDuplicates().withColumn("data_date",F.lit(date_start))

        for idate in range(1,daysBack):
            date_val = get_date_window(date_start,idate)[-1]
            
            try:
                df_temp_kpi = self.spark.read.option("header","true")\
                                .csv("hdfs://njbbepapa1.nss.vzwnet.com:9000//fwa/workorder_oracle/DT={}".format(date_val))\
                                .dropDuplicates()\
                                .withColumn("data_date",F.lit(date_val))
                df_kpis_w360 = df_kpis_w360.union(df_temp_kpi.select(df_kpis_w360.columns))
                
            except:
                print("data missing for {}".format(date_val))
                
        df_tickets_w360 =  df_kpis_w360.withColumnRenamed("STATUS", "status")\
                        .withColumn("event_start_date",F.to_date(F.substring(F.col("EVENT_START_DATE"),1,10)))\
                        .withColumn("event_end_date",F.to_date(F.substring(F.col("EVENT_END_DATE"),1,10)))\
                        .withColumn("CELL_ID",F.col("CELL__"))\
                        .withColumn("enb_id_w360",F.when(F.length("CELL_ID")>8,F.expr("substring(CELL_ID,1,length(CELL_ID)-4)") ).otherwise(F.col("CELL_ID")) )\
                        .filter(F.col("CELL_ID").isNotNull())\
                        .filter(F.lower(F.col("status")).isin("open"))\
                        .select( 'NOC_TT_ID', 'status',"enb_id_w360","CELL_ID","event_start_date","event_end_date", F.col('LAT').alias("lat_enb"), F.col('LONG_X').alias("lng_enb"))\
                        .dropDuplicates()\
                        .withColumn("recent_tickets_row",F.row_number().over(window_recentTickets))\
                        .filter(F.col("recent_tickets_row")==1)\
                        .select('NOC_TT_ID', 'status',"enb_id_w360","CELL_ID","event_start_date","event_end_date",F.col("lat_enb").cast("double"),F.col("lng_enb").cast("double"))
        
        df_tickets_agg =  df_tickets_w360.groupby('NOC_TT_ID', 'status',"enb_id_w360","event_start_date","event_end_date","lat_enb","lng_enb").count()
        
        df_w360 = df_enb_list.join(df_tickets_agg,F.lpad(df_enb_list.enodeb_event,6,'0') ==F.lpad(df_tickets_agg.enb_id_w360,6,'0'))\
                                .withColumn("includeTicket",F.when(F.col("event_start_date")>=F.to_date("event_date_final"),F.lit("Y")).otherwise(F.lit("N"))).filter(F.col("includeTicket")=="Y")\
                                .withColumn("ticket_source",F.lit("w360"))\
                                .select("enodeb_event",F.col("lat_enb").alias("enb_lat"),F.col("lng_enb").alias('enb_lng'),F.col("NOC_TT_ID").alias("trouble_id"),"event_date_final",F.col("event_start_date").alias("create_date"),"ticket_source")\
                                .sort("enodeb_event","event_date_final","create_date")
        df_w360_tickets = df_w360.groupby("enodeb_event")\
                                .agg(F.count("ticket_source").alias('w360_ticket_counts'))\
                                .select(F.col('enodeb_event').alias("ENODEB"),F.col('w360_ticket_counts') )
        return df_w360_tickets

class SNAP_post_carrier(SNAP_post):
    def __init__(self,*args, **kwargs): 
        super().__init__(*args, **kwargs)

        self.df_add = self.get_df_add()
        self.result_df = self.patch2( self.patch1( self.join_df() ) )
    
    def patch2(self,result_df):
        result_df = result_df.fillna({"rtp_gap_change_rate": 0})\
                             .filter( col("EUTRANCELL") != '0' )\
                             .dropDuplicates()
        return result_df
        
    def get_df_add(self, date_str = None, id_column = None):
        if date_str is None:
            date_str = self.date_str
        if id_column is None:
            id_column = self.id_column
        
        
        try:
            oracle_file = f"hdfs://njbbepapa1.nss.vzwnet.com:9000/fwa/atoll_oracle_daily/date={date_str}"
            
            if id_column == ['ENODEB','EUTRANCELL','CARRIER']:
                df_add = self.spark.read.option("header","true").csv(oracle_file)\
                                .select( 
                                            col('ENODEB_ID').alias(id_column[0]), 
                                            col('SECTOR').alias(id_column[1]),
                                            col('CARRIER_NUMBER').alias(id_column[2]),
                                            col('AZIMUTH_DEG')
                                        )\
                                .withColumn('ENODEB', lpad(col('ENODEB'), 6, '0'))
            elif id_column == ['ENODEB','EUTRANCELL']:
                df_add = self.spark.read.option("header","true").csv(oracle_file)\
                            .select( 
                                        col('ENODEB_ID').alias(id_column[0]), 
                                        col('SECTOR').alias(id_column[1]),
                                        col('AZIMUTH_DEG')
                                    )\
                            .withColumn('ENODEB', lpad(col('ENODEB'), 6, '0'))
            return df_add
        except:
            oracle_file = f"hdfs://njbbepapa1.nss.vzwnet.com:9000/fwa/atoll_oracle_daily/date=2023-11-15"
            if id_column == ['ENODEB','EUTRANCELL','CARRIER']:
                df_add = self.spark.read.option("header","true").csv(oracle_file)\
                                .select( 
                                            col('ENODEB_ID').alias(id_column[0]), 
                                            col('SECTOR').alias(id_column[1]),
                                            col('CARRIER_NUMBER').alias(id_column[2]),
                                            col('AZIMUTH_DEG')
                                        )\
                                .withColumn('ENODEB', lpad(col('ENODEB'), 6, '0'))
            elif id_column == ['ENODEB','EUTRANCELL']:
                df_add = self.spark.read.option("header","true").csv(oracle_file)\
                            .select( 
                                        col('ENODEB_ID').alias(id_column[0]), 
                                        col('SECTOR').alias(id_column[1]),
                                        col('AZIMUTH_DEG')
                                    )\
                            .withColumn('ENODEB', lpad(col('ENODEB'), 6, '0'))
            return df_add
        
    def join_df(self, df_add = None, df_pre_post = None, id_column = None):
        if df_add is None:
            df_add = self.df_add
        if df_pre_post is None:
            df_pre_post = self.df_pre_post
        if id_column is None:
            id_column = self.id_column
            
        joined_df = ( 
            df_add 
            .join(broadcast(df_pre_post), id_column, 'right') 
        )
        joined_df = self.round_numeric_columns(joined_df,decimal_places=4)
        joined_df = self.lower_case_col_names( joined_df)
        return joined_df

	
if __name__ == "__main__":
    spark = SparkSession.builder\
            .appName('MonitorEnodebPef_Enodeb_level')\
            .master("spark://njbbepapa1.nss.vzwnet.com:7077") \
            .config("spark.sql.adapative.enabled","true")\
            .enableHiveSupport().getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")
