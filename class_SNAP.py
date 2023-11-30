
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 
from pyspark.sql.functions import ( 
    abs, avg, broadcast, col, concat ,concat_ws, countDistinct, exp, expr, explode, first, from_unixtime, 
    lpad, length, lit, max, min, rand, regexp_replace, round, sum, to_date, udf, when, 
) 
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

def get_date_window(start_date, end_time = None,  days = 1, direction = "forward", formation ="str"): 
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
                 date_str, 
                 id_column, 
                ) -> None:
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
                df_kpis = spark.read.option("header", "true").csv(file_path)
                
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

        features_without_s1u = [e for e in features_list if e != "sip_dc_rate"]
        df_event_enodeb_daily_features = self.fill_allday_zero_with_NA(df_event_enodeb_daily_features, features_without_s1u, ["day"] + id_column) 
        return df_event_enodeb_daily_features
        df_event_enodeb_daily_features.repartition(1).write.csv(daily_outputpath.format(date_before_td), header=True, mode="overwrite") 

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




if __name__ == "__main__":
    spark = SparkSession.builder.appName('MonitorEnodebPef_Enodeb_level').config("spark.sql.adapative.enabled","true").enableHiveSupport().getOrCreate()
    
    date_str = "2023-11-28"

    hdfs_title = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'
    sourse_path = hdfs_title + "/user/rohitkovvuri/nokia_fsm_kpis_updated_v3/NokiaFSMKPIsSNAP_{}.csv"
    path_list = ["/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_List_Date/Event_Enodeb_List_{}.csv",
                "/user/ZheS/MonitorEnodebPef/enodeb//Daily_KPI_14_days_pre_Event/Daily_KPI_14_days_pre_{}.csv",
                "/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_Pre_Feature_Date/Event_Enodeb_Pre_{}.csv"]
    path_list = [ hdfs_title + path.format(date_str) for path in path_list]

    SnapPreEnodeb = SNAP_pre_enodeb( 
        date_str=date_str, 
        id_column=['ENODEB'], 
        xlap_enodeb_path=sourse_path
    ) 

    dataframes_list = [ 
        (SnapPreEnodeb.df_event_enodeb, path_list[0]), 
        (SnapPreEnodeb.df_event_enodeb_daily_features, path_list[1]), 
        (SnapPreEnodeb.df_enodeb_stats, path_list[2]) 
    ] 

    for df, output_path in dataframes_list: 
        df.repartition(1).write.csv(output_path, header=True, mode="overwrite") 

    avg_features_list = [f"{feature}_avg" for feature in features_list] 
    std_features_list = [f"{feature}_std" for feature in features_list] 
    change_rate_features_list = [f"{feature}_change_rate" for feature in features_list] 
    threshold_std_features_list = [f"{feature}_threshold_std" for feature in features_list] 
    threshold_avg_features_list = [f"{feature}_threshold_avg" for feature in features_list] 
    threshold_max_features_list = [f"{feature}_threshold_max" for feature in features_list] 
    alert_features_list = [f"{feature}_alert" for feature in features_list] 