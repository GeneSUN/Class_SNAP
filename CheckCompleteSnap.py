import argparse 
import requests 
import pandas as pd 
import json
from datetime import timedelta, date , datetime
import argparse 
import requests 
import pandas as pd 
import json

import smtplib
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.utils import COMMASPACE, formatdate
from ftplib import FTP
import pandas as pd

from hdfs import InsecureClient 
import os
import re 

class ClassDailyCheckBase:
    global neglect_days
    neglect_days = ["2023-11-23", "2023-11-24","2023-11-27", "2023-12-25","2023-12-26", "2023-11-14","2024-01-01","2024-01-02","2024-01-15"]
    def __init__(self, name, expect_delay):
        self.name = name
        self.expect_delay = expect_delay
        self.expect_date = date.today() - timedelta( days = self.expect_delay )

class ClassDailyCheckHdfs(ClassDailyCheckBase): 

    def __init__(self,  hdfs_host, hdfs_port, hdfs_folder_path, file_name_pattern, *args, **kwargs): 
        super().__init__(*args, **kwargs) 
        self.hdfs_host = hdfs_host 
        self.hdfs_port = hdfs_port 
        self.hdfs_folder_path = hdfs_folder_path 
        self.file_name_pattern = file_name_pattern 
        self.hdfs_latest_file = None 
        self.hdfs_latest_date = None 
        self.hdfs_delayed_days = None 
        self.find_latest_hdfs_file() 
        self.set_hdfs_date() 
        self.hdfs_miss_days = self.find_all_empty()
        
    def find_latest_hdfs_file(self): 
        client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}') 
        files = client.list(self.hdfs_folder_path) 
        latest_file = None 
        latest_date = None 
        date_pattern = re.compile(self.file_name_pattern) 

        for file_name in files: 
            match = date_pattern.search(file_name) 
            if match: 
                date_str = match.group(0) 
                if self.file_name_pattern == r'(\d{4})(\d{2})(\d{2})': 
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" 
                folder_name_date = datetime.strptime(date_str, '%Y-%m-%d') 
                if latest_date is None or folder_name_date > latest_date: 
                    latest_file = file_name 
                    latest_date = folder_name_date
                    
        if latest_file: 
            self.hdfs_latest_file = latest_file 
            self.hdfs_latest_date = latest_date 
        else: 
            self.hdfs_latest_file = None 
            self.hdfs_latest_date = None
    
    def set_hdfs_date(self): 
        self.hdfs_delayed_days = (self.expect_date -self.hdfs_latest_date.date()).days 
    
    def find_hdfs_file(self, search_date = None): 
        
        client = InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}') 
        files = client.list(self.hdfs_folder_path) 
        date_pattern = re.compile(self.file_name_pattern) 
    
        for file_name in files: 
            match = date_pattern.search(file_name) 
            if match: 
                date_str = match.group(0) 
                if self.file_name_pattern == r'(\d{4})(\d{2})(\d{2})': 
                    date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" 
    
                folder_name_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                if folder_name_date == search_date: 
                    return True

    def find_all_empty(self, expect_date = None, time_window = None):
        if time_window is None:
            time_window = time_range
        if expect_date == None:
            expect_date = self.expect_date
        start_date = expect_date

        end_date = start_date - timedelta(days = time_window)
        cur_date = start_date
        miss_days = []
        while cur_date >= end_date:
            cur_date -= timedelta(days = 1)

            if self.find_hdfs_file(cur_date):
                #print(f"{cur_date} existed")
                pass
            else:
                miss_days.append(f"{cur_date}")
        
        if self.name[:9] == 'snap_data':
            date_objects = [datetime.strptime(date, "%Y-%m-%d") for date in miss_days] 
            filtered_dates = [date.strftime("%Y-%m-%d") for date in date_objects if date.weekday() < 5] 
            filtered_dates = [d for d in filtered_dates if d not in neglect_days]
            return filtered_dates
        else:
            return miss_days
        return miss_days

class ClassDailyCheckDruid(ClassDailyCheckHdfs): 

    def __init__(self, init_payload,miss_payload, *args, **kwargs): 

        super().__init__(*args, **kwargs)
        self.payload = init_payload
        self.druid_latest_date = None
        self.druid_delayed_days = None
        self.set_druid_date()
        self.miss_payload = miss_payload
        
        self.druid_miss_days = self.find_miss_druid()
        
    def set_druid_date(self):
        r = self.query_druid(self.payload).json()
        self.druid_latest_date = datetime.strptime(r[0]['current_date_val'], "%Y-%m-%d").date() 
        self.druid_delayed_days = (self.expect_date -self.druid_latest_date).days
         
    def query_druid(self, payload = None, API_ENDPOINT="http://njbbvmaspd6.nss.vzwnet.com:8082/druid/v2/sql"): 

        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}   
        payload = json.dumps(payload) 
        response = requests.post(API_ENDPOINT, data=payload, headers=headers) 
        return response
    
    def find_miss_druid(self, p = None, task_name = None , time_window = None):
        if p is None:
            p = self.miss_payload
        if task_name is None:
            task_name = self.name
        if time_window is None:
            time_window = time_range
        current_date = datetime.now().date()  - timedelta(days= int(expected_delay[task_name]) )
        #current_date = datetime.now().date()
        target_date_list = [ datetime.strftime( (current_date - timedelta(days=i)), "%Y-%m-%d") for i in range(time_window)]
        
        
        dict_list = self.query_druid( p ).json()
        exist_date_list = [item["existed_date"] for item in dict_list]
    
        missing_dates = [date for date in target_date_list if date not in exist_date_list ]

        
        if task_name[:13] == 'snap_data_pre':
            date_objects = [datetime.strptime(date, "%Y-%m-%d") for date in missing_dates] 
            filtered_dates = [date.strftime("%Y-%m-%d") for date in date_objects if date.weekday() < 5] 
            filtered_dates = [d for d in filtered_dates if d not in neglect_days ]
            return filtered_dates
        else:
            return missing_dates


def send_mail(send_from, send_to, subject, cc,html_content, files=None, server='vzsmtp.verizon.com' ):
    assert isinstance(send_to, list)
    assert isinstance(cc, list)
    
    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Cc'] = COMMASPACE.join(cc)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject


    msg.attach(MIMEText(html_content, 'html')) 
    
    smtp = smtplib.SMTP(server)
    smtp.sendmail(send_from, send_to + cc, msg.as_string())
    smtp.close

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    #spark = SparkSession.builder.appName('Daily SNA data availability checking').enableHiveSupport().getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs for generating Post SNA Maintenance Script Trial")

    payload = { 

        "snap_data_pre_enb": {"query": """select SUBSTR(CAST(__time as VARCHAR),1,10) as current_date_val,count(*) as row_count from snap_data_pre_enb group by SUBSTR(CAST(__time as VARCHAR),1,10) ORDER BY current_date_val DESC limit 1"""}, 

        "snap_data_post_enb": {"query": """select SUBSTR(CAST(__time as VARCHAR),1,10) as current_date_val,count(*) as row_count from snap_data_post_enb group by SUBSTR(CAST(__time as VARCHAR),1,10) ORDER BY current_date_val DESC limit 1"""}, 

        "snap_data_pre_sector": {"query": """select SUBSTR(CAST(__time as VARCHAR),1,10) as current_date_val,count(*) as row_count from snap_data_pre_sector group by SUBSTR(CAST(__time as VARCHAR),1,10) ORDER BY current_date_val DESC limit 1"""}, 

        "snap_data_post_sector": {"query": """select SUBSTR(CAST(__time as VARCHAR),1,10) as current_date_val,count(*) as row_count from snap_data_post_sector group by SUBSTR(CAST(__time as VARCHAR),1,10) ORDER BY current_date_val DESC limit 1"""}, 

        "snap_data_pre_carrier": {"query": """select SUBSTR(CAST(__time as VARCHAR),1,10) as current_date_val,count(*) as row_count  from snap_data_pre_carrier group by SUBSTR(CAST(__time as VARCHAR),1,10) ORDER BY current_date_val DESC limit 1"""}, 

        "snap_data_post_carrier": {"query": """select SUBSTR(CAST(__time as VARCHAR),1,10) as current_date_val,count(*) as row_count from snap_data_post_carrier group by SUBSTR(CAST(__time as VARCHAR),1,10) ORDER BY current_date_val DESC limit 1"""},

        "wifi_score_v2": {"query": """select SUBSTR(CAST(__time as VARCHAR),1,10) as current_date_val,count(*) as row_count from wifi_score_v2 group by SUBSTR(CAST(__time as VARCHAR),1,10) ORDER BY current_date_val DESC limit 1"""}, 
    
        "wifi_score_v3": {"query": """select SUBSTR(CAST(__time as VARCHAR),1,10) as current_date_val,count(*) as row_count from wifi_score_v3 group by SUBSTR(CAST(__time as VARCHAR),1,10) ORDER BY current_date_val DESC limit 1"""}, 

    }
    time_range = 20
    time_window = time_range
    query_template = f"""SELECT DISTINCT SUBSTR(CAST(__time AS VARCHAR), 1, 10) as existed_date FROM key_name  ORDER BY existed_date desc limit {time_window}""" 
    #query_template = f"""SELECT DISTINCT "day" as existed_date FROM key_name  ORDER BY existed_date desc limit {time_window}""" 

    miss_date_payload = {} 

    for key in payload.keys(): 

        updated_query = query_template.replace("key_name", key) 
        
        miss_date_payload[key] = {"query": updated_query} 

    hdfs_location = { 
                'snap_data_pre_enb': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/enodeb/Daily_KPI_14_days_pre_Event', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                }, 
                'snap_data_post_enb': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/enodeb/Event_Enodeb_Post_tickets_Feature_Date', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                },
                'snap_data_pre_sector': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/Sector/Daily_KPI_14_days_pre_Event', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                },
                'snap_data_post_sector': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/Sector/Event_Enodeb_Post_tickets_Feature_Date', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                },
                'snap_data_pre_carrier': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/Carrier/Daily_KPI_14_days_pre_Event', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                },
                'snap_data_post_carrier': { 
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/MonitorEnodebPef/Carrier/Event_Enodeb_Post_tickets_Feature_Date', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}' 
                },
                "wifi_score_v2": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/wifi_score_v2/homeScore_dataframe/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
                "wifi_score_v3": {
                    "hdfs_host": 'njbbvmaspd11.nss.vzwnet.com', 
                    "hdfs_port": "9870", 
                    "hdfs_folder_path": '/user/ZheS/wifi_score_v3/extenderScore_dataframe/', 
                    "file_name_pattern": r'\d{4}-\d{2}-\d{2}'
                },
        }
    
    expected_delay = {  
                    'snap_data_pre_enb': '1', 
                    'snap_data_post_enb': '1', 
                    'snap_data_pre_sector': '1', 
                    'snap_data_post_sector': '1', 
                    'snap_data_pre_carrier': '1', 
                    'snap_data_post_carrier': '1', 
                    "wifi_score_v2":'1',
                    "wifi_score_v3":'1',
                    }



# Druid -----------------------------------------------------------------------------------------
    druid_dict = {}
    for daily_task in payload.keys():
        
        druid_dict[daily_task] = ClassDailyCheckDruid( init_payload = payload[daily_task], 
                                        miss_payload = miss_date_payload[daily_task],
                                        hdfs_host = hdfs_location[daily_task]["hdfs_host"], 
                                        hdfs_port = hdfs_location[daily_task]["hdfs_port"], 
                                        hdfs_folder_path = hdfs_location[daily_task]["hdfs_folder_path"], 
                                        file_name_pattern = hdfs_location[daily_task]["file_name_pattern"],
                                        name = daily_task, 
                                        expect_delay = int(expected_delay[daily_task]) )
    data_druid = [vars( ins ) for ins in list( druid_dict.values() ) ]
    df_druid = pd.DataFrame(data_druid)[["name","druid_latest_date","druid_delayed_days","druid_miss_days","hdfs_latest_date","hdfs_delayed_days","hdfs_miss_days"]]
        # Convert DataFrames to HTML tables and add <br> tags 
    
#-----------------------------------------------------------------------------------------
    import html
    if df_druid['druid_delayed_days'].eq(0).all(): 
    #if all( [i==[] for i in df_druid["hdfs_miss_days"]] ):
    #if all( [i==[] for i in df_druid["hdfs_miss_days"]] ) and all( [i==[] for i in df_druid["druid_miss_days"]] ):
        send_mail(  'sassupport@verizon.com', 
                    ['zhe.sun@verizonwireless.com'], 
                    "SNAP 6 table check Success",
                    [], 
                    html_content = '<br><br>'.join(df.to_html() for df in [df_druid]), 
                    files=None, 
                    server='vzsmtp.verizon.com' )
        
    else:
        send_mail(  'sassupport@verizon.com', 
                ['zhe.sun@verizonwireless.com'], 
                "SNAP 6 table check Failed",
                [], 
                html_content = '<br><br>'.join(df.to_html() for df in [df_druid]), 
                files=None, 
                server='vzsmtp.verizon.com' )
        #"david.you@verizonwireless.com"