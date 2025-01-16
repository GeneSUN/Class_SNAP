from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 
import airflow.settings
from airflow.models import DagModel
import time

from datetime import datetime, timedelta, date 
from textwrap import dedent
import sys

default_args = { 
    'owner': 'ZheSun', 
    'depends_on_past': False, 
} 

with DAG(
    dag_id="snap_enodeb_sector_carrier",
    default_args=default_args,
    description="get abnormal enodeb/sector/carrier after event",
    schedule_interval="00 21 * * *",
    start_date=days_ago(1),
    tags=["SAS","SNAP","ENB"],
    catchup=False,
    max_active_runs=1,
) as dag:
    """
    """
    hdfs_tasks = {}
    bash_names = ["pre_enodeb","post_enodeb","pre_sector","post_sector","pre_carrier","post_carrier"] 
    for bash_name in bash_names: 

        task = BashOperator( 
            task_id=bash_name, 
            bash_command = f"/usr/apps/vmas/script/ZS/SNAP/{bash_name.split('_')[1]}/{bash_name}.sh ", 
            env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
            dag=dag, 
        ) 

        hdfs_tasks[bash_name] = task
    
    delay_python_task: PythonOperator = PythonOperator(task_id="delay_python_task",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(300))

    check_task = BashOperator(
                                task_id = "CheckCompleteSnap",
                                bash_command='python3 /usr/apps/vmas/script/ZS/SNAP/CheckCompleteSnap.py ',
                                dag = dag
                                )
    druid_hash = {
                "pre_enodeb_druid": "/usr/apps/vmas/venky/Event_Enodeb_Post_tickets_Feature_Date/Event_Enodeb_Post_tickets_Feature_Date_v1.py",
                "post_enodeb_druid": "/usr/apps/vmas/venky/Event_Enodeb_Post_tickets_Feature_Date/sector/Event_Enodeb_Post_tickets_Feature_Date_v1.py ",
                "pre_sector_druid": "/usr/apps/vmas/venky/Event_Enodeb_Post_tickets_Feature_Date/Carrier/Event_Enodeb_Post_tickets_Feature_Date.py ",
                "post_sector_druid": "/usr/apps/vmas/venky/Daily_KPI_14_days_pre_Event/Daily_KPI_14_days_pre_Event_v1.py ",
                "pre_carrier_druid": "/usr/apps/vmas/venky/Daily_KPI_14_days_pre_Event/sector/Daily_KPI_14_days_pre_Event_v1.py ",
                "post_carrier_druid": "/usr/apps/vmas/venky/Daily_KPI_14_days_pre_Event/Carrier/Daily_KPI_14_days_pre_Event.py "
                    }

    druid_tasks = {}
    for d in druid_hash: 

        druid_task = BashOperator( 
            task_id=d, 
            bash_command = f"python3 {druid_hash[d]}", 
            env={'VAR_RUN_ID': '{{ run_id }}', 'VAR_TRY_NUMBER': '{{ task_instance.try_number }}'},
            dag=dag, 
        )
        druid_tasks[d] = druid_task

    hdfs_tasks["pre_enodeb"] >> hdfs_tasks["post_enodeb"] 
    hdfs_tasks["post_enodeb"] >> [hdfs_tasks["pre_sector"],hdfs_tasks["pre_carrier"]] 
    hdfs_tasks["pre_sector"].set_downstream( [hdfs_tasks["post_sector"],hdfs_tasks["post_carrier"]  ])
    hdfs_tasks["pre_carrier"].set_downstream( [hdfs_tasks["post_sector"],hdfs_tasks["post_carrier"]  ])
    hdfs_tasks["post_sector"].set_downstream( list( druid_tasks.values() ) )
    hdfs_tasks["post_carrier"] .set_downstream( list( druid_tasks.values() ) )
    list( druid_tasks.values() ) >> delay_python_task >> check_task

