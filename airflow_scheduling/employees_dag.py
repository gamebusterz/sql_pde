import os
import airflow
from airflow import DAG
# import af_config

from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator

source_dir = 'sql_pde'

default_args = {
    'owner': 'Sailesh',
    'depends_on_past': False,
    # 'start_date': airflow.utils.dates.days_ago(2),
    'start_date': str(datetime.now())[:10],
    'email': ['saileshchoyal@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

employees_dag = DAG(
    'employees',
    default_args=default_args,
    description='Scheduled on 8:00 AM every day',
    # schedule_interval=timedelta(days=1),
    schedule_interval='*/2 * * * *',
)

"""Execute Task in dags directory"""
task = BashOperator(
    task_id = 'sql_pde_employees',
    # root_dir = 
    bash_command = 'python3 ' + os.path.dirname(__file__).split(source_dir)[0] + source_dir + '/main.py --spec'+os.path.dirname(__file__).split(source_dir)[0] + source_dir+' task_spec/employees.json',
    dag = employees_dag,
)

task