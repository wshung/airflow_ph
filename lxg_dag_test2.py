
from datetime import datetime, timedelta
# from airflow import DAG
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable, XCom

default_args = {
    'owner': 'airflow',
    'max_active_runs': 1,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="lxg_dag_test",
    default_args=default_args,
    start_date=datetime(2024, 7, 22),
    schedule_interval='@once',
    catchup=False,
    tags=['lxg_tag_test']
    )
def bash_dag():
    
    task_1=BashOperator(
        task_id='lxg_flow_04', 
        bash_command='python3 /home/eileen_liao_yulon_group_com/airflow/dags/test/airflow_test/luxgen/flow_04.py',
         )
    
    task_1

run_dag = bash_dag()
