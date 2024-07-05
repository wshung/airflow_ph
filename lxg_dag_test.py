from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


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
    start_date=datetime(2024, 6, 14,)
    schedule_interval=None,
    catchup=False,
    tags=['lxg_tag']
    )

def bash_dag():
    task_1=BashOperator(
        task_id='lxg_flow_02', 
        bash_command='python3 /home/eileen_liao_yulon_group_com/airflow/dags/luxgen/flow_02.py',
         )

    task_2=BashOperator(
        task_id='lxg_flow_03', 
        bash_command='python3 /home/eileen_liao_yulon_group_com/airflow/dags/luxgen/flow_03.py',
         )
    
    task_1>>task_2

run_dag = bash_dag()
