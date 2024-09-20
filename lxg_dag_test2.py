
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'max_active_runs': 1,
    'start_date': datetime(2024, 7, 22),
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="lxg_dag_test",
    default_args=default_args,
    schedule_interval='@once',
)

lxg_flow_04=BashOperator(
    task_id='lxg_flow_04', 
    bash_command='python /home/eileen_liao_yulon_group_com/airflow/dags/test/airflow_ph/luxgen/flow_04.py',
    dag=dag,
)

lxg_flow_04
