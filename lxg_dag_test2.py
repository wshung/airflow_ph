from datetime import datetime, timedelta
# from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import test.airflow_ph.luxgen.flow_04 as flow_04
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
    
    task_1=PythonOperator(
        task_id='lxg_flow_04', 
        python_callable=flow_04
         )
    
    task_1

run_dag = bash_dag()
