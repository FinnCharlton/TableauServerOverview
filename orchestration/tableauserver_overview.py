from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime

with DAG(
    dag_id='tableauserver_overview',
    start_date=datetime.datetime(2024, 8, 19),
    schedule_interval='@daily',
    catchup=True,
    tags=['prod','python','dbt'],

) as dag:
    task_1 = BashOperator(
    task_id='ingestion_task',
    bash_command="python /opt/airflow/dags/src/main.py"
    )

    task_2 = BashOperator(
    task_id='modelling_task',
    bash_command="cd /opt/airflow/plugins/tableauserver_overview && dbt build --profiles-dir ..",
    )

    task_1 >> task_2