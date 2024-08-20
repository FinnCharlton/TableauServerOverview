from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime

with DAG(

    #Define DAG name and run schedule
    dag_id='tableauserver_overview',
    start_date=datetime.datetime(2024, 8, 19),
    schedule_interval='@daily',
    catchup=True,
    tags=['prod','python','dbt'],

) as dag:
    
    #Task 1 runs the ingestion script
    task_1 = BashOperator(
    task_id='ingestion_task',
    bash_command="python /opt/airflow/dags/src/main.py"
    )

    #Task 2 navigates to the dbt project folder and runs the build command, specifying the location of profiles.yml
    task_2 = BashOperator(
    task_id='modelling_task',
    bash_command="cd /opt/airflow/plugins/tableauserver_overview && dbt build --profiles-dir ..",
    )

    #Task 2 specified as downstream of Task 1
    task_1 >> task_2