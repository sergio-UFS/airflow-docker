# Criar uma DAG de exemplo
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from operators.collect_data import Collect_data_operator

with DAG("dag_spotify_data", start_date=datetime(2024, 2, 24), schedule_interval="@daily", catchup=False) as dag:

    create_table_triagem_identificador = PythonOperator(
    task_id='get_daily_spotify_data',
    python_callable= Collect_data_operator.collect_daily_spotify,
    provide_context=True,
    dag=dag)