# Criar uma DAG de exemplo
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from operators.collect_data import Collect_data_operator

with DAG("dag_steam_games", start_date=datetime(2024, 4, 22), schedule_interval='*/10 * * * *', catchup=False) as dag:

    create_table_triagem_identificador = PythonOperator(
    task_id='get_games_list',
    python_callable= Collect_data_operator.data_steam_games,
    provide_context=True,
    dag=dag)