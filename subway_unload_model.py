import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
  dag_id="A_unload_subway_model", # The name that shows up in the UI
  start_date=datetime.datetime(2024, 9, 25),
  schedule_interval = '*/3 * * * *',# Start date of the DAG
  catchup=False,
  template_searchpath='/var/dags',
) as dag:
    
    # def _generate_query(**context):
    #     with open('/var/dags/dags_arina/postgres_query.sql', 'w') as file:
    #         file.write(f"INSERT INTO dbt_schema.metadata_airflow VALUES ('{context['run_id']}','{context['execution_date']}');\ncommit;")

     
        
    # generate_query = PythonOperator(
    # task_id="get_query",
    # python_callable=_generate_query,
    # dag=dag,
    # )
    
    
    # unload_table = PostgresOperator(
    #     task_id = 'unload_source_table',
    #     postgres_conn_id = 'dbt_postgres',
    #     sql = 'subway_sqripts/unload_source_tab.sql',
    #     dag = dag,
        
    # )
    #  выгрузка данный из внешнего источника в таблица dbt_schemf.ods_client
    #   test for commit aaaaaa

    increm_hub = PostgresOperator(
        task_id = 'record_hub',
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/increm_insert_hub.sql',
        params = {"run_id" : "{{ run_id}}", "execution_date" : "{{ execution_date }}"},
        dag = dag,
    )
    
  
    
    
    # generate_query >> write_to_postgres 