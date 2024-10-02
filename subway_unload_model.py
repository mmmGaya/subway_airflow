import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
  dag_id="A_unload_subway_model_develop", # The name that shows up in the UI
  start_date=datetime.datetime(2024, 9, 25),
  schedule_interval = '*/3 * * * *',# Start date of the DAG
  catchup=False,
  template_searchpath='/var/dags',
) as dag:

 
    # измения в таблице с мета полями id_потока и время запуска
    cut_ods_table = PostgresOperator(
        task_id = 'get_query_cut',
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/update_metadata.sql',
        params = {"run_id" : "{{ run_id}}", "execution_date" : "{{ execution_date }}"},
        dag = dag,
    )

# тестовый оператор, имитирует выгрузку данных в ods таблицу из таблицы в исходной системы
    _test_insert_ods = PostgresOperator(
        task_id = 'test_insert_ods',
        postgres_conn_id = 'dbt_postgres',
        sql = '''
              insert into raw_source_subway
              (select 
              execution_date, 
              'dbt_schema.start_client_test'::regclass::oid, 
              s.* 
              from start_client_test s, metadata_airflow ma)''',
        dag = dag,
    )

#  запускаем dbt модель обрезка таблицы ods
    cut_table_dbt = BashOperator(
          task_id="cut_dbt",
          bash_command=f"cd /home/anarisuto-12/dbt/subway_project" # Go to the path containing your dbt project
          + '&& source /home/anarisuto-12/dbt/venv/bin/activate' # Load Pyenv
          + f"&& dbt run --models models/example/ods_client_cut.sql", # run the model!
      )
    
    



    cut_ods_table >> _test_insert_ods >> cut_table_dbt
