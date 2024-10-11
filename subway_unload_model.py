import os
import json
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
  dag_id="N_unload_subway_model_develop", # The name that shows up in the UI
  start_date=datetime.datetime(2024, 9, 25),
  schedule_interval = '*/5 * * * *',# Start date of the DAG
  catchup=False,
  template_searchpath='/var/dags/dags_arina/subway_arina_flow/subway_airflow',
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
    
# слой Raw Vault
    hub_compare_ins = PostgresOperator(
        task_id = "update_hub",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/GPR_RV_H_CLIENT.sql',
        dag = dag, 
    )
    
    satelite_compare_ins = PostgresOperator(
        task_id = "update_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/GPR_RV_S_CLIENT.sql',
        dag = dag, 
    )
    
    eff_sat_compare_ins = PostgresOperator(
        task_id = "update_eff_satelite",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/GPR_RV_E_CLIENT.sql',
        dag = dag, 
    )
    
    unif_sal_compare_ins = PostgresOperator(
        task_id = "update_sal",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/GPR_BV_A_CLIENT.sql',
        dag = dag, 
    )

    pit_compare_ins = PostgresOperator(
        task_id = "update_pit",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/GPR_BV_P_CLIENT.sql',
        dag = dag, 
    )

    dim_compare_ins = PostgresOperator(
        task_id = "update_dim",
        postgres_conn_id = 'dbt_postgres',
        sql = 'subway_sqripts/GPR_EM_DIM_CLIENT.sql',
        dag = dag, 
    )



    cut_ods_table >> _test_insert_ods >> cut_table_dbt >> [hub_compare_ins, satelite_compare_ins, eff_sat_compare_ins] 

    [hub_compare_ins, satelite_compare_ins, eff_sat_compare_ins] >> unif_sal_compare_ins >> pit_compare_ins >> dim_compare_ins