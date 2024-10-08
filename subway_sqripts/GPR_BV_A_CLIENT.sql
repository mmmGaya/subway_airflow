insert into dbt_schema."GPR_BV_A_CLIENT"
select 
	ma.run_id  dataflow_id,
    ma.execution_date dataflow_dttm,
    client_rk,
    x_client_rk
from
	(
	select 
		s1.client_rk x_client_rk,
		min(s2.client_rk) client_rk
	from 
		dbt_schema."GPR_RV_S_CLIENT" s1
		join 
		dbt_schema."GPR_RV_S_CLIENT" s2
		on s1.client_name_desc = s2.client_name_desc and s1.client_phone_desc = s2.client_phone_desc
	where s1.client_rk in 
		(
		select hub_key  from dbt_schema."GPR_RV_H_CLIENT"
		except 
		select x_client_rk from dbt_schema."GPR_BV_A_CLIENT"
		)
	group by s1.client_rk
	)
	, dbt_schema.metadata_airflow ma ;

commit;

--  придумать единный алгоритм формиравание мастер ключа 

-- вопрос который задать (может ли прийти dataflow_id, dataflow_dttm равное null)

-- логика выбора мастер-ключа, выбирается самый минимальный из выгрузки и назначен мастер источник 