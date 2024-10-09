-- вставка новых значений, если записи об экземпляре ест ьи данные там изменились
insert into dbt_schema."GPR_BV_P_CLIENT"				   	  
select run_id dataflow_id, execution_date dataflow_dtt, client_rk, 
       mx_dt valid_from_dttm,
       to_timestamp( '5999-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_to_dttm,
       mx_dt client_subway_star_vf_dttm
from (
select ac.client_rk client_rk, max(sc.valid_from_dttm) mx_dt
from dbt_schema."GPR_BV_A_CLIENT" ac join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk and sc.delete_flg <> 1 and sc.actual_flg = 1
group by  ac.client_rk) tb, dbt_schema.metadata_airflow
where client_rk in (select client_rk from dbt_schema."GPR_BV_P_CLIENT") 
  and mx_dt > (select max (client_subway_star_vf_dttm) from dbt_schema."GPR_BV_P_CLIENT" where client_rk = tb.client_rk);

  
-- обновление конца периода в случае, когда экземпляр сущности не удален
update dbt_schema."GPR_BV_P_CLIENT"
set (dataflow_id, dataflow_dttm, valid_to_dttm) = (select dataflow_id, dataflow_dtt, ld - interval '1 minute'
					   from 
							(select run_id dataflow_id, execution_date dataflow_dtt,
									lead(valid_from_dttm, 1, valid_to_dttm) over (partition by client_rk, valid_to_dttm order by valid_from_dttm) ld
							   from dbt_schema."GPR_BV_P_CLIENT", dbt_schema.metadata_airflow)
							  where valid_to_dttm <> ld)
where (client_rk, valid_from_dttm) in (select client_rk, valid_from_dttm
										from 
											(select client_rk, valid_from_dttm,
												   lead(valid_from_dttm, 1, valid_to_dttm) over (partition by client_rk, valid_to_dttm order by valid_from_dttm) ld
											from dbt_schema."GPR_BV_P_CLIENT")
											where valid_to_dttm <> ld);

-- обновление конца периода в случае, когда экземпляр сущности удален
update dbt_schema."GPR_BV_P_CLIENT"								
set valid_to_dttm = valid_from_dttm, (dataflow_id, dataflow_dttm) = (select run_id, execution_date from dbt_schema.metadata_airflow)
where (client_rk, valid_from_dttm) in (select ac.client_rk a, max(valid_from_dttm)
										from dbt_schema."GPR_RV_S_CLIENT" sc 
										join dbt_schema."GPR_BV_A_CLIENT" ac on sc.client_rk = ac.x_client_rk and delete_flg = 1
										group by ac.client_rk);

-- вставка записей о новых экземпляров
insert into dbt_schema."GPR_BV_P_CLIENT"					   	  
select *
from
(select distinct run_id dataflow_id, execution_date dataflow_dttm, client_rk, 
	   to_timestamp( '1960-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_from_dttm,
	   mx_dt - interval '1 minute' valid_to_dttm,
	   to_timestamp( '1960-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') client_subway_star_vf_dttm
from (select ac.client_rk client_rk, max(sc.valid_from_dttm) mx_dt
from dbt_schema."GPR_BV_A_CLIENT" ac join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk
group by ac.client_rk), dbt_schema.metadata_airflow
where client_rk not in (select client_rk from dbt_schema."GPR_BV_P_CLIENT")
union
select run_id dataflow_id, execution_date dataflow_dttm, client_rk, 
	   mx_dt valid_from_dttm,
	   to_timestamp( '5999-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_to_dttm,
	   mx_dt client_subway_star_vf_dttm
from (select ac.client_rk client_rk, max(sc.valid_from_dttm) mx_dt
from dbt_schema."GPR_BV_A_CLIENT" ac join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk
group by ac.client_rk), dbt_schema.metadata_airflow
where client_rk not in (select client_rk from dbt_schema."GPR_BV_P_CLIENT"))
order by client_rk, valid_from_dttm;