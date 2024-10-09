-- вставка новых значений, если записи об экземпляре ест ьи данные там изменились
insert into "GPR_BV_P_CLIENT"				   	  
select run_id dataflow_id, execution_date dataflow_dtt, client_rk, 
       mx_dt valid_from_dttm,
       to_timestamp( '5999-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_to_dttm,
       mx_dt client_subway_star_vf_dttm
from (
select ac.client_rk client_rk, max(sc.valid_from_dttm) mx_dt
from "GPR_BV_A_CLIENT" ac join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk
group by  ac.client_rk) tb, dbt_schema.metadata_airflow
where client_rk in (select client_rk from "GPR_BV_P_CLIENT") 
  and mx_dt > (select max (client_subway_star_vf_dttm) from "GPR_BV_P_CLIENT" where client_rk = tb.client_rk)

  
-- обновление конца периода
update "GPR_BV_P_CLIENT"
set valid_to_dttm = valid_from_dttm + interval '5 minute'
where (client_rk, valid_from_dttm) in (select client_rk, valid_from_dttm
										from 
											(select client_rk, valid_from_dttm, valid_to_dttm, client_subway_star_vf_dttm,
												   lead(valid_from_dttm, 1, valid_to_dttm) over (partition by client_rk, valid_to_dttm order by valid_from_dttm) ld
											from "GPR_BV_P_CLIENT")
											where valid_to_dttm <> ld)

-- вставка записей о новых экземпляров
insert into "GPR_BV_P_CLIENT"					   	  
select *
from
(select distinct run_id dataflow_id, execution_date dataflow_dttm, client_rk, 
	   to_timestamp( '1960-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_from_dttm,
	   execution_date - interval '5 minute' valid_to_dttm,
	   to_timestamp( '1960-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') client_subway_star_vf_dttm
from "GPR_BV_A_CLIENT", dbt_schema.metadata_airflow
where client_rk not in (select client_rk from "GPR_BV_P_CLIENT")
union
select run_id dataflow_id, execution_date dataflow_dttm, client_rk, 
	   mx_dt valid_from_dttm,
	   to_timestamp( '5999-01-01 00:00:00', 'yyyy-mm-dd hh24:mi:ss') valid_to_dttm,
	   mx_dt client_subway_star_vf_dttm
from (select ac.client_rk client_rk, max(sc.valid_from_dttm) mx_dt
from "GPR_BV_A_CLIENT" ac join dbt_schema."GPR_RV_S_CLIENT" sc on ac.x_client_rk = sc.client_rk
group by ac.client_rk), dbt_schema.metadata_airflow
where client_rk not in (select client_rk from "GPR_BV_P_CLIENT"))
order by client_rk, valid_from_dttm;