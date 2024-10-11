
-- вставка новой записи или изменившийся 
INSERT INTO  dbt_schema."GPR_RV_E_CLIENT"
select 
	ma.run_id  dataflow_id,
	ma.execution_date dataflow_dttm,
	md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key,
	md5(id || '#' || oid) client_rk,
	0 delete_flg,
	1 actual_flg, 
	oid source_system_dk,
	current_timestamp valid_from_dttm
from 
	ods_client_cut, dbt_schema.metadata_airflow ma
where md5(id || '#' || oid) in
(
select 
	client_rk
from
	(
	select 
		md5(id || '#' || oid) client_rk, 
		md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key 
	from 
	ods_client_cut occ 
	except
	select 
		client_rk, 
		hashdiff_key   
	from 
		dbt_schema."GPR_RV_E_CLIENT" where actual_flg = 1 and delete_flg = 0)
		);


-- добавление новой записи с пометкой удаление на источнике 
INSERT INTO  dbt_schema."GPR_RV_E_CLIENT"
select 
	ma.run_id  dataflow_id,
	ma.execution_date dataflow_dttm,
	hashdiff_key,
	client_rk,
	1 delete_flg,
	1 actual_flg, 
	source_system_dk,
	ma.execution_date valid_from_dttm
from 
	dbt_schema."GPR_RV_E_CLIENT", dbt_schema.metadata_airflow ma
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		dbt_schema."GPR_RV_E_CLIENT"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(id || '#' || oid) client_rk
	from 
		ods_client_cut occ )
		)
	and actual_flg = 1;


-- изменяем флаг актуальность на 0, для старой записи
UPDATE dbt_schema."GPR_RV_E_CLIENT"
SET actual_flg = 0
WHERE (client_rk, valid_from_dttm) IN 
(
SELECT 
client_rk, min(valid_from_dttm) 
FROM 
dbt_schema."GPR_RV_E_CLIENT"
WHERE actual_flg = 1
GROUP BY client_rk, actual_flg
HAVING COUNT(*) > 1
);

COMMIT;