
-- вставляем измененые данные либо новые 
INSERT INTO dbt_schema."GPR_RV_S_CLIENT"
SELECT
    ma.run_id  dataflow_id,
    ma.execution_date dataflow_dttm,
    oid source_system_dk, 
    md5(id|| '#' || oid) client_rk, 
    ma.execution_date valid_from_dttm, 
    md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key,
    1 actual_flg,
    0 delete_flg,
    name client_name_desc,
    phone client_phone_desc,
    city client_city_desc,
    birthday client_city_dt,
    age client_age_cnt
FROM 
    ods_client_cut, dbt_schema.metadata_airflow ma
WHERE md5(id || '#' || oid) IN 
            (SELECT
                hub_key
            FROM 
            -- если будут дубли вопрос (просмотреть)
                (SELECT md5(id || '#' || oid) hub_key, md5(name || '#' || phone || '#' || city || '#' || birthday || '#' || age) hashdiff_key FROM ods_client_cut
                except
                SELECT client_rk, hashdiff_key FROM dbt_schema."GPR_RV_S_CLIENT" where actual_flg = 1 and delete_flg = 0
                 )
            );

INSERT INTO  dbt_schema."GPR_RV_S_CLIENT"
select 
	ma.run_id  dataflow_id,
    ma.execution_date dataflow_dttm,
    source_system_dk, 
    client_rk, 
    ma.execution_date valid_from_dttm, 
    hashdiff_key,
    1 actual_flg,
    1 delete_flg,
    client_name_desc,
    client_phone_desc,
    client_city_desc,
    client_city_dt,
    client_age_cnt
from 
	dbt_schema."GPR_RV_S_CLIENT", dbt_schema.metadata_airflow ma
where client_rk in
(
select 
	client_rk
from
	(
	select 
		client_rk
	from 
		dbt_schema."GPR_RV_S_CLIENT"
	 where delete_flg = 0 and actual_flg = 1
    except
    select 
		md5(id || '#' || oid) client_rk
	from 
		ods_client_cut occ )
		)
	and actual_flg = 1;


--   изменяем флаг актуальности для записи у которой поменялся атрибут
UPDATE dbt_schema."GPR_RV_S_CLIENT"
SET actual_flg = 0
WHERE (client_rk, valid_from_dttm) IN 
(
SELECT 
client_rk, min(valid_from_dttm) 
FROM 
dbt_schema."GPR_RV_S_CLIENT"
WHERE actual_flg = 1
GROUP BY client_rk, actual_flg
HAVING COUNT(*) > 1
);

COMMIT;

--  отслеживать полность удаление хаба так его атрибуты тоже становятся неактуальными, требуется изменить флаг delete_flg с 0 на 1  