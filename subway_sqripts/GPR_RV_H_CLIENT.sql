
INSERT INTO dbt_schema."GPR_RV_H_CLIENT"
SELECT 
    run_id dataflow_id,
    execution_date dataflow_dttm,
    oid source_system_dk,
    id client_rk,
    md5(id || '#' || oid) hub_key
FROM 
    (select 
        ma.run_id,
        o.*
    from 
        ods_client_cut o, dbt_schema.metadata_airflow ma
    where 
        (oid, id) in  
            (select oid, id from ods_client_cut
            except
            select source_system_dk, client_rk from dbt_schema."GPR_RV_H_CLIENT"
            )
    );
COMMIT;


--  разобрать кейс из источника приходит две записи с одинаковым id 









