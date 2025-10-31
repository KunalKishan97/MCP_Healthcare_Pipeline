
  
  create view "bronze"."main"."dx_table__dbt_tmp" as (
    select
    load_id,
    load_dtm,
    src,
    enc_id,
    chart_number,
    dos,
    icd10_1,
    icd10_2,
    icd10_3,
    icd10_4,
    icd10_5,
    icd10_6,
    icd10_7,
    icd10_8,
    icd10_9,
    icd10_10,
    icd10_11,
    icd10_12
from 
    (
        select *
        from (
            select *,
                row_number() over (partition by chart_number order by load_dtm desc) as row_num
            from "bronze"."main"."bronze_table_data"
        ) t
        where row_num = 1
    )

  );
