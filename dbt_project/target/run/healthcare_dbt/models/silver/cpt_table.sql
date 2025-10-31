
  
  create view "bronze"."main"."cpt_table__dbt_tmp" as (
    select
    load_id,
    load_dtm,
    src,
    enc_id,
    chart_number,
    dos,
    cpt,
    cpt_desc
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
