
  
    
    

    create  table
      "bronze"."main"."bronze_table_data__dbt_tmp"
  
    as (
      -- models/bronze/raw_data.sql


SELECT *
FROM raw_data
    );
  
  