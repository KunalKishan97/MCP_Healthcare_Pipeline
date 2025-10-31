-- models/bronze/raw_data.sql
{{ config(
    materialized='table'
) }}

SELECT *
FROM raw_data
