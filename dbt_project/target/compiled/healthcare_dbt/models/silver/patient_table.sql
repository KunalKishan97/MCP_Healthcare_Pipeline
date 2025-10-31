select
    load_id,
    load_dtm,
    src,
    chart_number,
    trim(coalesce(name_first, '') || ' ' || coalesce(name_last, ''))
 as patientname,
    trim(coalesce(address_1, '') || ', ' || coalesce(address_2, '') || ', ' || coalesce(city, '') || ', ' || coalesce(state, '') || ', ' || coalesce(zipcode, ''))
 as full_address,
    dob,
    city,
    state,
    zipcode,
    mobile_no,
    sex,
    race,
    ethnicity,
    language,
    dos_first,
    client
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
