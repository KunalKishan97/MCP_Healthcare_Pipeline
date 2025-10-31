select
    load_id,
    load_dtm,
    src,
    chart_number,
    {{ concat_columns(['name_first', 'name_last']) }} as patientname,
    {{ concat_columns(['address_1', 'address_2', 'city', 'state','zipcode'], ', ') }} as full_address,
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
from {{ deduplicate(ref('bronze_table_data'), 'chart_number') }}
