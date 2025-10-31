select
    load_id,
    load_dtm,
    src,
    enc_id,
    chart_number,
    dos,
    appointment_status,
    type,
    ins_primary,
    ins_secondary,
    client
from {{ deduplicate(ref('bronze_table_data'), 'chart_number') }}
