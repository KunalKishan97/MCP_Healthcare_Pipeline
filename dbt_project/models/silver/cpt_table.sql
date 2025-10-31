select
    load_id,
    load_dtm,
    src,
    enc_id,
    chart_number,
    dos,
    cpt,
    cpt_desc
from {{ deduplicate(ref('bronze_table_data'), 'chart_number') }}
