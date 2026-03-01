select
    t.hash_key,
    t.block_number,
    t.block_timestamp,
    t.transcation_category,
    f.value:address::string as address,
    f.value:value::number as value

from {{ ref('stg_btc') }} as t,
LATERAL flatten(input => outputs) as f