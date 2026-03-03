{{
  config(
    materialized = 'incremental',
    unique_key= 'hash_key',
    incremental_strategy= 'merge'
    )
}}

with cte_incremental as(

select
    t.hash_key,
    t.block_number,
    t.block_timestamp,
    t.transcation_category,
    f.value:address::string as address,
    f.value:value::number as value

from {{ ref('stg_btc') }} as t,
LATERAL flatten(input => outputs) as f

{%if is_incremental()%}
    where block_timestamp >= (select max(block_timestamp) from {{this}})

{%endif%}


)

select 
    hash_key,
    block_number,
    block_timestamp,
    transcation_category,
    address,
    value
from cte_incremental