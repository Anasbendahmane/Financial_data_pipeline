{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'hash_key'
    )
}}


select
    *
from {{ source('BTC_SCHEMA', 'BTC_TABLE') }}

{% if is_incremental() %}
    where BLOCK_TIMESTAMP >= (select max(BLOCK_TIMESTAMP) from {{ this }})
{% endif %}