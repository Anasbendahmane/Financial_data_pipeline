{{
  config(
    materialized = 'ephemeral',
    )
}}

select
    *
from {{ ref('stg_btc_outputs') }}
where transcation_category = 'False'