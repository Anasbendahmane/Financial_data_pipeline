select
    *
from {{ source('BTC_SCHEMA', 'BTC_TABLE') }}