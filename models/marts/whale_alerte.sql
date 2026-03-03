with cte_whale as(
    select
        address,
        sum(value) as total_sent,
        count(*) as total_count


    from {{ ref('stg_btc_transactions') }}
    where value > 10
    group by address

),
latest_price as(
    select
        price


    from {{ ref('btc_usd_max') }}
    where cast(Replace(SNAPPED_AT,'UTC','') as date) = current_date()




)

select
    w.address,
    w.total_sent,
    w.total_count,
    l.price,
    l.price * w.total_sent as total_amount
from cte_whale as w
cross join latest_price as l