with cte_whale as(
    select
        address,
        sum(value) as total_sent,
        count(*) as total_count


    from {{ ref('stg_btc_transactions') }}
    where value > 10
    group by address

)


select
    *
from cte_whale
order by total_sent desc
