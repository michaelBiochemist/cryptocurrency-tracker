with price_progression_cte as (
    select
        last_updated,
        price,
        symbol,
        lead(price) over (win) as price_next,
        lead(last_updated) over (win) as updated_next
    from quotes
    window win as (partition by symbol order by last_updated asc)
),

next_cte as (
    select
        q.symbol,
        date(ppc.last_updated) as datey,
        case
            when
                q.price * 2 between min(
                    ppc.price, ppc.price_next
                ) and max(ppc.price, ppc.price_next)
                then 'double' else 'half' end as datetype
    from price_progression_cte ppc
    inner join quote_latest q on ppc.symbol = q.symbol
    where
        q.price * 2 between min(
            ppc.price, ppc.price_next
        ) and max(ppc.price, ppc.price_next)
        or q.price / 2 between min(
            ppc.price, ppc.price_next
        ) and max(ppc.price, ppc.price_next)
    union
    select
        q.symbol,
        enddate as datey,
        case
            when q.price * 2 between low and high then 'double' else 'half'
        end as datetype
    from historical h
    inner join quote_latest q on h.symbol = q.symbol
    where q.price * 2 between h.low and h.high
        or q.price / 2 between h.low and h.high
),

final_date_cte as (
    select
        symbol,
        datetype,
        max(datey) as datey
    from next_cte
    group by symbol, datetype
)

select
    q.name,
    q.symbol,
    q.price,
    q.last_updated,
    coalesce(double.datey, 'Never') as last_at_double,
    coalesce(half.datey, 'Never') as last_at_half
from quote_latest q
left join
    final_date_cte double on
        q.symbol = double.symbol and double.datetype = 'double'
left join
    final_date_cte half on q.symbol = half.symbol and half.datetype = 'half'
