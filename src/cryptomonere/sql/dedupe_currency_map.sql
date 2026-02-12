drop table if exists currency;

create table currency as
select
    id,
    currency_rank,
    name,
    symbol,
    slug,
    is_active,
    status,
    first_historical_data,
    last_historical_data,
    platform_name,
    platform_symbol,
    platform_slug
from
    (select
            *,
            row_number() over (
                partition by symbol order by currency_rank asc
            ) as row_rank,
            count(*) over (partition by symbol) as total_duplicates
        from cryptocurrency_map
    )
where row_rank = 1
order by currency_rank asc;

create index currency_symbol on currency(symbol);
