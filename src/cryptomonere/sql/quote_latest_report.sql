select
    last_updated,
    symbol,
    name,
    price,
    percent_change_24h,
    percent_change_7d,
    percent_change_30d,
    percent_change_60d,
    percent_change_90d,
    COALESCE(volume_24h / market_cap, 0)
from quote_latest
order by price desc;
