with variability_alert_cte as (
    select
        q.symbol,
        ql.last_updated as date_latest,
        q.last_updated as date_prev,
        --    julianday(ql.last_updated) - julianday(q.last_updated) as datediff,
        --    ql.price as price_latest,
        --    q.price as price_prev,
        --    ql.price - q.price as price_diff,
        (ql.price - q.price) / q.price as variation,
        avr.id as rule_id
    from quote_latest ql
    inner join
        alert_variability_rule avr on
            (ql.symbol = avr.symbol or avr.symbol is null)
    inner join
        quote q on
            ql.symbol = q.symbol
            and
            julianday(ql.last_updated) - julianday(q.last_updated)
            between avr.duration and round(
                avr.duration + avr.duration * 0.1 + 0.5
            )
    where
        (
            100 * (
                ql.price - q.price
            ) / q.price > avr.percent_change and avr.percent_change > 0
        )
        or
        (
            100 * (
                ql.price - q.price
            ) / q.price < avr.percent_change and avr.percent_change < 0
        )
    union all
    select
        ql.symbol,
        ql.last_updated as date_latest,
        h.enddate as date_prev,
        --    julianday(ql.last_updated) - julianday(h.enddate) as datediff,
        --    ql.price as price_latest,
        --    h.close as price_prev,
        --    ql.price - h.close as price_diff,
        (ql.price - h.close) / h.close as variation,
        avr.id as rule_id
    from quote_latest ql
    inner join
        alert_variability_rule avr on
            (ql.symbol = avr.symbol or avr.symbol is null)
    inner join
        historical h on
            ql.symbol = h.symbol
            and
            julianday(ql.last_updated) - julianday(h.enddate)
            between avr.duration and round(
                avr.duration + avr.duration * 0.1 + 0.5
            )
    where
        (
            100 * (
                ql.price - h.close
            ) / h.close > avr.percent_change and avr.percent_change > 0
        )
        or
        (
            100 * (
                ql.price - h.close
            ) / h.close < avr.percent_change and avr.percent_change < 0
        )
    order by date_prev desc
),

windowed_alert_cte as (
    select
        *,
        row_number() over (
            partition by symbol, rule_id order by date_prev desc
        ) as row_num
    from variability_alert_cte
)

select
    symbol,
    date_latest,
    date_prev,
    variation * 100 as percent_variation
from windowed_alert_cte
where row_num = 1
