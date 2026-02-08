drop table if exists quote_latest;

create table quote_latest as
select * from (
    select
        *,
        row_number() over (
            partition by symbol order by timestamp desc
        ) as is_latest
    from quote
)
where is_latest = 1
