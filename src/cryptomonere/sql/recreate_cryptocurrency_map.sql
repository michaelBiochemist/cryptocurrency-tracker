Drop Table If Exists cryptocurrency_map;
Create Table cryptocurrency_map (
    id int Unique,
    currency_rank int,
    name varchar(50),
    symbol varchar(10),
    slug varchar(50),
    is_active tinyint,
    status tinyint,
    first_historical_data datetime,
    last_historical_data datetime,
    platform_id int,
    platform_name varchar(50),
    platform_symbol varchar(10),
    platform_slug varchar(50)
);
