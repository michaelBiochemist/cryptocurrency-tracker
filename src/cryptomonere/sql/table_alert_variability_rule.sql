drop table if exists alert_variability_rule;

create table alert_variability_rule(
    id int primary key,
    symbol varchar(10),
    percent_change int,
    duration real
);
