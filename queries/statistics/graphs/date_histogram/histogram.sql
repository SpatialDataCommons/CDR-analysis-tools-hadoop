select explode(histogram_numeric(active_days, 10)) as active_day_bins from
(select count(*) as active_days, td.uid from (select year(to_date(call_time)) as year,
month(to_date(call_time)) as month, day(to_date(call_time)) as day, uid
from {provider_prefix}_consolidate_data_all group by uid, year(to_date(call_time)),
month(to_date(call_time)), day(to_date(call_time)) order by year, month, day, uid) td
group by td.uid) td2