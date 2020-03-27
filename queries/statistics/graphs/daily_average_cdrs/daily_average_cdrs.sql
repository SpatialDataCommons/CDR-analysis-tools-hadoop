select date, total_records/total_uids as daily_average_cdr
from(select to_date(call_time) as date, count(distinct uid) as total_uids,
count(*) as total_records from {provider_prefix}_consolidate_data_all a1
group by to_date(call_time) order by date)td1