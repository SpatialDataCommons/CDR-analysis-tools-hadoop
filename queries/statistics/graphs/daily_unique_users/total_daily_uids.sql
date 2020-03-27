select to_date(call_time) as date, count(distinct uid) as total_users
from {provider_prefix}_consolidate_data_all group by to_date(call_time) order by date