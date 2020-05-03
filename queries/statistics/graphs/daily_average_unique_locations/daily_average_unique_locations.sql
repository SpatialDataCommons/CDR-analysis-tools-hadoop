select date, unique_locations/unique_users as daily_avg_locations, unique_cell_ids/unique_users as daily_avg_cell_ids
from (select to_date(call_time) as date, count(distinct latitude, longitude)  as unique_locations,
count(distinct uid) as unique_users, count(distinct cell_id) as unique_cell_ids
from {provider_prefix}_consolidate_data_all  group by to_date(call_time) order by date) td1