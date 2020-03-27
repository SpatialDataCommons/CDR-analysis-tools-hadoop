select date, unique_locations/unique_users as daily_avg_locations, unique_cell_ids/unique_users as daily_avg_cell_ids
from (select to_date(call_time) as date, count(distinct a2.latitude, a2.longitude)  as unique_locations,
count(distinct a1.uid) as unique_users, count(distinct a1.cell_id) as unique_cell_ids
from {provider_prefix}_consolidate_data_all a1 join {provider_prefix}_cell_tower_data_preprocess a2
on(a1.cell_id = a2.cell_id) group by to_date(call_time) order by date) td1