select to_date(call_time) as date, count(distinct a2.latitude, a2.longitude) as unique_locations
from {provider_prefix}_consolidate_data_all a1 join {provider_prefix}_cell_tower_data_preprocess a2
on(a1.cell_id = a2.cell_id) group by to_date(call_time) order by date