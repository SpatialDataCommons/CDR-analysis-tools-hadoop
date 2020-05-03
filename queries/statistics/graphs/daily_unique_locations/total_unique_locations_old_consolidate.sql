select count(*) as count_unique_locations from (select distinct a2.latitude, a2.longitude
from {provider_prefix}_consolidate_data_all a1
join {provider_prefix}_cell_tower_data_preprocess a2
on(a1.cell_id = a2.cell_id)) td