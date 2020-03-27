select sum(td.count)/{total_days} as average_{level}_per_day
from ( select count(distinct a1.{level}) as count from {provider_prefix}_cell_tower_data_preprocess a1
JOIN {provider_prefix}_consolidate_data_all a2
on (a1.cell_id = a2.cell_id) group by to_date(call_time))td