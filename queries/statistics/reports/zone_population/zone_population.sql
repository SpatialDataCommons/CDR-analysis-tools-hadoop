select lv as {level}, sum(td.count) as count_activities, count(td.uid) as count_unique_ids from
(select a1.{level} as lv,  count(a1.{level}) as count, a2.uid as uid
from {provider_prefix}_cell_tower_data_preprocess a1 JOIN {provider_prefix}_consolidate_data_all a2
on (a1.cell_id = a2.cell_id) group by a1.{level}, a2.uid) td group by lv