INSERT INTO TABLE  {provider_prefix}_consolidate_data_all
PARTITION (pdt) select {arg_con}, to_date(a1.call_time) as pdt
from {provider_prefix}_preprocess a1 join
{provider_prefix}_cell_tower_data_preprocess a2
on(a1.cell_id = a2.cell_id)

