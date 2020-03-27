INSERT OVERWRITE TABLE  {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_detail PARTITION (pdt)
select uid ,home_cell_id,home_zone,m as arr,pdt
from la_cdr_all_with_ant_zone_by_uid_od t1
LATERAL VIEW explode(t1.arr) myTable1 AS m