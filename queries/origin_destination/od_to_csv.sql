insert overwrite local directory '/tmp/hive/csv/la_cdr_all_with_ant_zone_by_uid_od_sum.csv' select CONCAT_WS('\t',pdt,origin ,
destination,cast(tcount as string),cast(tusercount as string))
from la_cdr_all_with_ant_zone_by_uid_od_sum t1