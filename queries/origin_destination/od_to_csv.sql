insert overwrite local directory '/tmp/hive/od_result'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
select CONCAT_WS('\t',pdt,origin ,
destination,cast(tcount as string),cast(tusercount as string))
from {provider_prefix}_la_cdr_all_with_ant_zone_by_uid_od_sum t1